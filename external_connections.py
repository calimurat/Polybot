"""
EXTERNAL CONNECTIONS — Binance BTC price + official Polymarket resolutions
WS primary + REST fallback for each source

Removed 2026-09-29 (backups/pre_chainlink_removal_2026-09-29): the
ChainlinkOracle (legacy RTDS feed + on-chain Polygon read, whose prices
nothing used — the signal and every settlement run on Binance, and
Polymarket is retiring RTDS's price topics), and the AlchemyRPC
transaction sender, which nothing called (see wallet_manager.py).

Production-grade: thread-safe, error recovery, fallback logic, caching
"""

import logging
import threading
import time
import requests
import websocket
import json
from collections import deque
from typing import Optional, Dict, Any, Callable, List, Tuple
from dataclasses import dataclass

from feed_lag import BINANCE_FEED_LAG
from ws_heartbeat import start_text_ping

logger = logging.getLogger("ExternalConnections")

# Skip websocket-client's pure-Python UTF-8 check on text frames (no
# wsaccel installed): it costs 30-55x the JSON parse that follows, and
# every handler here parses with json.loads, which rejects invalid UTF-8
# itself. Handlers then receive bytes instead of str. See
# polymarket_production_bot.BotConfig.ws_skip_utf8_validation for the
# "slow consumer" disconnects this cost was causing.
WS_SKIP_UTF8_VALIDATION = True

# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class ExternalConfig:
    """External connections configuration"""
    timeout: int = 10
    max_retries: int = 3
    retry_backoff: float = 1.5
    cache_ttl: int = 60  # Cache price for 60 seconds

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class PriceData:
    """BTC price data"""
    btc_usd: float
    timestamp: float  # when WE received it (local clock)
    source: str  # "binance_ws" or "binance_rest"
    # Binance WS only: the exchange's own event time (its clock, Unix
    # seconds) and how late the message reached us (feed_lag.py).
    exchange_ts: Optional[float] = None
    feed_lag_s: Optional[float] = None

    def is_fresh(self, max_age_seconds: int = 60) -> bool:
        """Check if price data is fresh"""
        age = time.time() - self.timestamp
        return age < max_age_seconds

# ============================================================================
# BINANCE SPOT (WS Primary + REST Fallback)
# ============================================================================

class BinanceSpot:
    """
    Binance Spot BTCUSDT
    
    Primary: WebSocket stream (real-time)
    Fallback: REST API
    """
    
    def __init__(self):
        self.logger = logging.getLogger("BinanceSpot")
        
        # WS URL — aggTrade (real trade prints, effectively real-time) not
        # ticker (Binance pushes that stream on a fixed 1000ms clock
        # regardless of how fast the market is actually moving, capping
        # every signal's BTC leg at 1s staleness no matter what).
        self.ws_url = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
        
        # REST URL
        self.rest_url = "https://api.binance.com/api/v3/ticker/price"
        
        # WS connection
        self.ws = None
        self.ws_connected = False
        self.price_cache = None
        self.cache_time = 0
        self.cache_lock = threading.RLock()
        # Last time a REST fallback was attempted — see REST_MIN_INTERVAL.
        self._last_rest_attempt = 0.0
        # Reconnect loop control — see start_ws() and stop().
        self.should_run = True
        self._reconnect_attempts = 0

        # Set by BotController for the duration of a trading loop —
        # called (off the cache lock) after every fresh WS tick so the
        # bot can evaluate a signal the instant a new price lands instead
        # of polling on a fixed interval. None outside an active loop.
        self.on_price_update: Optional[Callable[["PriceData"], None]] = None

        # Short (trade_time, price) history of every WS print, keyed on
        # Binance's own trade time ("T"), not our receive time. Lets the
        # window boundaries read the price AT the boundary (price_at())
        # instead of whatever tick happens to be cached when run.py gets
        # round to asking, ~1s later. Bounded by age in _ws_on_message.
        self._tick_history: deque = deque()

        # Stats
        self.stats = {
            "ws_messages": 0,
            "rest_calls": 0,
            "errors": 0
        }

        self.logger.info("[BINANCE] Initialized")

    # How long _tick_history keeps prints. twap_before() needs the full 60s
    # before a boundary, looked up a second or two after it; 90s leaves
    # room for a slow market load.
    TICK_HISTORY_SECONDS = 90.0

    # A gap between consecutive prints longer than this means the feed was
    # down, not that the market was quiet: a quiet Sunday measured 2-3
    # prints a second with gaps of up to 3s.
    FEED_GAP_SECONDS = 15.0

    def twap_before_rest(self, ts: float, seconds: int = 60) -> Optional[float]:
        """
        Mean of Binance's 1-second closes over the `seconds` seconds before
        `ts` — the Binance counterpart of the Chainlink 60s TWAP Polymarket
        settles these markets on. Against 820 official results, comparing
        this at window start and end reproduced 98.0% of outcomes (the spot
        price: 84.3%). Binance emits a candle for every second, so it is
        defined in a quiet market and long after the fact (a restart in the
        middle of a window). None on any failure or an incomplete series.
        """
        end = int(ts)
        start = end - seconds
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1s",
                        "startTime": start * 1000, "limit": seconds},
                timeout=5,
            )
            response.raise_for_status()
            rows = response.json()
            self.stats["rest_calls"] += 1
            if (len(rows) == seconds and int(rows[0][0]) == start * 1000
                    and int(rows[-1][0]) == (end - 1) * 1000):
                return sum(float(r[4]) for r in rows) / seconds
        except Exception as e:
            self.logger.warning(f"[BINANCE] 1s klines (TWAP) fetch failed: {e}")
            self.stats["errors"] += 1
        return None

    def twap_before(self, ts: float, seconds: int = 60) -> Optional[float]:
        """
        twap_before_rest() rebuilt from the WS tick history: each second's
        close is the last print before it ends, carried forward through
        seconds with no trades. None when the history doesn't reach back
        to the start of the span or has a FEED_GAP_SECONDS hole in it
        (including between the last print and the end of the span).
        """
        end = int(ts)
        start = end - seconds
        with self.cache_lock:
            ticks = list(self._tick_history)
        before = [t for t in ticks if t[0] < start]
        if not before:
            return None  # history doesn't reach back to the start
        span = [before[-1]] + [t for t in ticks if start <= t[0] < end]
        if any(b[0] - a[0] > self.FEED_GAP_SECONDS for a, b in zip(span, span[1:])):
            return None
        if end - span[-1][0] > self.FEED_GAP_SECONDS:
            return None
        closes, i, last = [], 0, None
        for second in range(start, end):
            while i < len(span) and span[i][0] < second + 1:
                last = span[i][1]
                i += 1
            closes.append(last)
        return sum(closes) / seconds

    def price_before_rest(self, ts: float) -> Optional[float]:
        """
        Last traded price at `ts`: the close of Binance's 1-second candle
        for the second just before it. Binance emits a candle for every
        second and carries the previous close through seconds with no
        trades, so this is defined in a quiet market and long after the
        fact (a restart in the middle of a window) — which the WS history
        in price_at() is not. Used only as a fallback when no TWAP is
        available (see run.py's _settlement_price). None on any failure.
        """
        second = int(ts) - 1
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1s",
                        "startTime": second * 1000, "limit": 1},
                timeout=5,
            )
            response.raise_for_status()
            rows = response.json()
            self.stats["rest_calls"] += 1
            if rows and int(rows[0][0]) == second * 1000:
                return float(rows[0][4])
        except Exception as e:
            self.logger.warning(f"[BINANCE] 1s kline fetch failed: {e}")
            self.stats["errors"] += 1
        return None

    def price_at(self, ts: float, max_gap_seconds: float = 15.0) -> Optional[float]:
        """
        Last traded price at or before `ts` (unix seconds, Binance trade
        time), or None if the newest such print is more than
        max_gap_seconds older than `ts` or `ts` has aged out of the
        history. None means "fall back to get_price()", never "guess".

        The gap allowance is generous on purpose: on a quiet Sunday
        BTCUSDT was measured at 2-3 prints a second with 3s gaps, and the
        last print before the boundary is the prevailing price however
        old it is. 15s only rejects a feed that was actually down.
        """
        with self.cache_lock:
            best = None
            for trade_ts, price in reversed(self._tick_history):
                if trade_ts <= ts:
                    best = (trade_ts, price)
                    break
        if best is None or ts - best[0] > max_gap_seconds:
            return None
        return best[1]

    def get_minute_closes(self, limit: int = 62) -> List[Tuple[int, float]]:
        """
        Closed 1-minute BTCUSDT candles from REST as (open_time_s, close),
        oldest first. The still-forming current minute is dropped. Returns
        [] on any failure — callers treat missing history as "no
        volatility estimate yet", not as an error.
        """
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1m", "limit": limit},
                timeout=10,
            )
            response.raise_for_status()
            now_ms = time.time() * 1000
            self.stats["rest_calls"] += 1
            return [(int(k[0]) // 1000, float(k[4])) for k in response.json() if k[6] < now_ms]
        except Exception as e:
            self.logger.warning(f"[BINANCE] 1m klines fetch failed: {e}")
            self.stats["errors"] += 1
            return []

    def get_second_closes(self, limit: int = 330) -> List[Tuple[int, float]]:
        """
        Closed 1-second BTCUSDT candles from REST as (open_time_s, close),
        oldest first — history for the signal's log-only ShortVolatility.
        The still-forming current second is dropped. [] on any failure.
        """
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1s", "limit": limit},
                timeout=10,
            )
            response.raise_for_status()
            now_ms = time.time() * 1000
            self.stats["rest_calls"] += 1
            return [(int(k[0]) // 1000, float(k[4])) for k in response.json() if k[6] < now_ms]
        except Exception as e:
            self.logger.warning(f"[BINANCE] 1s klines fetch failed: {e}")
            self.stats["errors"] += 1
            return []
    
    def _ws_on_message(self, ws, message):
        """WS message handler"""
        try:
            data = json.loads(message)
            
            # Extract BTC/USDT price from Binance aggTrade
            if "p" in data:  # trade price
                price = float(data["p"])
                now = time.time()
                trade_ts = data["T"] / 1000 if "T" in data else now
                # "E" = when Binance sent the event: its lateness is the
                # transport delay, which is what the Polymarket book gets
                # compared against (feed_lag.py).
                event_ts = data["E"] / 1000 if "E" in data else None
                feed_lag = BINANCE_FEED_LAG.observe(event_ts, now) if event_ts is not None else None

                with self.cache_lock:
                    self.price_cache = PriceData(
                        btc_usd=price,
                        timestamp=now,
                        source="binance_ws",
                        exchange_ts=event_ts,
                        feed_lag_s=feed_lag,
                    )
                    self.cache_time = now
                    self._tick_history.append((trade_ts, price))
                    cutoff = trade_ts - self.TICK_HISTORY_SECONDS
                    while self._tick_history and self._tick_history[0][0] < cutoff:
                        self._tick_history.popleft()

                self.stats["ws_messages"] += 1
                self.logger.debug(f"[BINANCE] WS price: ${price}")

                # Fire outside the lock — the callback (BotController's
                # tick request) never touches this cache, but holding a
                # lock across an external callback is a habit worth not
                # forming. Best-effort: a broken callback must never take
                # down this WS thread.
                callback = self.on_price_update
                if callback is not None:
                    try:
                        callback(self.price_cache)
                    except Exception as cb_err:
                        self.logger.error(f"[BINANCE] on_price_update callback failed: {cb_err}")
        
        except Exception as e:
            self.logger.error(f"[BINANCE] WS message error: {e}")
            self.stats["errors"] += 1
    
    def _ws_on_error(self, ws, error):
        """WS error handler"""
        self.logger.error(f"[BINANCE] WS error: {error}")
        self.ws_connected = False
        self.stats["errors"] += 1
    
    def _ws_on_close(self, ws, close_status_code, close_msg):
        """WS close handler"""
        self.logger.warning("[BINANCE] WS closed")
        self.ws_connected = False
    
    def _ws_on_open(self, ws):
        """WS open handler"""
        self.logger.info("[BINANCE] WS connected")
        self.ws_connected = True
        self._reconnect_attempts = 0  # a good connection resets the backoff
    
    def start_ws(self):
        """
        Start the price WebSocket (non-blocking, reconnects with backoff).

        The reconnect loop is not optional. This used to call run_forever()
        exactly once: when the socket dropped, the thread simply ended and
        the stream never came back for the rest of the process's life.
        Observed in bot.log on 2026-09-22 — a ping/pong timeout at 14:19:24
        killed it and the next "WS connected" line belongs to a NEW process
        at 17:44:36. For three and a half hours every BTC price behind every
        signal came from the stale cache or a REST poll, with nothing in the
        logs to say the primary feed was gone.

        It also has to work for get_price() to mean anything now: that
        method serves a price at most DEFAULT_MAX_AGE old and only retries
        REST once a second, so a permanently dead stream no longer degrades
        quietly — it stops the bot trading altogether.
        """

        def run_ws():
            while self.should_run:
                try:
                    websocket.enableTrace(False)
                    self.ws = websocket.WebSocketApp(
                        self.ws_url,
                        on_open=self._ws_on_open,
                        on_message=self._ws_on_message,
                        on_error=self._ws_on_error,
                        on_close=self._ws_on_close
                    )
                    self.ws.run_forever(ping_interval=30, ping_timeout=10,
                                        skip_utf8_validation=WS_SKIP_UTF8_VALIDATION)

                except Exception as e:
                    self.logger.error(f"[BINANCE] WS startup failed: {e}")
                    self.stats["errors"] += 1

                if not self.should_run:
                    break

                self.ws_connected = False
                # Exponential
                # backoff capped at 30s, reset by a successful open (see
                # _ws_on_open).
                delay = min(2 ** min(self._reconnect_attempts, 5), 30)
                self._reconnect_attempts += 1
                self.logger.warning(
                    f"[BINANCE] WS down — reconnecting in {delay}s "
                    f"(attempt {self._reconnect_attempts})"
                )
                time.sleep(delay)

        threading.Thread(target=run_ws, daemon=True, name="BinanceWS").start()

        # Give the first connection a moment to come up.
        time.sleep(2)

    def stop(self):
        """
        Stop the reconnect loop and close the socket.

        Without this the WS thread outlived the bot's own shutdown: it kept
        reconnecting and writing into price_cache after run.py had already
        torn everything else down. A signal evaluation was logged 16
        seconds AFTER "[BOT] Shutdown complete" on 2026-09-22 for exactly
        this reason.
        """
        self.should_run = False
        try:
            if self.ws:
                self.ws.close()
        except Exception as e:
            self.logger.debug(f"[BINANCE] WS close during shutdown: {e}")
    
    def get_price_rest(self) -> Optional[PriceData]:
        """Get BTC/USDT price from REST API (fallback)"""
        
        try:
            params = {"symbol": "BTCUSDT"}
            response = requests.get(self.rest_url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            price = float(data["price"])
            
            with self.cache_lock:
                self.price_cache = PriceData(
                    btc_usd=price,
                    timestamp=time.time(),
                    source="binance_rest"
                )
                self.cache_time = time.time()
            
            self.stats["rest_calls"] += 1
            self.logger.debug(f"[BINANCE] REST price: ${price}")
            
            return self.price_cache
        
        except Exception as e:
            self.logger.error(f"[BINANCE] REST failed: {e}")
            self.stats["errors"] += 1
            return None
    
    # How old a cached price may be before this class stops serving it.
    # Was 60s, which is indefensible for a bot resolving 300-second
    # windows: the opening and closing price of a window could each be a
    # minute off, i.e. 40% of the window's real span missing from a
    # comparison that decides UP vs DOWN. BTCUSDT trades many times a
    # second on Binance, so anything older than a few seconds means the WS
    # is actually down and REST should be tried instead of serving a price
    # from before the question was even asked.
    DEFAULT_MAX_AGE = 5.0

    # The window boundaries (see run.py's _start_market/_end_market) decide
    # the resolution every position settles against, so they demand a
    # tighter bound than an ordinary signal tick does.
    RESOLUTION_MAX_AGE = 2.0

    # Minimum gap between REST fallback attempts. Without this, tightening
    # the freshness bound above would be actively dangerous: a signal tick
    # fires on every WS message (hundreds/sec), so a dead Binance WS would
    # turn every one of those into its own REST call and get this client
    # rate-limited or banned within seconds. One attempt per second is
    # plenty to notice a real outage without tripping Binance's REST
    # rate limits.
    REST_MIN_INTERVAL = 1.0

    def get_price(self, max_age_seconds: Optional[float] = None,
                  bypass_throttle: bool = False) -> Optional[PriceData]:
        """
        Latest BTC/USDT price, or None if no sufficiently fresh one is
        available.

        Returning None rather than a stale price is deliberate. Every
        caller here already handles a missing price safely — a signal tick
        skips evaluation, a window boundary settles VOID — and all of
        those are better outcomes than silently computing against a price
        from a minute ago. A wrong resolution is not recoverable; a
        skipped cycle is.

        max_age_seconds: how fresh the caller needs it. Defaults to
            DEFAULT_MAX_AGE; resolution boundaries pass
            RESOLUTION_MAX_AGE.
        bypass_throttle: skip REST_MIN_INTERVAL for this call.

            The throttle exists to stop the signal tick — which fires on
            every WS message, hundreds per second — from turning a dead
            stream into hundreds of REST calls a second. A window boundary
            is the opposite kind of caller: it runs exactly twice per
            five-minute cycle and its answer decides whether the cycle can
            trade at all.

            Without this the two interacted badly. _end_market() would
            find the cache stale and spend the one allowed REST attempt;
            _start_market() ran about a second later, got throttled,
            received None, and the next full five-minute window opened
            with no opening price and therefore could not trade or
            resolve. One WebSocket hiccup at a cycle boundary cost a whole
            cycle.
        """
        if max_age_seconds is None:
            max_age_seconds = self.DEFAULT_MAX_AGE

        with self.cache_lock:
            if self.price_cache and self.price_cache.is_fresh(max_age_seconds=max_age_seconds):
                return self.price_cache

            # Cache is too old — fall back to REST, but not more often
            # than REST_MIN_INTERVAL (see that constant), unless this
            # caller is one of the rare important ones. When throttled,
            # report "no fresh price" instead of handing back the stale
            # cache we just rejected.
            now = time.time()
            if not bypass_throttle and now - self._last_rest_attempt < self.REST_MIN_INTERVAL:
                return None
            self._last_rest_attempt = now

        fresh = self.get_price_rest()
        if fresh is not None and fresh.is_fresh(max_age_seconds=max_age_seconds):
            return fresh
        return None

# ============================================================================
# RESOLUTION WATCHER (audit trail — cross-checks our own resolution against
# Polymarket's official market_resolved event)
# ============================================================================

class ResolutionWatcher:
    """
    Listens for Polymarket's own official "market_resolved" event on the
    CLOB market WS and cross-checks it against our own Binance-
    computed resolution for that same market.

    Verified live (2026-09-14): the official resolution arrives ~2-5 minutes
    AFTER a market's 5-minute window closes — far too slow to use for
    actually closing positions (that still happens immediately via
    ExternalDataCoordinator.resolve_market()). This class is a pure audit
    trail: did our fast, real-time-approximated call actually match
    Polymarket's real, official settlement? Runs as one persistent
    connection for the whole bot lifetime, independent of each 5-minute
    cycle's own short-lived PolymarketBot/WS, so it doesn't miss a
    resolution that lands after we've already rolled over to the next
    market.
    """

    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    REST_RESOLUTIONS_URL = "https://data-api.polymarket.com/v2/resolutions"

    PENDING_TIMEOUT = 900       # give up waiting on a resolution after 15 minutes
    REST_GRACE_PERIOD = 150     # let the WS event win the race for the first 2.5 min
    REST_POLL_INTERVAL = 30     # then check REST this often, per docs.polymarket.com/market-data/public-analytics

    def __init__(self, db=None, on_official_resolution=None):
        self.logger = logging.getLogger("ResolutionWatcher")
        self.should_run = True
        self.ws = None
        self._subscribed = False
        # Reconnect backoff state — see start()'s run_ws loop.
        self._reconnect_attempts = 0
        self._last_ws_error = None
        self._last_close_code = None
        # Optional callback(condition_id, official_resolution), fired once
        # a market's official result is known — whether it agreed with
        # ours or not. This is what turns the class from a pure audit
        # trail into a correction path: run.py points it at
        # PositionManager.resettle_for_official_resolution(), so trades
        # settled on the bot's own (18.6%-wrong, measured) BTC-derived
        # call get their P&L corrected once the real answer arrives.
        # Called on this class's own background thread, best-effort.
        self.on_official_resolution = on_official_resolution
        # Optional Database handle — if given, each result also updates
        # that cycle's market_sessions row (official_resolution/
        # resolution_match) once it's known. Runs from this class's own
        # background threads, so the DB side uses its own short-lived
        # connection rather than the main thread's shared one (see
        # Database.update_cycle_resolution).
        self.db = db

        self.tracked_tokens = set()
        self.tokens_lock = threading.RLock()

        # token_yes_id -> {token_no_id, condition_id, question, our_resolution, added_at}
        self.pending = {}
        self.pending_lock = threading.RLock()

        self.stats = {
            "resolutions_seen": 0, "matches": 0, "mismatches": 0, "unmatched": 0,
            "via_ws": 0, "via_rest": 0
        }

    def track_session(self, token_yes_id: str, token_no_id: str, condition_id: str,
                      question: str, our_resolution: str):
        """
        Register a just-ended market's own computed resolution so it can be
        checked against Polymarket's official result once available — first
        via the WS market_resolved event, and if that hasn't shown up after
        REST_GRACE_PERIOD, via a periodic REST /resolutions poll (see
        docs.polymarket.com/market-data/public-analytics) as a backup path
        independent of the WS connection entirely.
        """
        if our_resolution not in ("UP", "DOWN"):
            return  # nothing to cross-check if we couldn't resolve it ourselves

        with self.pending_lock:
            self._drop_stale()
            self.pending[token_yes_id] = {
                "token_no_id": token_no_id,
                "condition_id": condition_id,
                "question": question,
                "our_resolution": our_resolution,
                "added_at": time.time()
            }

        with self.tokens_lock:
            self.tracked_tokens.add(token_yes_id)
            self.tracked_tokens.add(token_no_id)

        self._resubscribe()

    def _drop_stale(self):
        """Must be called with pending_lock held"""
        now = time.time()
        stale = [tid for tid, info in self.pending.items() if now - info["added_at"] > self.PENDING_TIMEOUT]
        for tid in stale:
            info = self.pending.pop(tid)
            self.stats["unmatched"] += 1
            self.logger.warning(f"[RESOLVE-WATCH] Gave up waiting on resolution for: {info['question']}")
            with self.tokens_lock:
                self.tracked_tokens.discard(tid)
                self.tracked_tokens.discard(info["token_no_id"])

    def _resubscribe(self):
        try:
            with self.tokens_lock:
                tokens = list(self.tracked_tokens)
            if self.ws and tokens:
                self.ws.send(json.dumps({
                    "type": "market", "assets_ids": tokens, "custom_feature_enabled": True
                }))
                self._subscribed = True
        except Exception as e:
            self.logger.debug(f"[RESOLVE-WATCH] Resubscribe failed (will retry on reconnect): {e}")

    def _on_open(self, ws):
        self.logger.info("[RESOLVE-WATCH] Connected")
        self._subscribed = False   # PINGs wait for the first subscription (see start_text_ping)
        self._reconnect_attempts = 0  # a good connection resets the backoff
        self._resubscribe()

    def _on_message(self, ws, message):
        try:
            # bytes, not str, since skip_utf8_validation (see
            # WS_SKIP_UTF8_VALIDATION); json.loads takes either.
            needle = b"market_resolved" if isinstance(message, (bytes, bytearray)) else "market_resolved"
            if not message or needle not in message:
                return  # cheap filter before paying for a full json.loads
            parsed = json.loads(message)
            items = parsed if isinstance(parsed, list) else [parsed]
            for item in items:
                if item.get("event_type") == "market_resolved":
                    self._handle_resolution(item)
        except Exception as e:
            self.logger.debug(f"[RESOLVE-WATCH] Message error: {e}")

    def _handle_resolution(self, item: dict):
        """WS market_resolved handler"""
        winning_outcome = (item.get("winning_outcome") or "").strip().upper()  # "Up"/"Down" -> "UP"/"DOWN"
        assets = item.get("assets_ids", [])

        with self.pending_lock:
            match_key = next((t for t in assets if t in self.pending), None)
            if not match_key:
                return  # resolution for a market we're not tracking (or already matched)
            info = self._pop_pending(match_key)

        self._record_result(info, winning_outcome, source="ws")

    def _pop_pending(self, token_yes_id: str) -> dict:
        """Must be called with pending_lock held"""
        info = self.pending.pop(token_yes_id)
        with self.tokens_lock:
            self.tracked_tokens.discard(token_yes_id)
            self.tracked_tokens.discard(info["token_no_id"])
        return info

    def _record_result(self, info: dict, winning_outcome: str, source: str):
        self.stats["resolutions_seen"] += 1
        self.stats["via_ws" if source == "ws" else "via_rest"] += 1
        our = info["our_resolution"]
        match = (our == winning_outcome)

        if match:
            self.stats["matches"] += 1
            self.logger.info(
                f"[RESOLVE-WATCH] ✓ MATCH ({source}) — {info['question']}: "
                f"our={our}, official={winning_outcome}"
            )
        else:
            self.stats["mismatches"] += 1
            self.logger.warning(
                f"[RESOLVE-WATCH] ✗ MISMATCH ({source}) — {info['question']}: "
                f"our={our}, official={winning_outcome}"
            )

        if self.db is not None and info.get("condition_id"):
            try:
                self.db.update_cycle_resolution(
                    session_id=info["condition_id"],
                    official_resolution=winning_outcome,
                    match=match,
                )
            except Exception as e:
                self.logger.debug(f"[RESOLVE-WATCH] DB update failed: {e}")

        # Correct any trades settled on our own call. Fired for matches
        # too — the callback stamps the official result on agreeing rows
        # so "checked and agreed" stays distinguishable from "never
        # checked". Best-effort: a failure here must not take down this
        # watcher's thread, since it also feeds the audit stats above.
        if self.on_official_resolution is not None and info.get("condition_id"):
            try:
                self.on_official_resolution(info["condition_id"], winning_outcome)
            except Exception as e:
                self.logger.error(f"[RESOLVE-WATCH] Re-settlement callback failed: {e}")

    def _check_rest_resolution(self, token_yes_id: str, info: dict):
        """
        REST fallback: GET /resolutions?condition=... (data-api.polymarket.com,
        see docs.polymarket.com/market-data/public-analytics). Independent of
        the WS connection entirely — catches a resolution even if the WS
        missed it (dropped connection, subscription race, etc).
        """
        condition_id = info.get("condition_id")
        if not condition_id:
            return

        winning_outcome = self.fetch_official_resolution(condition_id)
        if winning_outcome is None:
            return  # not resolved yet (or unreadable), try again next poll

        with self.pending_lock:
            if token_yes_id not in self.pending:
                return  # the WS path already caught this one first
            info = self._pop_pending(token_yes_id)

        self._record_result(info, winning_outcome, source="rest")

    def fetch_official_resolution(self, condition_id: str) -> Optional[str]:
        """
        One-shot lookup of a market's official resolution: GET
        /resolutions?condition=... (data-api.polymarket.com, see
        docs.polymarket.com/market-data/public-analytics). Returns "UP",
        "DOWN", or None if it is not settled yet or the answer cannot be
        read.

        Public and stateless on purpose. Besides the watcher's own REST
        fallback, run.py calls this at startup for markets that a previous
        run left positions open in — those are never in `pending` (that
        only holds markets THIS run traded), so they need a lookup that
        does not depend on the watcher's tracking state at all.
        """
        if not condition_id:
            return None

        try:
            response = requests.get(
                self.REST_RESOLUTIONS_URL, params={"condition": condition_id}, timeout=10
            )
            response.raise_for_status()
            rows = response.json().get("data", [])
            if not rows or rows[0].get("status") != "resolved":
                return None  # not resolved yet

            payouts = rows[0].get("payouts")
            if not payouts or len(payouts) < 2:
                return None

            # payouts[0] = Up token, payouts[1] = Down token (same order as
            # outcomes/clobTokenIds — verified 2026-09-14); whichever is
            # nonzero won. A genuinely resolved binary market always has a
            # distinct 0/1 pair; equal payouts means the API's data isn't
            # trustworthy yet despite status=="resolved" (seen transiently
            # right as a market settles). Guessing a direction from that
            # would also disagree with resolve_market()'s own tie rule
            # (ties go UP, not DOWN) — safer to report "unknown" and let
            # the caller try again.
            if payouts[0] == payouts[1]:
                self.logger.debug(
                    f"[RESOLVE-WATCH] REST resolution has equal payouts "
                    f"{payouts} for {condition_id}, treating as not-yet-settled"
                )
                return None

            return "UP" if payouts[0] > payouts[1] else "DOWN"

        except Exception as e:
            self.logger.debug(f"[RESOLVE-WATCH] REST resolution check failed: {e}")
            return None

    def _rest_poll_loop(self):
        """Background loop: after the WS grace period, poll REST for any
        market still pending. Runs for the whole bot lifetime."""

        while self.should_run:
            time.sleep(self.REST_POLL_INTERVAL)

            with self.pending_lock:
                self._drop_stale()
                now = time.time()
                due = [
                    (tid, info) for tid, info in self.pending.items()
                    if now - info["added_at"] > self.REST_GRACE_PERIOD
                ]

            for token_yes_id, info in due:
                self._check_rest_resolution(token_yes_id, info)

    def _on_error(self, ws, error):
        self._last_ws_error = error
        self.logger.debug(f"[RESOLVE-WATCH] WS error: {error}")

    def _on_close(self, ws, code, msg):
        self._last_close_code = code
        self.logger.debug(f"[RESOLVE-WATCH] WS closed: {code} {msg}")

    def start(self):
        """
        Start the persistent watcher: the WS connection (non-blocking, auto-
        reconnects) plus a background REST-polling fallback thread that
        catches anything the WS side misses.
        """

        def run_ws():
            while self.should_run:
                try:
                    self.ws = websocket.WebSocketApp(
                        self.WS_URL,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close
                    )
                    # Polymarket's documented text heartbeat (ws_heartbeat.py);
                    # the PONG replies are dropped by _on_message's filter.
                    start_text_ping(self.ws, lambda: self.ws, lambda: self.should_run,
                                    name="ResolutionTextPing", ready=lambda: self._subscribed)
                    self.ws.run_forever(ping_interval=10, ping_timeout=5,
                                        skip_utf8_validation=WS_SKIP_UTF8_VALIDATION)
                except Exception as e:
                    self.logger.error(f"[RESOLVE-WATCH] WS startup failed: {e}")

                if not self.should_run:
                    break

                # Same shape as BinanceSpot: exponential
                # backoff capped at 30s, reset by a successful open (see
                # _on_open). This was a flat 3s, which during the multi-hour
                # Polymarket outages of 2026-09-24/25/27 meant ~1,290 failed
                # connects an hour — the bulk of bot.log's error lines.
                delay = min(2 ** min(self._reconnect_attempts, 5), 30)
                self._reconnect_attempts += 1
                error, self._last_ws_error = self._last_ws_error, None
                code, self._last_close_code = self._last_close_code, None
                if code == 1000 and self._reconnect_attempts == 1:
                    # Routine: Polymarket closes the socket itself once every
                    # asset it was subscribed to has resolved — about once a
                    # cycle in normal operation.
                    self.logger.debug(f"[RESOLVE-WATCH] WS closed by server — reconnecting in {delay}s")
                else:
                    self.logger.warning(
                        f"[RESOLVE-WATCH] WS down ({error or f'close code {code}'}) — "
                        f"reconnecting in {delay}s (attempt {self._reconnect_attempts})"
                    )
                time.sleep(delay)

        threading.Thread(target=run_ws, daemon=True).start()
        threading.Thread(target=self._rest_poll_loop, daemon=True).start()

    def stop(self):
        """
        Stop the watcher's WS and REST-poll threads.

        Matters more than an ordinary teardown because this class WRITES to
        the database — update_cycle_resolution() and, since the official
        resolution now feeds back into settlement, the re-settlement
        callback. Leaving it running past run.py's _cleanup() meant those
        writes could fire against a database the bot had already closed.
        """
        self.should_run = False
        try:
            if self.ws:
                self.ws.close()
        except Exception as e:
            self.logger.debug(f"[RESOLVE-WATCH] WS close during shutdown: {e}")

    def get_stats(self) -> Dict[str, Any]:
        return dict(self.stats)

# ============================================================================
# EXTERNAL DATA COORDINATOR
# ============================================================================

class ExternalDataCoordinator:
    """
    Orchestrate all external connections
    
    Provides:
    - binance — the BTC price feed (BinanceSpot)
    - resolve_market() — Compare opening vs closing price
    """
    
    def __init__(self, config: ExternalConfig):
        self.logger = logging.getLogger("ExternalCoordinator")
        self.config = config
        
        # Initialize all sources
        self.binance = BinanceSpot()
        
        self.logger.info("[COORDINATOR] Initialized")
    
    def start(self):
        """Start all WS connections"""
        
        try:
            self.logger.info("[COORDINATOR] Starting connections...")
            
            self.binance.start_ws()
            
            self.logger.info("[COORDINATOR] All connections started")
        
        except Exception as e:
            self.logger.error(f"[COORDINATOR] Start failed: {e}")
    
    def stop(self):
        """
        Stop every feed this coordinator started.

        There was no way to do this at all before: the class had start()
        and nothing else, so its WebSocket threads ran until the
        process died — past run.py's shutdown, past the database close.
        """
        self.logger.info("[COORDINATOR] Stopping connections...")
        for name, source in (("binance", self.binance),):
            try:
                source.stop()
            except Exception as e:
                self.logger.warning(f"[COORDINATOR] {name} stop failed: {e}")
        self.logger.info("[COORDINATOR] Connections stopped")

    def resolve_market(self, opening_price: float, closing_price: Optional[float] = None) -> str:
        """
        Resolve market outcome (UP/DOWN)

        Compare closing price vs opening price

        Args:
            opening_price: BTC/USD price when the market window opened
                (a dollar amount, e.g. 60000.0 — NOT a Polymarket token
                probability, which is on a 0.0-1.0 scale)
            closing_price: BTC/USD price to resolve against, if the caller
                already fetched one (e.g. to log/persist it) — reuse that
                SAME reading instead of fetching again here. Two independent
                price reads a moment apart can return different
                values if a new price tick lands on the WS feed in between
                (most likely right at a volatile market close, exactly when
                this runs), which would let the resolution disagree with
                whatever closing_price the caller already recorded. Only
                fetches fresh if the caller didn't already have one.

        Returns: "UP" or "DOWN"
        """

        try:
            if closing_price is None:
                price_data = self.binance.get_price()
                if not price_data:
                    self.logger.error("[COORDINATOR] Failed to get closing price")
                    return "UNKNOWN"
                closing_price = price_data.btc_usd

            # Compare — Polymarket's own market description (fetched live
            # from Gamma API, verified 2026-09-14) says verbatim: "resolve
            # to Up if the Bitcoin price at the end of the time range... is
            # greater than or equal to the price at the beginning of that
            # range" — i.e. a tie resolves UP, not DOWN.
            resolution = "UP" if closing_price >= opening_price else "DOWN"
            
            self.logger.info(
                f"[COORDINATOR] Market resolution: {resolution}\n"
                f"  Opening: {opening_price}\n"
                f"  Closing: {closing_price}"
            )
            
            return resolution
        
        except Exception as e:
            self.logger.error(f"[COORDINATOR] Resolve market failed: {e}")
            return "UNKNOWN"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics from all sources"""
        
        return {
            "binance": self.binance.stats,
        }
    
    def export_stats(self) -> str:
        """Export stats as formatted string"""
        
        stats = self.get_stats()
        
        lines = [
            "="*60,
            "EXTERNAL CONNECTIONS STATISTICS",
            "="*60,
            "",
            "BINANCE:",
            f"  WS messages:     {stats['binance']['ws_messages']}",
            f"  REST calls:      {stats['binance']['rest_calls']}",
            f"  Errors:          {stats['binance']['errors']}",
            "="*60
        ]
        
        return "\n".join(lines)

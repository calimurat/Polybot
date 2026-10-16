"""
POLYMARKET 5-MINUTE BTC BOT — PRODUCTION GRADE
Bağlantı: CLOB Market WS (primary) + REST (fallback)
State Machine: Thread-safe, no race conditions, no data corruption
Paper Trading: 100% simulation, no real orders
"""

import threading
import time
import socket
import requests
import websocket
import orjson
import logging
from enum import Enum
from typing import Optional, Callable
from dataclasses import dataclass, field, replace
from collections import deque

from feed_lag import POLYMARKET_FEED_LAG
from ws_heartbeat import is_pong, start_text_ping
from poly_book import apply_price_changes, levels_from

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PolyBot")

# ============================================================================
# ENUMS & CONSTANTS
# ============================================================================

class DataSource(Enum):
    """Active data source state"""
    CONNECTED_WS = "connected_ws"
    STALE_FALLBACK_REST = "stale_fallback_rest"
    RECONNECTING = "reconnecting"
    INITIALIZING = "initializing"
    SHUTDOWN = "shutdown"

# ============================================================================
# CONFIG
# ============================================================================

@dataclass
class BotConfig:
    """Bot configuration"""
    # Market
    market_duration_seconds: int = 300  # 5 minutes
    market_name: str = "BTCUSD-5M"
    
    # WebSocket
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ws_ping_interval: int = 10
    ws_ping_timeout: int = 5
    # Polymarket's documented application-level heartbeat: a text PING this
    # often, answered with PONG (see ws_heartbeat.py). 0 disables it.
    ws_text_ping_interval: float = 10.0
    ws_heartbeat_timeout: int = 45
    ws_heartbeat_check_interval: int = 10
    ws_reconnect_max_attempts: int = 50
    ws_reconnect_initial_delay: float = 1.0
    # Was 300.0 — equal to a full 5-minute market window. A few early
    # failures (attempt ~14-15 with multiplier=1.5) could grow the backoff
    # to the full window length, meaning one bad stretch right after a
    # market opens could commit the bot to REST-only (slower, polled) data
    # for the ENTIRE rest of that window instead of ever trying WS again.
    # 20s keeps retrying often enough (backoff caps by attempt ~8) to have
    # a real chance of recovering mid-window — see ws_reconnect_max_attempts
    # (50) for why this still comfortably outlasts a single 5-minute cycle
    # (~8 attempts to reach the cap, then ~42 more at 20s = ~14 more
    # minutes of retrying before ever giving up).
    ws_reconnect_max_delay: float = 20.0
    ws_reconnect_multiplier: float = 1.5
    ws_reconnect_jitter: float = 0.1
    # Skip websocket-client's UTF-8 check on text frames. Without the
    # optional wsaccel package it is pure Python, byte by byte: measured at
    # 100-670 us per message (30-55x the orjson parse right after it) on
    # this feed's 0.6-4 KB frames, ~110 frames/s on average and 700-800/s
    # in bursts. That held the reader thread — and the GIL — long enough
    # for Polymarket to drop the socket as a "slow consumer" (close code
    # 1013) 84-447 times a day. Nothing is lost: orjson rejects invalid
    # UTF-8 itself. With the check skipped, on_message receives bytes,
    # which orjson.loads takes directly.
    ws_skip_utf8_validation: bool = True
    # Keep the cached order books exact between full "book" snapshots by
    # applying "price_change" level updates (~96% of this feed's frames).
    # Without them the cache is whatever the last snapshot said — on this
    # feed ~0.8 snapshots/s per token, so up to ~2s behind the real book
    # (Orderbook.is_valid's limit). Every update is checked against the
    # best_bid/best_ask Polymarket sends with it; a book that disagrees
    # stops publishing until the next snapshot resets it, so a wrong
    # assumption about the payload can only fall back to snapshot-only
    # behaviour, never serve a corrupted book. See _apply_price_changes.
    apply_price_changes: bool = True

    # REST
    rest_url: str = "https://clob.polymarket.com"
    rest_timeout: int = 5
    rest_poll_interval: int = 5
    rest_max_retries: int = 3
    rest_verify_timeout: int = 2
    
    # Gamma API
    gamma_url: str = "https://gamma-api.polymarket.com"
    gamma_timeout: int = 10
    gamma_cache_ttl: int = 300
    
    # Token IDs (example)
    token_yes_id: str = "52114319501245915516055106046884209969926127482827954674443846427813813222426"
    token_no_id: str = "21742633143463906290569050155826241533067272736897614950488156847949938836455"
    
    # Trading
    paper_trade_size: int = 10
    signal_detection_interval: int = 1  # Check signal every 1 second
    
    # Monitoring
    stats_log_interval: int = 60

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Orderbook:
    """Orderbook snapshot"""
    bids: list = field(default_factory=list)
    asks: list = field(default_factory=list)
    timestamp: float = 0.0  # when WE received it (local clock)
    source: str = "unknown"  # "WS" or "REST"
    # Polymarket's own timestamp on the message that produced this state
    # (server clock, Unix seconds), and how late that message reached us
    # (feed_lag.py). Diagnostics for this book; how far behind the bot's
    # view is as a whole is PolymarketBot.feed_behind_s().
    server_ts: Optional[float] = None
    feed_lag_s: Optional[float] = None

    @property
    def best_bid(self) -> Optional[float]:
        # Polymarket's CLOB WS sends bids sorted ASCENDING by price (index 0
        # is the worst/deepest bid, not the best) — verified live 2026-09-14.
        # Best bid is the highest price, so take max(), not bids[0].
        # Guarded the same way order_book_imbalance()/vwap_fill_price()
        # guard their own identical level-parsing — a malformed/partial
        # entry (missing "price", non-numeric) from either the WS feed or
        # REST fallback would otherwise raise out of a property read
        # pervasively and unguarded throughout run.py.
        try:
            return max((float(b["price"]) for b in self.bids), default=None)
        except (KeyError, ValueError, TypeError):
            return None

    @property
    def best_ask(self) -> Optional[float]:
        # Same feed sends asks sorted DESCENDING by price (index 0 is the
        # worst/deepest ask). Best ask is the lowest price, so take min().
        try:
            return min((float(a["price"]) for a in self.asks), default=None)
        except (KeyError, ValueError, TypeError):
            return None
    
    @property
    def spread(self) -> Optional[float]:
        # `is not None`, not truthiness: a best_bid of exactly 0.0 is a
        # real price on this market (tokens are probabilities in [0,1] and
        # a worthless side genuinely trades at 0), but it is falsy, so the
        # old check reported "no spread" for a book that had one.
        # Verified: bid 0.0 / ask 0.03 returned None instead of 0.03.
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None
    
    def is_valid(self, max_age_seconds: Optional[float] = None) -> bool:
        """
        Check if orderbook has valid, sufficiently fresh data.

        Previously this only checked that bids/asks were non-empty, with
        no check on how old the snapshot was — a book cached right before
        a WS drop (e.g. the "slow consumer" disconnects, which cluster
        during high-volatility bursts) stayed "valid" indefinitely, so a
        trade signal firing during that gap would price off a stale
        best_bid/best_ask. Harmless in paper trading, but this same
        entry_price computation (run.py's best_bid/best_ask midpoint) is
        exactly what a real order would be priced from once real trading
        is implemented — so reject anything too old instead.

        This only knows when the book ARRIVED, not how old it was by
        then. 2026-09-28 measured WS messages arriving seconds late
        through a backed-up VPN tunnel, and those books passed this check
        as fresh — the execution path also checks
        PolymarketBot.feed_behind_s(), which measures that lateness from
        Polymarket's own timestamps (see feed_lag.py).

        2s already covers a full reconnect blip. REST fallback only
        refreshes every BotConfig.rest_poll_interval (default 5s), so it
        needs a looser bound — otherwise the cache would read as "stale"
        for most of every poll cycle even when REST fallback is working
        exactly as designed.
        """
        if not (self.bids and self.asks and self.timestamp > 0):
            return False
        if max_age_seconds is None:
            max_age_seconds = 2.0 if self.source == "WS" else 8.0
        return (time.time() - self.timestamp) < max_age_seconds

    def order_book_imbalance(self, levels: int) -> Optional[float]:
        """
        Order book imbalance at the given depth:
        (bid_depth - ask_depth) / (bid_depth + ask_depth)
        using the best `levels` price levels on each side. Bids are sorted
        ascending, asks descending (index 0 = worst on both sides — see
        best_bid/best_ask above), so the best `levels` are the LAST
        `levels` entries of each list. Positive = more size resting on the
        bid side (buy pressure); negative = more on the ask side.
        Returns None if there's no usable data on either side.
        """
        if not self.bids or not self.asks:
            return None
        try:
            bid_levels = self.bids[-levels:] if levels > 0 else self.bids
            ask_levels = self.asks[-levels:] if levels > 0 else self.asks
            bid_depth = sum(float(b["size"]) for b in bid_levels)
            ask_depth = sum(float(a["size"]) for a in ask_levels)
            total = bid_depth + ask_depth
            if total <= 0:
                return None
            return (bid_depth - ask_depth) / total
        except (KeyError, ValueError, TypeError):
            return None

    def vwap_fill_price(self, size: float, side: str) -> Optional[float]:
        """
        Volume-weighted average price to fill `size` shares as a taker,
        walking real order book depth instead of assuming the whole size
        fills at best_bid/best_ask. If the top level doesn't have enough
        resting size, a real marketable order keeps matching against the
        next levels at progressively worse prices — this reproduces that,
        so the result is the actual expected slippage for this size, not
        just the top-of-book spread.

        side: "buy" walks the ask side (best_ask outward, worse=higher),
              "sell" walks the bid side (best_bid outward, worse=lower).
        Returns None if the book doesn't have enough depth to fill all of
        `size` (the order is always the full, fixed size), or has no usable
        data.
        """
        levels = self.asks if side.lower() == "buy" else self.bids
        if not levels or size <= 0:
            return None
        buy = side.lower() == "buy"
        try:
            # Best price first. Sorted here rather than trusting the feed's
            # order (snapshots arrive worst-first): the list may also be
            # rebuilt from incremental updates, and pricing must not
            # depend on how it was assembled.
            book = sorted(((float(l["price"]), float(l["size"])) for l in levels),
                          reverse=not buy)
            remaining = size
            cost = 0.0
            for level_price, level_size in book:
                take = min(remaining, level_size)
                cost += take * level_price
                remaining -= take
                if remaining <= 0:
                    break
            if remaining > 1e-9:
                return None  # not enough resting depth
            return cost / size
        except (KeyError, ValueError, TypeError):
            return None

    def copy(self) -> "Orderbook":
        """Snapshot copy (used to read the cache outside the lock)"""
        return replace(self)

# ============================================================================
# MAIN BOT CLASS
# ============================================================================

class PolymarketBot:
    """
    Production-grade Polymarket 5-minute BTC bot
    
    Key features:
    - State machine: CONNECTED_WS ↔ STALE_FALLBACK_REST
    - Thread-safe: All shared data protected by locks
    - No race conditions: Only one thread writes to cache per state
    - Automatic recovery: Heartbeat monitors WS + triggers fallback/recovery
    """

    def __init__(self, config: BotConfig = None):
        self.config = config or BotConfig()

        # ===== STATE MACHINE =====
        self.state = DataSource.INITIALIZING
        self.state_lock = threading.RLock()
        
        # ===== DATA CACHE =====
        # Separate caches per token — the WS subscription carries "book"
        # events for BOTH the YES and NO token, distinguished only by
        # asset_id. A single shared cache overwritten by whichever book
        # arrives last would silently flip between the YES and NO order
        # book from one message to the next (verified live 2026-09-14:
        # events for both tokens interleave, e.g. NO,YES,YES,NO,YES,...).
        self.orderbook_cache_yes = Orderbook()
        self.orderbook_cache_no = Orderbook()
        self.cache_lock = threading.RLock()
        # Live level maps behind those caches when price_change updates are
        # applied (BotConfig.apply_price_changes): per token, side ->
        # {price: (price_str, size_str)}. Touched only by the WS reader
        # thread. _live_ok[token] is True from a "book" snapshot until an
        # update fails verification, then False until the next snapshot.
        self._live_levels = {t: {"bids": {}, "asks": {}} for t in ("yes", "no")}
        self._live_ok = {"yes": False, "no": False}

        # Set by BotController for the duration of a trading loop —
        # called (off cache_lock) after every fresh "book" event so the
        # bot can evaluate a signal the instant the order book actually
        # changes instead of polling on a fixed interval. None outside an
        # active loop, and never set on a prewarmed-but-not-yet-promoted
        # bot (see run.py's _prewarm_next_market()).
        self.on_book_update: Optional[Callable[[str, "Orderbook"], None]] = None

        # ===== TRADE FLOW =====
        # Rolling (timestamp, side, size) history per token from
        # last_trade_price WS events, for computing aggressive buy/sell
        # volume ratio (trade_flow_ratio()). Bounded by wall-clock age.
        self.trade_history_yes = deque()
        self.trade_history_no = deque()
        self.trade_history_lock = threading.RLock()
        self.TRADE_HISTORY_WINDOW = 60  # seconds

        # ===== TIMING =====
        self.last_ws_message_time = time.time()
        # Feed lag (see feed_lag.py and feed_behind_s()): the newest
        # Polymarket timestamp seen on the current connection (server
        # clock), the lag of the newest message, and the worst lag since
        # the last STATS line. Reset per connection in on_open.
        self.last_server_ts: Optional[float] = None
        self.feed_lag_s: Optional[float] = None
        self._lag_interval_max_s = 0.0
        self.market_start_time: Optional[float] = None
        self.market_end_time: Optional[float] = None
        
        # Set by start_remaining_threads() — see start_market()'s
        # full_threads docstring.
        self._remaining_threads_started = False

        # ===== WEBSOCKET =====
        self.ws = None
        self.ws_connected = False
        self.ws_reconnect_attempts = 0
        
        # ===== FLAGS =====
        self.should_run = True
        self.market_open = False
        self.rest_polling_active = False
        
        # ===== STATISTICS =====
        self.stats = {
            "ws_messages_received": 0,
            "ws_errors": 0,
            "rest_polls": 0,
            "rest_errors": 0,
            "state_changes": 0,
            # Feed-composition counters. book_snapshots vs
            # price_changes_ignored is exactly the ratio needed to decide
            # whether applying incremental updates is worth the risk —
            # see the price_change branch in ws_reader_thread().
            "book_snapshots": 0,
            "price_changes_ignored": 0,
            "best_bid_ask_ignored": 0,
            # price_change handling (see _apply_price_changes): updates
            # applied to a verified live book, updates whose result
            # disagreed with Polymarket's own best_bid/best_ask, and
            # updates that carried no best_bid/best_ask to check against.
            "price_changes_applied": 0,
            "price_change_mismatches": 0,
            "price_changes_unverifiable": 0,
            # Replies to the text PING heartbeat (ws_heartbeat.py).
            "pongs_received": 0,
        }
        self.stats_lock = threading.RLock()

        logger.info("Bot initialized")
    
    # ========================================================================
    # STATE MACHINE
    # ========================================================================
    
    def set_state(self, new_state: DataSource) -> bool:
        """
        Thread-safe state transition
        Returns: True if state changed, False otherwise
        """
        with self.state_lock:
            if self.state != new_state:
                old_state = self.state
                self.state = new_state
                
                with self.stats_lock:
                    self.stats["state_changes"] += 1
                
                logger.warning(f"STATE CHANGE: {old_state.value} → {new_state.value}")
                return True
            return False
    
    def get_state(self) -> DataSource:
        """Thread-safe state read"""
        with self.state_lock:
            return self.state

    def feed_behind_s(self, now: Optional[float] = None) -> Optional[float]:
        """
        How far behind Polymarket the cached books are right now, in
        seconds: nothing the exchange did after the newest timestamp we
        have seen can be in them. Lag of the newest message plus time
        since it arrived — see FeedLagEstimator.behind_s. Covers both
        books, since both tokens share one connection and the server
        sends in order. None until this connection has delivered a
        timestamped message.
        """
        return POLYMARKET_FEED_LAG.behind_s(
            self.last_server_ts, time.time() if now is None else now)

    def _observe_feed_lag(self, data: dict, receive_ts: float):
        """(server_ts, lag_s) for one event, recording it; (None, None)
        if the event carries no usable timestamp."""
        ts = data.get("timestamp")
        if ts is None:
            return None, None
        try:
            server_ts = float(ts) / 1000.0
        except (TypeError, ValueError):
            return None, None
        lag = POLYMARKET_FEED_LAG.observe(server_ts, receive_ts)
        self.feed_lag_s = lag
        if lag > self._lag_interval_max_s:
            self._lag_interval_max_s = lag
        if self.last_server_ts is None or server_ts > self.last_server_ts:
            self.last_server_ts = server_ts
        return server_ts, lag

    def trade_flow_ratio(self, token: str) -> Optional[float]:
        """
        Fraction of recent (last TRADE_HISTORY_WINDOW seconds) trade
        volume that was aggressive BUY-side, for 'yes' or 'no'. >0.5 =
        buy-dominated, <0.5 = sell-dominated. None if no trades yet.
        """
        history = self.trade_history_yes if token == "yes" else self.trade_history_no
        with self.trade_history_lock:
            trades = list(history)

        if not trades:
            return None

        total_vol = sum(size for _, _side, size in trades)
        if total_vol <= 0:
            return None

        buy_vol = sum(size for _, side, size in trades if side == "BUY")
        return buy_vol / total_vol

    # ========================================================================
    # WEBSOCKET READER THREAD
    # ========================================================================
    
    # ========================================================================
    # LIVE ORDER BOOK (snapshot + price_change updates)
    # ========================================================================

    def _token_side(self, asset_id) -> Optional[str]:
        if asset_id == self.config.token_yes_id:
            return "yes"
        if asset_id == self.config.token_no_id:
            return "no"
        return None

    @staticmethod
    def _book_from_levels(levels: dict, receive_ts: float,
                          server_ts: Optional[float], feed_lag_s: Optional[float]) -> Orderbook:
        """Rebuild an Orderbook in the feed's own layout (bids ascending,
        asks descending — worst price first), which order_book_imbalance()
        and the book_snapshot record in run.py still assume."""
        bids = [{"price": p, "size": s} for _, (p, s) in sorted(levels["bids"].items())]
        asks = [{"price": p, "size": s} for _, (p, s) in sorted(levels["asks"].items(), reverse=True)]
        return Orderbook(bids=bids, asks=asks, timestamp=receive_ts, source="WS",
                         server_ts=server_ts, feed_lag_s=feed_lag_s)

    def _publish_book(self, token_side: str, book: Orderbook):
        with self.cache_lock:
            if token_side == "yes":
                self.orderbook_cache_yes = book
            else:
                self.orderbook_cache_no = book
        # Fire outside cache_lock — best-effort, must never take down the
        # WS thread (book events have been seen at 700-800/s in bursts, so
        # a broken callback would fire that often too).
        callback = self.on_book_update
        if callback is not None:
            try:
                callback(token_side, book)
            except Exception as cb_err:
                logger.error(f"[WS] on_book_update callback failed: {cb_err}")

    def _apply_price_changes(self, data: dict, receive_ts: float,
                             server_ts: Optional[float] = None,
                             feed_lag_s: Optional[float] = None):
        """
        Apply one "price_change" event to the live books, verify, publish.

        Payload (Polymarket CLOB market channel): data["price_changes"] is a
        list of {asset_id, price, size, side, best_bid, best_ask, ...}.
        `size` is the new total resting size at that price ("0" removes the
        level), side "BUY" is the bid side and "SELL" the ask side, and
        best_bid/best_ask are that token's top of book after the change.

        Only a token whose live book was seeded by a snapshot is updated.
        After applying its changes, the rebuilt book's best bid/ask must
        equal the best_bid/best_ask of the token's last change in this
        event; if not — or if those fields are missing — the token stops
        publishing until the next snapshot. The previously published book
        then simply ages out (Orderbook.is_valid), i.e. the worst case is
        the old snapshot-only behaviour, never a corrupted book.
        """
        # The rules live in poly_book.py, shared with the market tape
        # recorder so both keep identical books (2026-10-05).
        verified, mismatches, counts = apply_price_changes(
            data.get("price_changes") or [], self._token_side, self._live_levels, self._live_ok)
        for token in verified:
            self._publish_book(token, self._book_from_levels(
                self._live_levels[token], receive_ts, server_ts, feed_lag_s))
        for token, have_bid, have_ask, want_bid, want_ask in mismatches:
            logger.warning(
                f"[WS] Live {token.upper()} book failed verification "
                f"(ours {have_bid}/{have_ask}, Polymarket {want_bid}/{want_ask}) "
                f"— waiting for the next snapshot"
            )

        with self.stats_lock:
            self.stats["price_changes_applied"] += counts["applied"]
            self.stats["price_change_mismatches"] += counts["mismatched"]
            self.stats["price_changes_unverifiable"] += counts["unverifiable"]
            self.stats["price_changes_ignored"] += counts["ignored"]

    def _handle_ws_event(self, data: dict, current_state, receive_ts: float):
        """
        Process a single parsed WS event dict (never a list).

        Runs on the WS reader thread, inside on_message. `receive_ts` is
        when on_message received the frame.
        """
        event_type = data.get("event_type")
        # Computed once per event instead of relying on logger.debug()'s
        # own internal level check: an f-string argument is built BEFORE
        # the call happens (Python evaluates arguments eagerly), so
        # without this guard every one of these string interpolations
        # runs on every message even with DEBUG disabled — real,
        # avoidable CPU cost at the hundreds-of-messages/sec rates this
        # feed pushes during volatile periods, on top of already-heavy
        # GIL contention with this process's other ~10+ threads (see
        # the slow-consumer disconnect investigation).
        debug_enabled = logger.isEnabledFor(logging.DEBUG)

        if current_state != DataSource.CONNECTED_WS:
            # WS is in standby (REST is active)
            # Prepare for recovery if we got fresh data
            if event_type == "book" and current_state == DataSource.STALE_FALLBACK_REST:
                logger.debug("[WS] Fresh data received while in REST fallback")
            return

        # ─────────────────────────────────────────────────────
        # ONLY PROCESS BELOW if state == CONNECTED_WS
        # ─────────────────────────────────────────────────────

        # One lock acquisition per message, not one per counter: this
        # runs on the WS reader thread at up to 700-800 events/s during
        # volatility bursts, and time spent here is time not spent
        # reading the socket.
        with self.stats_lock:
            self.stats["ws_messages_received"] += 1
            if event_type == "book":
                self.stats["book_snapshots"] += 1
            elif event_type == "price_change" and not self.config.apply_price_changes:
                self.stats["price_changes_ignored"] += 1
            elif event_type == "best_bid_ask":
                self.stats["best_bid_ask_ignored"] += 1

        # Every event type carries Polymarket's "timestamp" (ms) — checked
        # live 2026-09-28 for book, price_change, last_trade_price and
        # best_bid_ask; a subscribe-time snapshot is stamped when it was
        # generated, not when the book last changed.
        server_ts, feed_lag = self._observe_feed_lag(data, receive_ts)

        if event_type == "book":
            # Full orderbook snapshot — for whichever token (YES or NO)
            # this specific event is for. Route by asset_id instead of
            # overwriting one shared cache regardless of which token
            # sent it (see the comment on orderbook_cache_yes/no above).
            asset_id = data.get("asset_id")
            book = Orderbook(
                bids=data.get("bids", []),
                asks=data.get("asks", []),
                timestamp=receive_ts,
                source="WS",
                server_ts=server_ts,
                feed_lag_s=feed_lag,
            )
            token_side = self._token_side(asset_id)
            if token_side is None:
                if debug_enabled:
                    logger.debug(f"[WS] Book for unrecognized asset_id: {asset_id}")
            else:
                if self.config.apply_price_changes:
                    # A snapshot is the ground truth the live book is
                    # rebuilt from (and re-armed by, after a failed check).
                    self._live_levels[token_side] = {
                        "bids": levels_from(book.bids),
                        "asks": levels_from(book.asks),
                    }
                    self._live_ok[token_side] = True
                self._publish_book(token_side, book)
            if debug_enabled:
                logger.debug(f"[WS] Book ({'YES' if asset_id == self.config.token_yes_id else 'NO'}): {len(data.get('bids', []))} bids")

        elif event_type == "price_change":
            # Incremental level updates — applied to the live books when
            # BotConfig.apply_price_changes is on (see its comment and
            # _apply_price_changes for the verification that guards it).
            # Until 2026-09-28 these were only counted: the payload shape
            # had not been checked against the live feed, and a mis-parsed
            # update would silently corrupt the depth vwap_fill_price()
            # walks. The per-update best_bid/best_ask check is what makes
            # applying them safe without that prior verification.
            if self.config.apply_price_changes:
                self._apply_price_changes(data, receive_ts, server_ts, feed_lag)
            if debug_enabled:
                logger.debug(f"[WS] Price change: {data.get('price_changes')}")

        elif event_type == "last_trade_price":
            # Trade event — track for trade_flow_ratio() (aggressive
            # buy vs sell volume). Routed by asset_id same as book
            # events, for the same reason (YES and NO trades interleave
            # on this one subscription).
            asset_id = data.get("asset_id")
            side = data.get("side")
            try:
                size = float(data.get("size", 0))
            except (TypeError, ValueError):
                size = 0.0

            if asset_id in (self.config.token_yes_id, self.config.token_no_id) and side:
                history = (
                    self.trade_history_yes if asset_id == self.config.token_yes_id
                    else self.trade_history_no
                )
                now = time.time()
                cutoff = now - self.TRADE_HISTORY_WINDOW
                with self.trade_history_lock:
                    history.append((now, side, size))
                    while history and history[0][0] < cutoff:
                        history.popleft()

            if debug_enabled:
                logger.debug(f"[WS] Trade: {data.get('price')} side={side} size={size}")

        elif event_type == "market_resolved":
            # Market ended
            logger.info("[WS] Market resolved")
            self.market_open = False

        elif event_type == "best_bid_ask":
            # Also not applied: it carries only top-of-book, while the
            # cache holds full depth for vwap_fill_price(). Counted
            # for the same sizing reason as price_change above.
            if debug_enabled:
                logger.debug(
                    f"[WS] Best bid/ask: {data.get('best_bid')}/{data.get('best_ask')}"
                )

    def ws_reader_thread(self):
        """
        WebSocket reader thread.

        KEY RULE: Only write to orderbook_cache if state == CONNECTED_WS

        Every frame is parsed and handled inline in on_message. (A
        variant that handed frames to a second thread was built 2026-09-28
        for the "slow consumer" disconnects and removed 2026-09-29: the
        disconnects turned out to come from the network path, with this
        thread never behind — see feed_lag.py.)
        """

        def on_open(ws):
            if not self.should_run or not self.market_open:
                # Shutdown was signaled (or the market already ended) while
                # this connection attempt was in flight — don't subscribe,
                # just let it close. Avoids a pointless subscribe-then-
                # immediately-error-on-send race against end_market()'s
                # ws.close() call.
                logger.debug("[WS] Opened after shutdown/market-end signal, closing")
                try:
                    ws.close()
                except Exception:
                    pass
                return

            logger.info("[WS] Connected")
            self.ws_connected = True
            self.ws_reconnect_attempts = 0
            # Updates missed while disconnected make the live books
            # unknowable; the snapshot sent on subscribe re-arms them.
            self._live_ok = {"yes": False, "no": False}
            # Likewise how current the view is: only this connection's own
            # messages can say (see feed_behind_s()).
            self.last_server_ts = None
            self.feed_lag_s = None

            # Subscribe
            subscription = {
                "type": "market",
                "assets_ids": [self.config.token_yes_id, self.config.token_no_id],
                "custom_feature_enabled": True
            }
            try:
                ws.send(orjson.dumps(subscription))
                logger.info("[WS] Subscribed")
            except Exception as e:
                # The socket can close between run_forever() opening it and
                # this callback running (e.g. the server rejects a
                # subscription for an already-resolved market). on_close
                # will fire right after and drive the normal reconnect/stop
                # logic, so just log instead of letting this bubble up as
                # a second, more confusing error from the ws library itself.
                logger.warning(f"[WS] Subscribe failed, connection likely already closing: {e}")
                return

            # Transition to CONNECTED_WS now that the socket is genuinely
            # open — whether this is the very first connect
            # (INITIALIZING) or a reconnect after a drop (RECONNECTING).
            # Driven by the real WS handshake completing here, not by a
            # fixed-delay guess elsewhere (see start_market(), which used
            # to force this state after a blind time.sleep(2) regardless
            # of whether the socket had actually opened yet).
            if self.get_state() in (DataSource.RECONNECTING, DataSource.INITIALIZING):
                self.set_state(DataSource.CONNECTED_WS)
        
        def on_message(ws, message):
            if is_pong(message):
                # Reply to our text PING: the socket is alive, but this is
                # not market data. It must not refresh last_ws_message_time,
                # which heartbeat_monitor_thread reads as "data is flowing"
                # (a PONG every 10 s would otherwise mask a silent feed and
                # flip REST fallback back to WS without any data).
                with self.stats_lock:
                    self.stats["pongs_received"] += 1
                return
            now = time.time()
            self.last_ws_message_time = now  # WS-alive signal
            self._process_message(now, message)

        def on_error(ws, error):
            logger.error(f"[WS] Error: {error}")
            with self.stats_lock:
                self.stats["ws_errors"] += 1
        
        def on_close(ws, code, msg):
            self.ws_connected = False

            # Polymarket closes the socket itself with this reason once
            # every subscribed token has resolved — a normal end-of-market
            # event, not a dropped connection. Reconnecting would just
            # resubscribe to the same now-resolved tokens and immediately
            # get closed again (the "Connection is already closed" /
            # repeated-404 pattern this used to produce). Treat it as the
            # market ending rather than something to recover from.
            close_text = str(msg or "")
            if "resolved" in close_text.lower():
                logger.info(f"[WS] Closed — market resolved server-side (code: {code})")
                self.market_open = False
                return

            logger.warning(f"[WS] Closed (code: {code}): {close_text}")

            if self.get_state() == DataSource.CONNECTED_WS:
                self.set_state(DataSource.RECONNECTING)
        
        # Connection loop with exponential backoff
        while self.should_run:
            try:
                if not self.ws_connected:
                    # Calculate backoff delay
                    delay = min(
                        self.config.ws_reconnect_initial_delay * 
                        (self.config.ws_reconnect_multiplier ** self.ws_reconnect_attempts),
                        self.config.ws_reconnect_max_delay
                    )
                    
                    # Add jitter
                    import random
                    jitter = random.uniform(-self.config.ws_reconnect_jitter, self.config.ws_reconnect_jitter)
                    delay = delay * (1 + jitter)
                    
                    if self.ws_reconnect_attempts > 0:
                        logger.info(f"[WS] Reconnecting in {delay:.1f}s (attempt {self.ws_reconnect_attempts + 1})")
                        time.sleep(delay)

                    # Re-check after the backoff sleep (which can run for up
                    # to ws_reconnect_max_delay, 20s): a shutdown or market-
                    # end signal that arrived during the sleep must be
                    # honored here, before opening a new connection — the
                    # `while self.should_run` check at the top of the loop
                    # only runs once per full connect cycle, so without this
                    # a session that already ended would still open one more
                    # WS connection to a market whose tokens are resolved.
                    if not self.should_run or not self.market_open:
                        logger.debug("[WS] Shutdown/market-end signaled during backoff, stopping reconnect loop")
                        break

                    self.ws_reconnect_attempts += 1

                    if self.ws_reconnect_attempts > self.config.ws_reconnect_max_attempts:
                        logger.error("[WS] Max reconnect attempts reached")
                        self.set_state(DataSource.STALE_FALLBACK_REST)
                        time.sleep(5)
                        continue
                    
                    logger.info("[WS] Connecting...")
                    self.ws = websocket.WebSocketApp(
                        self.config.ws_url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close
                    )
                    if self.config.ws_text_ping_interval > 0:
                        start_text_ping(self.ws, lambda: self.ws, lambda: self.should_run,
                                        self.config.ws_text_ping_interval, name="PolymarketTextPing")

                    # run_forever() returns only once the socket is
                    # done. ws_connected is normally cleared by on_close,
                    # but if that callback never fires — an internal error
                    # in the ws library, an exception inside the handler —
                    # the flag stays True, the `if not self.ws_connected`
                    # above is skipped on the next iteration, and this
                    # while-loop spins at 100% CPU with no sleep anywhere
                    # in it. Clearing it unconditionally after the call
                    # makes the backoff path unreachable-by-accident.
                    self.ws.run_forever(
                        ping_interval=self.config.ws_ping_interval,
                        ping_timeout=self.config.ws_ping_timeout,
                        # A bigger OS-level receive buffer gives this thread
                        # more slack to fall behind under GIL contention with
                        # this process's other ~10+ threads during message
                        # bursts (up to 700-800/s seen during high BTC
                        # volatility) before the kernel's default buffer
                        # fills and TCP backpressure makes Polymarket's
                        # server see us as a "slow consumer" (WS close code
                        # 1013) and drop the connection. Doesn't eliminate
                        # the underlying GIL contention, just raises the
                        # threshold before it causes a disconnect.
                        sockopt=[(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)],
                        # The larger remaining cost per frame — see
                        # BotConfig.ws_skip_utf8_validation.
                        skip_utf8_validation=self.config.ws_skip_utf8_validation,
                    )
                    self.ws_connected = False

            except Exception as e:
                logger.error(f"[WS] Connection error: {e}")
                with self.stats_lock:
                    self.stats["ws_errors"] += 1
                time.sleep(2)

    def _process_message(self, receive_ts: float, message: bytes):
        """One WS frame: parse + dispatch."""
        try:
            current_state = self.get_state()
            parsed = orjson.loads(message)

            # NOTE: Polymarket's CLOB market channel can send either a
            # single event object OR a JSON array of event objects in
            # one WS frame (e.g. the initial snapshot dump right after
            # subscribing sends multiple "book" events at once, one per
            # asset_id). Handle both shapes instead of assuming a dict.
            if isinstance(parsed, list):
                for item in parsed:
                    if isinstance(item, dict):
                        self._handle_ws_event(item, current_state, receive_ts)
                    elif logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[WS] Skipping non-dict list item: {type(item)}")
            elif isinstance(parsed, dict):
                self._handle_ws_event(parsed, current_state, receive_ts)
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[WS] Unexpected message type: {type(parsed)}")

        except orjson.JSONDecodeError:
            logger.error("[WS] JSON decode error")
            with self.stats_lock:
                self.stats["ws_errors"] += 1
        except Exception as e:
            logger.error(f"[WS] Message error: {e}")
            with self.stats_lock:
                self.stats["ws_errors"] += 1

    # ========================================================================
    # HEARTBEAT MONITOR THREAD
    # ========================================================================

    def heartbeat_monitor_thread(self):
        """
        Monitor WS staleness and trigger state transitions

        Logic:
        - INITIALIZING: If the WS hasn't actually opened within the same
          timeout → STALE_FALLBACK_REST (it may never open at all)
        - CONNECTED_WS: If 45s no message → STALE_FALLBACK_REST
        - STALE_FALLBACK_REST: If WS recovers + fresh data → CONNECTED_WS
        """

        while self.should_run:
            try:
                current_state = self.get_state()
                elapsed = time.time() - self.last_ws_message_time

                # ─────────────────────────────────────────────────────
                # STATE 0: INITIALIZING — on_open() (ws_reader_thread)
                # transitions this to CONNECTED_WS the moment the socket
                # genuinely opens; if that never happens (slow/failed
                # handshake), nothing else would ever move this bot off
                # INITIALIZING — rest_fallback_poller_thread only polls
                # in STALE_FALLBACK_REST — so treat a stuck INITIALIZING
                # the same as an established connection going stale.
                # last_ws_message_time is set at __init__, effectively
                # "cycle start" for a fresh per-cycle PolymarketBot, so
                # this bounds it by the same ws_heartbeat_timeout instead
                # of a separate/longer wait.
                # ─────────────────────────────────────────────────────

                if current_state == DataSource.INITIALIZING:
                    if elapsed > self.config.ws_heartbeat_timeout:
                        logger.warning(f"[HB] WS never connected ({elapsed:.0f}s), switching to REST fallback")
                        self.set_state(DataSource.STALE_FALLBACK_REST)
                        if self.ws:
                            try:
                                self.ws.close()
                            except:
                                pass

                # ─────────────────────────────────────────────────────
                # STATE 1: CONNECTED_WS
                # ─────────────────────────────────────────────────────

                elif current_state == DataSource.CONNECTED_WS:
                    if elapsed > self.config.ws_heartbeat_timeout:
                        logger.warning(f"[HB] WS stale ({elapsed:.0f}s), switching to REST fallback")
                        self.set_state(DataSource.STALE_FALLBACK_REST)

                        # Close WS gracefully
                        if self.ws:
                            try:
                                self.ws.close()
                            except:
                                pass

                # ─────────────────────────────────────────────────────
                # STATE 2: STALE_FALLBACK_REST
                # ─────────────────────────────────────────────────────

                elif current_state == DataSource.STALE_FALLBACK_REST:
                    # Check if WS recovered (fresh data within 5 seconds)
                    if self.ws_connected and elapsed < 5:
                        logger.info(f"[HB] WS recovered, switching back (elapsed: {elapsed:.1f}s)")
                        self.set_state(DataSource.CONNECTED_WS)
                        self.rest_polling_active = False
                
                time.sleep(self.config.ws_heartbeat_check_interval)
            
            except Exception as e:
                logger.error(f"[HB] Error: {e}")
                time.sleep(self.config.ws_heartbeat_check_interval)
    
    # ========================================================================
    # REST FALLBACK POLLER THREAD
    # ========================================================================
    
    def rest_fallback_poller_thread(self):
        """
        REST fallback poller
        
        KEY RULE: Only poll if state == STALE_FALLBACK_REST
        """
        
        poll_count = 0
        
        while self.should_run:
            try:
                current_state = self.get_state()

                # Also require market_open: the heartbeat can leave the
                # state at STALE_FALLBACK_REST for a few seconds after a
                # market has actually resolved (state only flips back on
                # the next heartbeat tick), which otherwise means this
                # loop keeps hitting /book for tokens that no longer exist
                # and logging a fresh 404 every rest_poll_interval.
                if current_state == DataSource.STALE_FALLBACK_REST and self.market_open:
                    # ─────────────────────────────────────────────────
                    # ONLY POLL if state == STALE_FALLBACK_REST
                    # ─────────────────────────────────────────────────

                    self.rest_polling_active = True
                    poll_count += 1

                    try:
                        logger.debug(f"[REST] Fallback poll #{poll_count}")

                        # Fetch both tokens' books — matches the WS path,
                        # which tracks YES and NO separately.
                        poll_server_ts = []
                        for token_id, attr in (
                            (self.config.token_yes_id, "orderbook_cache_yes"),
                            (self.config.token_no_id, "orderbook_cache_no"),
                        ):
                            response = requests.get(
                                f"{self.config.rest_url}/book",
                                params={"token_id": token_id},
                                timeout=self.config.rest_timeout
                            )
                            response.raise_for_status()

                            book_data = response.json()
                            try:
                                server_ts = float(book_data.get("timestamp")) / 1000.0
                                poll_server_ts.append(server_ts)
                            except (TypeError, ValueError):
                                server_ts = None

                            with self.cache_lock:
                                setattr(self, attr, Orderbook(
                                    bids=book_data.get("bids", []),
                                    asks=book_data.get("asks", []),
                                    timestamp=time.time(),
                                    source="REST",
                                    server_ts=server_ts,
                                ))

                        # A polled book is as old as when Polymarket
                        # generated it, however recently we fetched it —
                        # the older of the two, so feed_behind_s() holds
                        # for both books (unknown if either lacked one).
                        self.last_server_ts = min(poll_server_ts) if len(poll_server_ts) == 2 else None

                        with self.stats_lock:
                            self.stats["rest_polls"] += 1

                        logger.debug("[REST] Cache updated")

                    except requests.exceptions.HTTPError as e:
                        if e.response is not None and e.response.status_code == 404:
                            # The CLOB stops serving a token's book the
                            # instant its market resolves — a 404 here means
                            # the market ended, not a transient API failure.
                            # Stop polling instead of repeating this every
                            # rest_poll_interval until the session tears down.
                            logger.info("[REST] Token no longer served (market resolved), stopping REST fallback")
                            self.market_open = False
                        else:
                            logger.error(f"[REST] Poll error: {e}")
                            with self.stats_lock:
                                self.stats["rest_errors"] += 1

                    except Exception as e:
                        logger.error(f"[REST] Poll error: {e}")
                        with self.stats_lock:
                            self.stats["rest_errors"] += 1

                    # Poll every 5 seconds
                    time.sleep(self.config.rest_poll_interval)
                
                else:
                    # Not in fallback state, sleep
                    self.rest_polling_active = False
                    time.sleep(1)
            
            except Exception as e:
                logger.error(f"[REST] Thread error: {e}")
                time.sleep(1)
    
    # ========================================================================
    # MARKET LIFECYCLE
    # ========================================================================
    
    def discover_market(self):
        """
        Discover market using Gamma API (one-time at startup)
        """
        try:
            logger.info("[DISCOVERY] Fetching market metadata...")
            
            response = requests.get(
                f"{self.config.gamma_url}/markets",
                params={"limit": 100, "active": True},
                timeout=self.config.gamma_timeout
            )
            response.raise_for_status()
            
            markets = response.json()
            logger.info(f"[DISCOVERY] Found {len(markets)} markets")
            
            return markets
        
        except Exception as e:
            logger.error(f"[DISCOVERY] Error: {e}")
            return []
    
    def start_market(self, full_threads: bool = True):
        """
        Start 5-minute trading session.

        Args:
            full_threads: False starts only WSReader — used by
                run.py's _prewarm_next_market(), which creates a whole
                second PolymarketBot ~20s before the current cycle ends so
                the next cycle's WS is already connected+subscribed. With
                full_threads=True (the default) that briefly doubles this
                class's thread count (4 -> 8) for that overlap window,
                which is exactly the kind of GIL contention that produces
                "slow consumer" WS disconnects (see ws_reader_thread's
                SO_RCVBUF comment). A prewarmed bot only needs its socket
                open and subscribed; it isn't "the" active bot yet, so its
                heartbeat/REST-fallback/stats threads have nothing
                meaningful to do until _load_market() promotes it — call
                start_remaining_threads() at that point instead.
        """

        logger.info("[MARKET] Opening...")
        self.market_open = True
        self.market_start_time = time.time()
        self.market_end_time = self.market_start_time + self.config.market_duration_seconds

        # This class only maintains the WS/REST orderbook_cache — signal
        # detection and position tracking happen in run.py's
        # BotController (_on_market_tick(), via MultiFactorSignalEngine +
        # PositionManager, both backed by bot_trades.db). An earlier
        # version of this class had its own separate signal/paper-order
        # loop and SQLite file (bot_orders.db); both were dead code that
        # nothing downstream ever read, and have been removed.
        threading.Thread(target=self.ws_reader_thread, daemon=True, name="WSReader").start()

        if full_threads:
            self.start_remaining_threads()

        # Brief head start before returning control — NOT a state
        # transition. on_open() (ws_reader_thread) now handles
        # INITIALIZING -> CONNECTED_WS for real, driven by the actual WS
        # handshake completing, instead of this forcing the state after a
        # fixed delay regardless of whether the socket had actually
        # opened. If it's still not connected after this,
        # heartbeat_monitor_thread's own INITIALIZING handling falls back
        # to REST once ws_heartbeat_timeout is reached.
        time.sleep(2)
        if self.get_state() != DataSource.CONNECTED_WS:
            logger.warning(
                f"[MARKET] WS not yet connected after 2s startup wait "
                f"(state={self.get_state().value}) — will fall back to REST "
                f"after {self.config.ws_heartbeat_timeout}s if it doesn't connect"
            )

    def start_remaining_threads(self):
        """
        Start Heartbeat/RESTPoller/Stats — the threads a prewarmed bot
        (start_market(full_threads=False)) deferred until it's actually
        promoted to the active bot in _load_market(). Idempotent: safe to
        call on a bot that was already started with full_threads=True.
        """
        if getattr(self, "_remaining_threads_started", False):
            return
        self._remaining_threads_started = True

        for t in (
            threading.Thread(target=self.heartbeat_monitor_thread, daemon=True, name="Heartbeat"),
            threading.Thread(target=self.rest_fallback_poller_thread, daemon=True, name="RESTPoller"),
            threading.Thread(target=self._stats_logger_thread, daemon=True, name="Stats"),
        ):
            t.start()
    
    def end_market(self):
        """End trading session"""

        logger.info("[MARKET] Closing...")
        self.market_open = False

        # This instance's WS/heartbeat/REST/stats threads all loop on
        # should_run. run.py's BotController already sets this before
        # calling end_market(), but a pre-warmed bot that never gets
        # promoted (see _prewarm_next_market()) has no other teardown
        # path — without this, its threads run forever reconnecting to
        # and REST-polling a market instance nothing else references.
        self.should_run = False

        # NOTE: this used to sleep(5) here "waiting for resolution", but
        # nothing downstream needs that: we don't wait for Polymarket to
        # resolve anything on this clock — run.py's BotController settles
        # positions using its own Binance price read, independent
        # of this class. Removing the wait shaves ~5s off every 5-minute
        # cycle transition.

        # Close WS
        if self.ws:
            try:
                self.ws.close()
            except:
                pass

        logger.info("[MARKET] Closed")

    def _stats_logger_thread(self):
        """Log statistics periodically"""
        
        while self.should_run:
            try:
                time.sleep(self.config.stats_log_interval)
                
                with self.stats_lock:
                    stats = self.stats.copy()

                # Sampled, not locked; the interval max is
                # reset here (a lost update from the WS thread in between
                # only loses one sample of a max).
                lag_max_ms = self._lag_interval_max_s * 1000
                self._lag_interval_max_s = 0.0
                behind = self.feed_behind_s()
                baseline = POLYMARKET_FEED_LAG.baseline_s

                logger.info(
                    f"STATS: WS_msgs={stats['ws_messages_received']}, "
                    f"WS_errs={stats['ws_errors']}, "
                    f"REST_polls={stats['rest_polls']}, "
                    f"REST_errs={stats['rest_errors']}, "
                    f"State_changes={stats['state_changes']}, "
                    f"book_snaps={stats['book_snapshots']}, "
                    f"price_changes_applied={stats['price_changes_applied']}, "
                    f"price_change_mismatches={stats['price_change_mismatches']}, "
                    f"price_changes_unverifiable={stats['price_changes_unverifiable']}, "
                    f"price_changes_ignored={stats['price_changes_ignored']}, "
                    f"best_bid_ask_ignored={stats['best_bid_ask_ignored']}, "
                    f"pongs={stats['pongs_received']}, "
                    # How far behind Polymarket the feed ran — see
                    # feed_lag.py. behind = now; lag_max = worst message
                    # since the last STATS line; baseline = clock offset
                    # + minimum path latency it is measured against.
                    f"feed_behind_ms={'n/a' if behind is None else f'{behind * 1000:.0f}'}, "
                    f"feed_lag_max_ms={lag_max_ms:.0f}, "
                    f"feed_lag_baseline_ms={'n/a' if baseline is None else f'{baseline * 1000:.0f}'}"
                )
            
            except Exception as e:
                logger.error(f"[STATS] Error: {e}")
    
    # ========================================================================
    # MAIN LOOP
    # ========================================================================
    
    def run(self):
        """Main bot loop"""
        
        try:
            logger.info("="*80)
            logger.info("POLYMARKET 5-MINUTE BTC BOT STARTING")
            logger.info("="*80)
            
            # Discover market
            self.discover_market()
            
            # Start market
            self.start_market()
            
            # Run until market closes
            while self.should_run and time.time() < (self.market_end_time or time.time() + 1):
                time.sleep(1)
            
            # End market
            self.end_market()
            
            logger.info("="*80)
            logger.info("BOT SHUTDOWN COMPLETE")
            logger.info("="*80)
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Bot error: {e}", exc_info=True)
        finally:
            self.should_run = False

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    config = BotConfig()
    bot = PolymarketBot(config)
    bot.run()

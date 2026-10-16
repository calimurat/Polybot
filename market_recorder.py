"""
MARKET RECORDER — a 100 ms tape of every price source a 5-minute BTC Up/Down
strategy might use, for EVERY window, whether the bot trades it or not.

Added 2026-10-02 at the user's request ("botu çalıştırdığımızda işlem için
aynı anda da ayrı olarak veri toplasın"), for simulations the bot's own
records cannot support:
  - Trade-time records (signal_features) only show the moments today's
    strategy picked, so another threshold, timing or latency cannot be
    replayed from them.
  - book_ticks (1 s, bot_trades.db) was enough for threshold/timing replays,
    but on 2026-10-02 Polymarket was measured absorbing 79-96% of a BTC jump
    within the same recorded second: order flow, lead-lag between venues and
    a VPS's latency (~150-200 ms) only show up below one second.
  - 100 ms keeps latency simulations possible in 100 ms steps at ~1/80 of the
    raw message volume. Order flow and traded volume are accumulated over
    each 100 ms, so no flow is lost — only the ordering inside a row.

Separate process, own database (market_tape.db): it shares nothing with the
trading bot, so it cannot slow the bot's decisions or contend for
bot_trades.db. run.py starts it (Config.record_market_tape) and stops it;
it also exits by itself when its parent dies. It can be run standalone, and
`python market_recorder.py --status` summarizes what has been recorded.

Sources (public WebSockets, no keys; rates measured 2026-10-02):
  bnp  Binance USDⓈ-M perpetual  btcusdt@depth5@100ms (~9/s), @aggTrade (~22/s)
  bns  Binance spot              btcusdt@depth5@100ms (~9/s), @aggTrade (~9/s)
  cb   Coinbase                  BTC-USD ticker (~8/s)
  kr   Kraken                    BTC/USD ticker, bbo trigger (~32/s)
  okx  OKX                       BTC-USDT bbo-tbt (~17/s)
  byb  Bybit                     BTCUSDT spot orderbook.1 (~21/s)
  cl   Polymarket RTDS           Chainlink btc/usd and its 60 s TWAP
  pm   Polymarket CLOB           the live window's Up and Down books (the
                                 next window's from PREWARM_S before it starts)
The perp's bookTicker stream (550-850 msg/s) was rejected on purpose: through
the VPN tunnel whose congestion caused the bot's earlier disconnects it would
add ~1.4 Mbit/s; depth5@100ms carries the same top of book every 100 ms at
~3 KB/s.

Tables:
  tape     one row per ~100 ms; columns in TAPE_COLUMNS. Per source:
             <p>_bid/_ask/_last       latest prices at the snapshot
             <p>_age_ms               snapshot time - when that update arrived
             <p>_lag_ms               arrival - the source's own timestamp
                                      (local clock vs theirs, so offsets apply)
           Binance also: _bid_qty/_ask_qty (top level), _bid_depth5/_ask_depth5,
           _ofi (order-flow imbalance at the top level since the previous row,
           BTC, Cont-Kukanov-Stoikov), _buy_vol/_sell_vol/_trades (taker
           volume since the previous row), _trade_age_ms.
           Polymarket per side (up/down): bid/ask, top sizes, vwap30 (average
           price to buy 30 shares walking the asks — the bot's order size),
           last trade, vol (shares traded since the previous row); pm_ok = 1
           while both books are verified (see poly_book.py).
  windows  one row per 5-minute window: slug, condition id, token ids, the
           Gamma market JSON when found and again after resolution, and the
           official result (data-api /resolutions).
"""

import argparse
import fcntl
import json
import logging
import math
import os
import queue
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional

import orjson
import requests

from sqlite_keys import ensure_autoincrement_key
from poly_book import apply_price_changes, best_ask, best_bid, levels_from, size_at, vwap_buys
from ws_feed import Feed
from ws_heartbeat import is_pong
import engine_shadow
import lab_shadow
import paper_ledger

HERE = os.path.dirname(os.path.abspath(__file__))
logger = logging.getLogger("MarketRecorder")

STEP_S = 0.1                  # tape resolution
WINDOW_S = 300                # 5-minute markets
PREWARM_S = 60                # connect to the next window this early
PM_FEED_STALL_S = 60.0        # Polymarket books can be quiet for a while
RESOLUTION_GIVE_UP_S = 1800   # stop chasing a window's result after 30 min
STATUS_INTERVAL_S = 300
VWAP_SHARES = 30.0            # Config.trade_size
EXTRA_VWAP_SIZES = (10, 50, 100, 200)   # other order sizes, for slippage studies
GAMMA_URL = "https://gamma-api.polymarket.com"
SIDES = ("up", "down")
SLUG_PREFIX = "btc-updown-5m-"
EXIT_ALREADY_RUNNING = 3      # run.py does not restart a recorder that exits with this
# A failed database write is retried with the next batch; after this long
# without success the tape rows are dropped (memory) but window inserts and
# resolution updates are kept and retried (2026-10-05).
WRITE_RETRY_S = 120.0


# ============================================================================
# SCHEMA
# ============================================================================

def _tape_columns():
    cols = [("ts", "REAL"), ("window_start", "INTEGER")]
    for p in ("bnp", "bns"):
        cols += [(f"{p}_{c}", "REAL") for c in (
            "bid", "ask", "bid_qty", "ask_qty", "bid_depth5", "ask_depth5",
            "ofi", "last", "buy_vol", "sell_vol")]
        cols += [(f"{p}_{c}", "INTEGER") for c in ("trades", "age_ms", "lag_ms", "trade_age_ms")]
    for p in ("cb", "kr", "okx", "byb"):
        cols += [(f"{p}_{c}", "REAL") for c in ("bid", "ask", "last")]
        cols += [(f"{p}_{c}", "INTEGER") for c in ("age_ms", "lag_ms")]
    cols += [("cl_price", "REAL"), ("cl_age_ms", "INTEGER"), ("cl_lag_ms", "INTEGER"),
             ("cl_twap60", "REAL"), ("cl_twap60_age_ms", "INTEGER")]
    for s in ("up", "down"):
        cols += [(f"pm_{s}_{c}", "REAL") for c in (
            "bid", "ask", "bid_qty", "ask_qty", "vwap30", "last", "vol")]
    cols += [("pm_age_ms", "INTEGER"), ("pm_lag_ms", "INTEGER"), ("pm_ok", "INTEGER")]
    # Added 2026-10-04: buy VWAP at other order sizes, so slippage can be
    # simulated for sizes other than the bot's 30 shares (pm_*_vwap30).
    for s in ("up", "down"):
        cols += [(f"pm_{s}_vwap{n}", "REAL") for n in EXTRA_VWAP_SIZES]
    return tuple(cols)


TAPE_COLUMNS = _tape_columns()
_TAPE_INSERT = ("INSERT INTO tape (" + ", ".join(n for n, _ in TAPE_COLUMNS) + ") VALUES ("
                + ",".join("?" * len(TAPE_COLUMNS)) + ")")
_SCHEMA = (
    # id: a stable key for parquet_sync.py's export and cleanup (sqlite_keys.py)
    "CREATE TABLE IF NOT EXISTS tape (id INTEGER PRIMARY KEY AUTOINCREMENT, "
    + ", ".join(f"{n} {t}" for n, t in TAPE_COLUMNS) + ")",
    "CREATE INDEX IF NOT EXISTS idx_tape_window ON tape(window_start)",
    lab_shadow.SCHEMA,          # log-only strategy shadow (lab_shadow.py, 2026-10-10)
    engine_shadow.SCHEMA,       # the bot's signal engine on the tape (engine_shadow.py, 2026-10-12)
    paper_ledger.SCHEMA,        # stale-book kept like paper-bot trades (paper_ledger.py, 2026-10-12)
    paper_ledger.INDEX,
    """CREATE TABLE IF NOT EXISTS windows (
        window_start INTEGER PRIMARY KEY, window_end INTEGER, slug TEXT,
        condition_id TEXT, token_up TEXT, token_down TEXT,
        discovered_at REAL, market_json TEXT,
        official_resolution TEXT, resolved_at REAL, market_json_closed TEXT)""",
)


def _ms(seconds: Optional[float]) -> Optional[int]:
    return None if seconds is None else int(round(seconds * 1000))


def _f(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# ============================================================================
# STATE
# ============================================================================

class Quote:
    """Latest state of one exchange source, plus flow accumulated since the
    previous tape row. Mutated only under Recorder.lock."""

    __slots__ = ("bid", "ask", "bid_qty", "ask_qty", "bid_depth5", "ask_depth5", "last",
                 "rx", "ex_ts", "trade_rx", "ofi", "buy_vol", "sell_vol", "trades",
                 "_prev_top")

    def __init__(self):
        for name in self.__slots__:
            setattr(self, name, None)
        self.ofi = self.buy_vol = self.sell_vol = 0.0
        self.trades = 0

    def set_top(self, bid, ask, rx, ex_ts=None, bid_qty=None, ask_qty=None):
        self.bid, self.ask, self.bid_qty, self.ask_qty = bid, ask, bid_qty, ask_qty
        self.rx, self.ex_ts = rx, ex_ts

    def apply_depth(self, bids, asks, rx, ex_ts):
        """bids/asks: [(price, qty), ...] best first. Updates the top of book
        and accumulates order-flow imbalance against the previous top."""
        if not bids or not asks:
            return
        (pb, qb), (pa, qa) = bids[0], asks[0]
        prev = self._prev_top
        if prev is not None:
            ppb, pqb, ppa, pqa = prev
            self.ofi += ((qb if pb >= ppb else 0.0) - (pqb if pb <= ppb else 0.0)
                         - (qa if pa <= ppa else 0.0) + (pqa if pa >= ppa else 0.0))
        self._prev_top = (pb, qb, pa, qa)
        self.set_top(pb, pa, rx, ex_ts, qb, qa)
        self.bid_depth5 = sum(q for _, q in bids[:5])
        self.ask_depth5 = sum(q for _, q in asks[:5])

    def apply_trade(self, price, qty, taker_sells, rx):
        self.last, self.trade_rx = price, rx
        if taker_sells:
            self.sell_vol += qty
        else:
            self.buy_vol += qty
        self.trades += 1

    def reset_flow(self):
        self.ofi = self.buy_vol = self.sell_vol = 0.0
        self.trades = 0

    def forget_top(self):
        """After a reconnect the previous top of book is from before the gap:
        order-flow imbalance against it would be a spike of missed flow, not
        flow. The next depth message starts the comparison afresh."""
        self._prev_top = None


class PolyWindow:
    """One 5-minute market: its Gamma metadata, books and CLOB connection."""

    def __init__(self, start: int):
        self.start = start
        self.end = start + WINDOW_S
        self.slug = f"{SLUG_PREFIX}{start}"
        self.market = None            # market_loader.MarketData
        self.market_json = None
        self.next_discovery = 0.0
        self.feed: Optional[Feed] = None
        # Books by side ("up"/"down"), kept by the bot's own rules
        # (poly_book.py): levels[side] = {"bids": {...}, "asks": {...}},
        # ok[side] = verified since the last snapshot.
        self.levels = {s: {"bids": {}, "asks": {}} for s in SIDES}
        self.ok = {s: False for s in SIDES}
        self.last = {s: None for s in SIDES}   # last trade price
        self.vol = {s: 0.0 for s in SIDES}     # shares traded since the previous tape row
        self.rx = None
        self.server_ts = None
        self.official = None
        self.next_resolution = self.end + 60
        self.row_written = False
        self.busy = False             # a Gamma/resolution lookup is in flight


# ============================================================================
# RECORDER
# ============================================================================

class Recorder:
    def __init__(self, db_path: str, parent_pid: Optional[int] = None,
                 parquet_dir: Optional[str] = None, bot_db: Optional[str] = None,
                 retention_days: Optional[float] = None):
        self.db_path = db_path
        self.parent_pid = parent_pid
        # Parquet copy of this tape and the bot's database (parquet_sync.py);
        # None = off. A missing pyarrow turns it off with a warning, never
        # stops the recording itself.
        self.parquet = None
        if parquet_dir:
            try:
                from parquet_sync import ParquetSync
                self.parquet = ParquetSync([p for p in (bot_db, db_path) if p], parquet_dir,
                                           retention_days=retention_days)
            except Exception as e:
                logger.warning(f"[REC] Parquet copy disabled: {e}")
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.q: "queue.Queue" = queue.Queue()
        self.src = {p: Quote() for p in ("bnp", "bns", "cb", "kr", "okx", "byb")}
        self.cl = {"price": None, "rx": None, "ex_ts": None, "twap60": None, "twap60_rx": None}
        self.windows: Dict[int, PolyWindow] = {}
        self.rows_written = 0
        self.last_write = None
        self.write_errors = 0
        self._next_settle = 0.0       # paper_ledger: when open positions are next closed against the official results
        # Log-only strategy shadow fed by every snapshot (lab_shadow.py). It
        # takes the windows' opening price from this tape (the bot saves its own
        # only when a window ends); any error in it is logged and never stops
        # the recording.
        self.lab = None
        self.lab_errors = 0
        try:
            self.lab = lab_shadow.LabShadow(
                [n for n, _ in TAPE_COLUMNS],
                self._lab_emit,
                lab_shadow.tape_opening_price(self.db_path))
        except Exception as e:
            logger.warning(f"[LAB] strategy shadow disabled: {e}")
        # The bot's own signal engine run on every snapshot, log-only (engine_shadow.py): K, R1, R2, R3 without the bot trading.
        self.engine = None
        self.engine_errors = 0
        try:
            self.engine = engine_shadow.EngineShadow(
                [n for n, _ in TAPE_COLUMNS],
                lambda params: self.q.put(("sql", (engine_shadow.INSERT, params))),
                lab_shadow.tape_opening_price(self.db_path))
        except Exception as e:
            logger.warning(f"[ENGINE] engine shadow disabled: {e}")

        # Imported here, not at module top, so `--status` works without them.
        from market_loader import MarketLoader
        from external_connections import ResolutionWatcher
        self.market_parser = MarketLoader()
        self.resolutions = ResolutionWatcher()   # passive: only its REST lookup is used

        self.feeds = [
            Feed("binance-perp-depth", ["wss://fstream.binance.com/public/ws/btcusdt@depth5@100ms",
                                        "wss://fstream.binance.com/ws/btcusdt@depth5@100ms"],
                 lambda m, rx: self._binance_depth(self.src["bnp"], m, rx, perp=True),
                 on_open=lambda: self._forget_top("bnp")),
            Feed("binance-perp-trades", ["wss://fstream.binance.com/market/ws/btcusdt@aggTrade"],
                 lambda m, rx: self._binance_trade(self.src["bnp"], m, rx)),
            Feed("binance-spot-depth", ["wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"],
                 lambda m, rx: self._binance_depth(self.src["bns"], m, rx, perp=False),
                 on_open=lambda: self._forget_top("bns")),
            Feed("binance-spot-trades", ["wss://stream.binance.com:9443/ws/btcusdt@aggTrade"],
                 lambda m, rx: self._binance_trade(self.src["bns"], m, rx)),
            Feed("coinbase", ["wss://ws-feed.exchange.coinbase.com"], self._coinbase,
                 subscribe={"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}),
            Feed("kraken", ["wss://ws.kraken.com/v2"], self._kraken,
                 subscribe={"method": "subscribe", "params": {
                     "channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "bbo"}}),
            Feed("okx", ["wss://ws.okx.com:8443/ws/v5/public"], self._okx,
                 subscribe={"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT"}]},
                 text_ping=("ping", 20)),
            Feed("bybit", ["wss://stream.bybit.com/v5/public/spot"], self._bybit,
                 subscribe={"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]},
                 text_ping=({"op": "ping"}, 20)),
            # Same subscription shape the bot used until 2026-09-29
            # (backups/pre_chainlink_removal_2026-09-29).
            Feed("chainlink-rtds", ["wss://ws-live-data.polymarket.com"], self._rtds,
                 subscribe={"action": "subscribe", "subscriptions": [
                     {"topic": "crypto_prices_chainlink", "type": "*",
                      "filters": "{\"symbol\":\"btc/usd\"}"},
                     {"topic": "crypto_prices_twap_sixty", "type": "update",
                      "filters": "{\"symbol\":\"btc/usd\"}"}]},
                 # Chainlink updates ~2/s; on 2026-10-03 the socket stayed
                 # open but silent for 58 s before a 60 s limit caught it.
                 stall_s=10.0),
        ]

    # ---- exchange handlers (WS threads) ----------------------------------

    def _forget_top(self, source: str):
        with self.lock:
            self.src[source].forget_top()

    def _binance_depth(self, q: Quote, message, rx, perp: bool):
        d = orjson.loads(message)
        bids, asks = (d.get("b"), d.get("a")) if perp else (d.get("bids"), d.get("asks"))
        bids = sorted(((float(p), float(s)) for p, s in bids or []), reverse=True)
        asks = sorted((float(p), float(s)) for p, s in asks or [])
        ex = d.get("E")
        with self.lock:
            q.apply_depth(bids, asks, rx, ex / 1000.0 if ex else None)

    def _binance_trade(self, q: Quote, message, rx):
        d = orjson.loads(message)
        if "p" not in d:
            return
        with self.lock:
            # "m": the buyer is the maker, i.e. the taker sold.
            q.apply_trade(float(d["p"]), float(d.get("q", 0)), bool(d.get("m")), rx)

    def _coinbase(self, message, rx):
        d = orjson.loads(message)
        kind = d.get("type")
        if kind == "error":
            logger.warning(f"[coinbase] {d}")
            return
        if kind != "ticker":
            return
        ex = d.get("time")
        ex_ts = datetime.fromisoformat(ex).timestamp() if ex else None
        q = self.src["cb"]
        with self.lock:
            q.set_top(_f(d.get("best_bid")), _f(d.get("best_ask")), rx, ex_ts)
            q.last = _f(d.get("price"))

    def _kraken(self, message, rx):
        d = orjson.loads(message)
        if d.get("method") == "subscribe" and not d.get("success", True):
            logger.warning(f"[kraken] {d}")
            return
        if d.get("channel") != "ticker":
            return
        for item in d.get("data") or []:
            q = self.src["kr"]
            with self.lock:
                q.set_top(_f(item.get("bid")), _f(item.get("ask")), rx, None)
                q.last = _f(item.get("last"))

    def _okx(self, message, rx):
        # Frames arrive as bytes: run_forever skips UTF-8 validation.
        if message in ("pong", b"pong"):
            return
        d = orjson.loads(message)
        if d.get("event") == "error":
            logger.warning(f"[okx] {d}")
            return
        for item in d.get("data") or []:
            bids, asks = item.get("bids") or [], item.get("asks") or []
            if not bids or not asks:
                continue
            ts = _f(item.get("ts"))
            with self.lock:
                self.src["okx"].set_top(float(bids[0][0]), float(asks[0][0]), rx,
                                        ts / 1000.0 if ts else None)

    def _bybit(self, message, rx):
        d = orjson.loads(message)
        if "topic" not in d:
            if d.get("success") is False:
                logger.warning(f"[bybit] {d}")
            return
        data = d.get("data") or {}
        q = self.src["byb"]
        bid = next((float(p) for p, s in data.get("b") or [] if float(s) > 0), None)
        ask = next((float(p) for p, s in data.get("a") or [] if float(s) > 0), None)
        ts = _f(d.get("ts"))
        with self.lock:
            q.set_top(bid if bid is not None else q.bid, ask if ask is not None else q.ask,
                      rx, ts / 1000.0 if ts else None)

    def _rtds(self, message, rx):
        if not message:
            return
        d = orjson.loads(message)
        payload = d.get("payload") or {}
        if payload.get("symbol") != "btc/usd" or payload.get("value") is None:
            return
        topic = d.get("topic")
        with self.lock:
            if topic == "crypto_prices_chainlink":
                ts = _f(payload.get("timestamp"))
                self.cl.update(price=float(payload["value"]), rx=rx,
                               ex_ts=ts / 1000.0 if ts else None)
            elif topic == "crypto_prices_twap_sixty":
                self.cl.update(twap60=float(payload["value"]), twap60_rx=rx)

    # ---- Polymarket CLOB (one connection per window) ---------------------

    def _pm_handler(self, w: PolyWindow):
        side_of = {w.market.token_yes_id: "up", w.market.token_no_id: "down"}

        def handle(message, rx):
            if is_pong(message):
                return
            parsed = orjson.loads(message)
            events = parsed if isinstance(parsed, list) else [parsed]
            with self.lock:
                w.rx = rx
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    ts = _f(ev.get("timestamp"))
                    if ts:
                        w.server_ts = ts / 1000.0
                    kind = ev.get("event_type")
                    if kind == "book":
                        side = side_of.get(ev.get("asset_id"))
                        if side:
                            w.levels[side] = {"bids": levels_from(ev.get("bids")),
                                              "asks": levels_from(ev.get("asks"))}
                            w.ok[side] = True
                    elif kind == "price_change":
                        apply_price_changes(ev.get("price_changes") or [], side_of.get, w.levels, w.ok)
                    elif kind == "last_trade_price":
                        side = side_of.get(ev.get("asset_id"))
                        price, size = _f(ev.get("price")), _f(ev.get("size"))
                        if side and price is not None:
                            w.last[side] = price
                            w.vol[side] += size or 0.0
        return handle

    def _pm_reset(self, w: PolyWindow):
        # Updates missed while disconnected make the books unknowable until
        # the snapshot the subscription sends.
        def reset():
            with self.lock:
                for side in SIDES:
                    w.ok[side] = False
        return reset

    # ---- snapshot (sampler thread) ---------------------------------------

    def _snapshot(self):
        with self.lock:
            now = time.time()
            ws = int(now // WINDOW_S * WINDOW_S)
            row = [now, ws]
            for p in ("bnp", "bns"):
                q = self.src[p]
                row += [q.bid, q.ask, q.bid_qty, q.ask_qty, q.bid_depth5, q.ask_depth5,
                        q.ofi, q.last, q.buy_vol, q.sell_vol, q.trades,
                        _ms(now - q.rx) if q.rx else None,
                        _ms(q.rx - q.ex_ts) if q.rx and q.ex_ts else None,
                        _ms(now - q.trade_rx) if q.trade_rx else None]
                q.reset_flow()
            for p in ("cb", "kr", "okx", "byb"):
                q = self.src[p]
                row += [q.bid, q.ask, q.last,
                        _ms(now - q.rx) if q.rx else None,
                        _ms(q.rx - q.ex_ts) if q.rx and q.ex_ts else None]
            cl = self.cl
            row += [cl["price"], _ms(now - cl["rx"]) if cl["rx"] else None,
                    _ms(cl["rx"] - cl["ex_ts"]) if cl["rx"] and cl["ex_ts"] else None,
                    cl["twap60"], _ms(now - cl["twap60_rx"]) if cl["twap60_rx"] else None]
            w = self.windows.get(ws)
            extra = []
            for side in SIDES:
                if not (w and w.feed):
                    row += [None] * 7
                    extra += [None] * len(EXTRA_VWAP_SIZES)
                    continue
                if w.ok[side]:
                    book = w.levels[side]
                    bid, ask = best_bid(book), best_ask(book)
                    vw = vwap_buys(book["asks"], (VWAP_SHARES,) + EXTRA_VWAP_SIZES)
                    row += [bid, ask, size_at(book["bids"], bid), size_at(book["asks"], ask), vw[0]]
                    extra += vw[1:]
                else:
                    row += [None] * 5
                    extra += [None] * len(EXTRA_VWAP_SIZES)
                row += [w.last[side], w.vol[side]]
                w.vol[side] = 0.0
            if w and w.rx:
                row += [_ms(now - w.rx), _ms(w.rx - w.server_ts) if w.server_ts else None,
                        int(w.ok["up"] and w.ok["down"])]
            else:
                row += [None, None, 0]
            row += extra
        return row

    def _lab_emit(self, params):
        """A lab trade goes to lab_shadow; the stale-book strategy's first trade of a window also opens a paper_ledger position."""
        self.q.put(("sql", (lab_shadow.INSERT, params)))
        row = paper_ledger.row_from_lab(params)
        if row:
            self.q.put(("sql", (paper_ledger.INSERT, row)))

    def _sampler_loop(self):
        next_t = math.floor(time.time() / STEP_S) * STEP_S + STEP_S
        while not self.stop_event.is_set():
            delay = next_t - time.time()
            if delay > 0 and self.stop_event.wait(delay):
                break
            row = self._snapshot()
            self.q.put(("tape", row))
            if self.lab:
                try:
                    self.lab.on_sample(row)
                except Exception as e:
                    self.lab_errors += 1
                    if self.lab_errors == 1 or self.lab_errors % 1000 == 0:
                        logger.error(f"[LAB] strategy shadow error #{self.lab_errors}: {e}")
            if time.time() >= self._next_settle:
                self._next_settle = time.time() + 60.0
                self.q.put(("sql", (paper_ledger.SETTLE_OUTCOME, ())))
                self.q.put(("sql", (paper_ledger.SETTLE_PNL, ())))
            if self.engine:
                try:
                    self.engine.on_sample(row)
                except Exception as e:
                    self.engine_errors += 1
                    if self.engine_errors == 1 or self.engine_errors % 1000 == 0:
                        logger.error(f"[ENGINE] engine shadow error #{self.engine_errors}: {e}")
            next_t += STEP_S
            late = time.time() - next_t
            if late > 1.0:
                # Machine sleep or a long stall: skip ahead, never burst rows.
                next_t = math.floor(time.time() / STEP_S) * STEP_S + STEP_S

    # ---- database (writer thread) ----------------------------------------

    def _init_db(self):
        """Schema setup on the main thread, so a bad path or a locked file
        fails at startup rather than inside the writer."""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        for stmt in _SCHEMA:
            conn.execute(stmt)
        have = {r[1] for r in conn.execute("PRAGMA table_info(tape)")}
        for name, sqltype in TAPE_COLUMNS:
            if name not in have:
                conn.execute(f"ALTER TABLE tape ADD COLUMN {name} {sqltype}")
        # lab_shadow before 2026-10-10 evening had no trade_no: its rows are all first trades.
        if "trade_no" not in {r[1] for r in conn.execute("PRAGMA table_info(lab_shadow)")}:
            conn.execute("ALTER TABLE lab_shadow ADD COLUMN trade_no INTEGER DEFAULT 1")
        conn.execute(paper_ledger.BACKFILL)        # idempotent: the ledger also holds the lab's trades from before this start
        conn.commit()
        # A tape from before 2026-10-05 has implicit rowids: give it the
        # stable id key once, before any thread writes (sqlite_keys.py).
        # A failure (e.g. a lock) only postpones it to the next start.
        try:
            ensure_autoincrement_key(conn, "tape")
        except sqlite3.Error as e:
            logger.warning(f"[REC] tape id key not added yet ({e}); retried at next start")
        conn.close()

    def _writer_loop(self):
        # sqlite3 connections belong to the thread that opened them.
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA synchronous=NORMAL")
        pending: list = []          # a batch whose write failed, retried with the next one
        failing_since = None
        final_tries = 3             # after stop, a still-failing batch gets a few more tries
        while True:
            items = pending
            try:
                items = items + [self.q.get(timeout=1.0)]
            except queue.Empty:
                if self.stop_event.is_set() and (not pending or final_tries <= 0):
                    break
            while True:
                try:
                    items.append(self.q.get_nowait())
                except queue.Empty:
                    break
            if not items:
                continue
            tape = [row for kind, row in items if kind == "tape"]
            other = [row for kind, row in items if kind == "sql"]
            try:
                with conn:
                    if tape:
                        conn.executemany(_TAPE_INSERT, tape)
                    for sql, params in other:
                        conn.execute(sql, params)
                self.rows_written += len(tape)
                self.last_write = time.time()
                pending, failing_since = [], None
            except sqlite3.Error as e:
                # Keep the batch (window rows, resolution updates and tape
                # rows) for the next attempt instead of dropping it: a lock
                # held by a reader or a cleanup pass is usually gone a second
                # later. Only a failure lasting WRITE_RETRY_S drops tape rows.
                self.write_errors += 1
                failing_since = failing_since or time.time()
                pending = items
                if time.time() - failing_since > WRITE_RETRY_S and tape:
                    pending = [it for it in items if it[0] == "sql"]
                    logger.error(f"[DB] writes failing for {time.time() - failing_since:.0f}s "
                                 f"({e}) — dropped {len(tape)} tape rows, keeping "
                                 f"{len(pending)} window updates for retry")
                elif self.write_errors == 1 or self.write_errors % 60 == 0:
                    logger.error(f"[DB] write failed, will retry ({len(items)} items held): {e}")
                if self.stop_event.is_set():
                    final_tries -= 1
            # Batch about once a second; the queue absorbs the wait.
            if not self.stop_event.is_set():
                self.stop_event.wait(1.0)
        if pending:
            logger.error(f"[DB] stopped with {len(pending)} items never written")
        conn.close()

    # ---- windows (manager thread) ----------------------------------------

    def _gamma_market(self, slug: str):
        r = requests.get(f"{GAMMA_URL}/events", params={"slug": slug}, timeout=8)
        r.raise_for_status()
        events = r.json()
        events = events if isinstance(events, list) else events.get("events", [])
        for ev in events:
            markets = ev.get("markets") or []
            if markets:
                return markets[0]
        return None

    def _discover(self, w: PolyWindow, now: float):
        # Live/next windows are retried quickly; past ones only for the record.
        w.next_discovery = now + (5 if now < w.end else 60)
        try:
            raw = self._gamma_market(w.slug)
        except Exception as e:
            logger.debug(f"[PM] {w.slug}: Gamma lookup failed: {e}")
            return
        if raw is None:
            return
        market = self.market_parser._parse_market(raw)
        if market is None:
            logger.warning(f"[PM] {w.slug}: market found but not parseable as Up/Down")
            w.next_discovery = now + 60
            return
        w.market, w.market_json = market, json.dumps(raw)
        self.q.put(("sql", (
            "INSERT OR IGNORE INTO windows (window_start, window_end, slug, condition_id, "
            "token_up, token_down, discovered_at, market_json) VALUES (?,?,?,?,?,?,?,?)",
            (w.start, w.end, w.slug, market.condition_id, market.token_yes_id,
             market.token_no_id, now, w.market_json))))
        w.row_written = True
        logger.info(f"[PM] {w.slug}: found ({market.condition_id[:12]}…)")

    def _start_pm_feed(self, w: PolyWindow):
        w.feed = Feed(f"pm-{w.start}", ["wss://ws-subscriptions-clob.polymarket.com/ws/market"],
                      self._pm_handler(w),
                      subscribe={"type": "market",
                                 "assets_ids": [w.market.token_yes_id, w.market.token_no_id],
                                 "custom_feature_enabled": True},
                      # Polymarket's text heartbeat, see ws_heartbeat.py.
                      text_ping=("PING", 10),
                      stall_s=PM_FEED_STALL_S, on_open=self._pm_reset(w)).start()

    def _resolve(self, w: PolyWindow, now: float):
        w.next_resolution = now + 30
        official = self.resolutions.fetch_official_resolution(w.market.condition_id)
        if official not in ("UP", "DOWN"):
            return
        w.official = official
        closed_json = None
        try:
            raw = self._gamma_market(w.slug)
            closed_json = json.dumps(raw) if raw else None
        except Exception:
            pass
        self.q.put(("sql", (
            "UPDATE windows SET official_resolution=?, resolved_at=?, market_json_closed=? "
            "WHERE window_start=?", (official, now, closed_json, w.start))))

    def _lookup(self, fn, w: PolyWindow, now: float):
        def run():
            try:
                fn(w, now)
            except Exception as e:
                logger.warning(f"[PM] {w.slug}: {e}")
            finally:
                w.busy = False
        w.busy = True
        threading.Thread(target=run, name=f"Lookup-{w.start}", daemon=True).start()

    def _manager_loop(self):
        last_status = time.time()
        while not self.stop_event.wait(1.0):
            now = time.time()
            if self.parent_pid and os.getppid() != self.parent_pid:
                logger.warning("[REC] parent process is gone — stopping")
                self.stop_event.set()
                break
            current = int(now // WINDOW_S * WINDOW_S)
            wanted = [current] + ([current + WINDOW_S] if now >= current + WINDOW_S - PREWARM_S else [])
            for start in wanted:
                if start not in self.windows:
                    with self.lock:
                        self.windows[start] = PolyWindow(start)
            for start, w in list(self.windows.items()):
                try:
                    if w.market and w.feed is None and now < w.end:
                        self._start_pm_feed(w)
                    if w.feed and now > w.end + 10:
                        w.feed.stop()
                    # HTTP lookups run off this thread, at most one per
                    # window, so a slow or unreachable Polymarket never
                    # delays the next window's connection or the stall checks.
                    if not w.busy and now < w.end + RESOLUTION_GIVE_UP_S:
                        if w.market is None and now >= w.next_discovery:
                            self._lookup(self._discover, w, now)
                        elif w.market and w.official is None and now >= w.next_resolution:
                            self._lookup(self._resolve, w, now)
                    if now > w.end + RESOLUTION_GIVE_UP_S and not w.busy:
                        if w.feed:
                            w.feed.stop()
                        if not w.row_written:
                            # Never found (Polymarket unreachable): keep the
                            # window's slug so it can be looked up later.
                            self.q.put(("sql", (
                                "INSERT OR IGNORE INTO windows (window_start, window_end, slug) "
                                "VALUES (?,?,?)", (w.start, w.end, w.slug))))
                        with self.lock:
                            self.windows.pop(start, None)
                except Exception as e:
                    logger.warning(f"[PM] {w.slug}: {e}")
            for f in self.feeds + [w.feed for w in list(self.windows.values()) if w.feed]:
                f.check_stall(now)
            if now - last_status >= STATUS_INTERVAL_S:
                self._log_status(now - last_status)
                last_status = now

    def _log_status(self, span: float):
        rates = ", ".join(f"{f.name} {f.messages / span:.1f}/s" for f in self.feeds)
        for f in self.feeds:
            f.messages = 0
        try:
            size_mb = os.path.getsize(self.db_path) / 1e6
        except OSError:
            size_mb = float("nan")
        cur = self.windows.get(int(time.time() // WINDOW_S * WINDOW_S))
        pm = ("no market" if not cur or not cur.market else
              f"books ok={cur.ok['up'] and cur.ok['down']}")
        lab = f", lab trades {self.lab.trades}" + (f" (errors {self.lab_errors})" if self.lab_errors else "") if self.lab else ""
        lab += (f", engine signals {self.engine.signals}" + (f" (errors {self.engine_errors})" if self.engine_errors else "")) if self.engine else ""
        logger.info(f"[REC] rows {self.rows_written}, db {size_mb:.0f} MB, write errors "
                    f"{self.write_errors}{lab}, Polymarket {pm} | {rates}")

    # ---- lifecycle -------------------------------------------------------

    def run(self):
        self._init_db()
        if self.engine:
            try:
                logger.info(f"[ENGINE] volatility primed from {self.engine.prime_from_db(self.db_path)} one-second prices of the tape")
            except Exception as e:
                logger.warning(f"[ENGINE] could not prime the volatility history: {e}")
        writer = threading.Thread(target=self._writer_loop, name="Writer", daemon=True)
        writer.start()
        for f in self.feeds:
            f.start()
        threads = [threading.Thread(target=self._sampler_loop, name="Sampler", daemon=True),
                   threading.Thread(target=self._manager_loop, name="Manager", daemon=True)]
        for t in threads:
            t.start()
        logger.info(f"[REC] recording to {self.db_path} every {STEP_S * 1000:.0f} ms "
                    f"({len(TAPE_COLUMNS)} columns)")
        if self.parquet:
            self.parquet.start()
        while not self.stop_event.wait(1.0):
            pass
        logger.info("[REC] stopping…")
        closers = [threading.Thread(target=f.stop, daemon=True)
                   for f in self.feeds + [w.feed for w in list(self.windows.values()) if w.feed]]
        for t in closers:
            t.start()
        for t in closers + threads:
            t.join(timeout=3)
        writer.join(timeout=10)
        if self.parquet:
            # After the writer, so the last rows are in SQLite to be copied.
            self.parquet.stop(final_sync=True)
        logger.info(f"[REC] stopped — {self.rows_written} rows written")


# ============================================================================
# ENTRY POINTS
# ============================================================================

def _acquire_lock(path: str):
    handle = open(path, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def _status(db_path: str):
    if not os.path.exists(db_path):
        print(f"{db_path} yok — henüz kayıt yapılmamış.")
        return
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    n, t0, t1 = conn.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM tape").fetchone()
    print(f"Veritabanı: {db_path} ({os.path.getsize(db_path) / 1e6:.0f} MB)")
    if not n:
        print("tape boş.")
        return
    fmt = lambda t: datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
    print(f"tape: {n:,} satır, {fmt(t0)} → {fmt(t1)} "
          f"(son satır {time.time() - t1:.0f} sn önce; kapsama %{100 * n * STEP_S / max(t1 - t0, 1):.0f})")
    since = t1 - 600
    checks = [("Binance perp", "bnp_bid"), ("Binance spot", "bns_bid"), ("Coinbase", "cb_bid"),
              ("Kraken", "kr_bid"), ("OKX", "okx_bid"), ("Bybit", "byb_bid"),
              ("Chainlink", "cl_price"), ("Chainlink TWAP60", "cl_twap60"),
              ("Polymarket Up", "pm_up_ask"), ("Polymarket Down", "pm_down_ask")]
    sel = ", ".join(f"SUM({c} IS NOT NULL)" for _, c in checks)
    m, *filled = conn.execute(f"SELECT COUNT(*), {sel} FROM tape WHERE ts >= ?", (since,)).fetchone()
    print("Son 10 dakikada dolu olma oranı:")
    for (label, _), k in zip(checks, filled):
        print(f"  {label:18s} %{100 * (k or 0) / max(m, 1):5.1f}")
    w, found, res = conn.execute(
        "SELECT COUNT(*), SUM(condition_id IS NOT NULL), SUM(official_resolution IS NOT NULL) FROM windows").fetchone()
    print(f"windows: {w} pencere, {found or 0} market bulundu, {res or 0} resmi sonuç")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="100 ms market tape recorder")
    parser.add_argument("--db", default="market_tape.db", help="tape database (default market_tape.db)")
    parser.add_argument("--parent-pid", type=int, default=None,
                        help="stop when this process is gone (set by run.py)")
    parser.add_argument("--status", action="store_true", help="summarize what has been recorded and exit")
    parser.add_argument("--parquet-dir", default="",
                        help="also keep a Parquet copy here (parquet_sync.py); empty = off")
    parser.add_argument("--retention-days", type=float, default=0,
                        help="delete tape/book_ticks rows older than this from SQLite once in Parquet; 0 = keep all")
    parser.add_argument("--bot-db", default="",
                        help="the bot's database, copied to Parquet as well (set by run.py)")
    args = parser.parse_args()
    db_path = args.db if os.path.isabs(args.db) else os.path.join(HERE, args.db)

    if args.status:
        _status(db_path)
        return

    handler = RotatingFileHandler(os.path.join(HERE, "recorder.log"), maxBytes=10_000_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    if sys.stderr.isatty():
        root.addHandler(logging.StreamHandler())
    logging.getLogger("websocket").setLevel(logging.CRITICAL)

    lock = _acquire_lock(os.path.join(HERE, ".market_recorder.lock"))
    if lock is None:
        logger.warning("[REC] another recorder is already running — exiting")
        sys.exit(EXIT_ALREADY_RUNNING)

    absolute = lambda p: p if not p or os.path.isabs(p) else os.path.join(HERE, p)
    rec = Recorder(db_path, parent_pid=args.parent_pid,
                   parquet_dir=absolute(args.parquet_dir), bot_db=absolute(args.bot_db),
                   retention_days=args.retention_days or None)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: rec.stop_event.set())
    rec.run()


if __name__ == "__main__":
    main()

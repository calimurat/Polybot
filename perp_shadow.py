"""
PERP MOMENTUM SHADOW — the Binance perpetual momentum signal, run live and
recorded beside the bot's own paper trades, never instead of them (added
2026-10-04, user-approved "seçenek A": log-only, the frozen calibration is
untouched).

The signal: when the BTCUSDT perpetual's last trade price has moved more than
`threshold_usd` over the past `window_s` seconds (Binance's own trade time),
buy that direction's token — the first such moment in each window, between
`min_remaining_s` and `max_remaining_s` before its close, one shadow trade per
window. On market_tape.db (359 windows, 2026-10-02/04) the same rule on the
trade price, checked every 100 ms, made +$741 with instant fills, +$595 at
+200 ms and +$355 at +1 s — the fastest single signal tested, and the reason
this runs on the raw aggTrade stream and decides on every message:
  - aggTrade arrives per trade (~20-60/s), not batched like depth5@100ms;
  - the 10 s look-back is measured on Binance's trade time, so network jitter
    cannot stretch or shrink it;
  - the decision runs on the feed's own thread, not the bot's evaluator queue,
    and prices the fill from the bot's in-memory Polymarket book at once.

Several (window_s, threshold_usd) variants run on the same feed
(Config.perp_momentum_variants), each with its own one-per-window count, so
they can be compared live; rows carry window_s/threshold_usd.

Each row keeps the latency of every step (exchange → us, us → decision) so the
gap to a VPS can be measured rather than guessed. Outcomes are not stored:
paper_report.py joins perp_shadow to market_sessions (session_id = condition_id)
for the official resolution, as it does for shadow_trades.
"""

import logging
import sqlite3
import threading
import time
from collections import deque

import orjson

from ws_feed import Feed

logger = logging.getLogger("PerpShadow")

PERP_AGGTRADE_URL = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade"
# Price at or above this buys too little to be worth a row; the replay skipped
# it too and kept waiting for a cheaper moment in the same window.
MAX_FILL_PRICE = 0.97
# A pause longer than this between two perp trades (Binance trade time) means
# the feed, not the market, went quiet — BTCUSDT perp trades many times a
# second. History before such a gap (or a reconnect) is dropped, so a signal
# is never measured against a price from before the gap (2026-10-05).
TRADE_GAP_RESET_S = 5.0
BOOK_LEVELS = 10
_KEEP_WINDOWS = 8


def _book_levels(book):
    """Top BOOK_LEVELS of each side, best first, as [[price, size], ...].
    Orderbook keeps both sides worst-first, so sort rather than trust order."""
    def side(levels, reverse):
        out = []
        for lv in levels or []:
            try:
                out.append([float(lv["price"]), float(lv["size"])])
            except (KeyError, TypeError, ValueError):
                continue
        return sorted(out, reverse=reverse)[:BOOK_LEVELS]
    return {"asks": side(book.asks, False), "bids": side(book.bids, True)}

_COLUMNS = (
    ("ts", "REAL"), ("condition_id", "TEXT"), ("window_end", "INTEGER"),
    ("seconds_remaining", "REAL"), ("direction", "TEXT"),
    ("delta_usd", "REAL"), ("price_now", "REAL"), ("price_ref", "REAL"),
    ("ref_age_s", "REAL"), ("trade_ts", "REAL"), ("recv_ts", "REAL"),
    ("net_lag_ms", "REAL"), ("decide_ms", "REAL"),
    ("fill_price", "REAL"), ("best_ask", "REAL"), ("size", "REAL"), ("fee", "REAL"),
    ("feed_behind_ms", "REAL"), ("window_s", "REAL"), ("threshold_usd", "REAL"),
    # added 2026-10-04: the bought token's book at the signal, best level
    # first, BOOK_LEVELS per side — {"asks": [[price, size], ...], "bids": [...]}
    ("book_json", "TEXT"),
)
_SCHEMA = ("CREATE TABLE IF NOT EXISTS perp_shadow ("
           + ", ".join(f"{n} {t}" for n, t in _COLUMNS) + ")")
_INSERT = ("INSERT INTO perp_shadow (" + ", ".join(n for n, _ in _COLUMNS) + ") VALUES ("
           + ",".join("?" * len(_COLUMNS)) + ")")


class _Variant:
    """One (window_s, threshold_usd) rule with its own trade history and its
    own one-row-per-window bookkeeping, so variants never take each other's
    windows."""

    def __init__(self, window_s: float, threshold_usd: float):
        self.window_s = window_s
        self.threshold = threshold_usd
        # (Binance trade time s, price), oldest first; trimmed so the first
        # element is the last trade at or before now - window_s.
        self.trades = deque()
        self.taken = {}               # condition_id -> True, last few windows
        self.signals = self.recorded = 0

    @property
    def label(self) -> str:
        return f"{self.window_s:g}s ±${self.threshold:g}"


class PerpMomentumShadow:
    def __init__(self, controller, variants, min_remaining_s: float, max_remaining_s: float,
                 size: float, fee_rate: float):
        """variants: iterable of (window_s, threshold_usd), each measured on its own."""
        self.ctrl = controller
        self.variants = [_Variant(float(w), float(t)) for w, t in variants]
        self.min_rem = min_remaining_s
        self.max_rem = max_remaining_s
        self.size = size
        self.fee_rate = fee_rate
        self._stop = threading.Event()
        self.feed = Feed("binance-perp-aggtrade", [PERP_AGGTRADE_URL], self._on_trade,
                         on_open=self._reset_history)
        conn = sqlite3.connect(controller.config.db_file, timeout=10)
        try:
            conn.execute(_SCHEMA)
            have = {r[1] for r in conn.execute("PRAGMA table_info(perp_shadow)")}
            for name, sqltype in _COLUMNS:
                if name not in have:
                    conn.execute(f"ALTER TABLE perp_shadow ADD COLUMN {name} {sqltype}")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_perp_shadow_cond ON perp_shadow(condition_id)")
            conn.commit()
        finally:
            conn.close()

    @property
    def signals(self) -> int:
        return sum(v.signals for v in self.variants)

    @property
    def recorded(self) -> int:
        return sum(v.recorded for v in self.variants)

    def start(self):
        self.feed.start()
        threading.Thread(target=self._watchdog, name="PerpShadowWatchdog", daemon=True).start()
        logger.info(f"[PERP-SHADOW] Recording perp momentum ({', '.join(v.label for v in self.variants)}; "
                    f"{self.min_rem:g}-{self.max_rem:g}s before close, 1 per window each) — log-only")
        return self

    def stop(self):
        self._stop.set()
        self.feed.stop()

    def _watchdog(self):
        while not self._stop.wait(1.0):
            self.feed.check_stall(time.time())

    def _reset_history(self):
        """Forget every variant's trades: called on each (re)connect, whose
        missed trades would otherwise make the oldest kept trade a stale
        reference. Runs on the feed's thread, like _on_trade."""
        for v in self.variants:
            v.trades.clear()

    # ---- hot path: one call per perp trade, on the feed's thread ---------

    def _on_trade(self, message, recv_ts: float):
        d = orjson.loads(message)
        if "p" not in d:
            return
        trade_ts = d["T"] / 1000.0
        price = float(d["p"])
        last = self.variants[0].trades[-1][0] if self.variants and self.variants[0].trades else None
        if last is not None and trade_ts - last > TRADE_GAP_RESET_S:
            self._reset_history()
        for v in self.variants:
            trades = v.trades
            trades.append((trade_ts, price))
            cutoff = trade_ts - v.window_s
            while len(trades) > 1 and trades[1][0] <= cutoff:
                trades.popleft()
            ref_ts, ref_price = trades[0]
            if ref_ts > cutoff:
                continue              # less than window_s of history yet
            delta = price - ref_price
            if delta > v.threshold:
                direction = "UP"
            elif delta < -v.threshold:
                direction = "DOWN"
            else:
                continue
            v.signals += 1
            self._consider(v, direction, delta, price, ref_price, trade_ts - ref_ts, trade_ts, recv_ts)

    def _consider(self, v, direction, delta, price, ref_price, ref_age, trade_ts, recv_ts):
        ctrl = self.ctrl
        market, pm = ctrl.market_data, ctrl.polymarket
        if not ctrl._loop_active or market is None or pm is None or not ctrl._loop_end:
            return
        cid = market.condition_id
        if cid in v.taken:
            return
        now = time.time()
        remaining = ctrl._loop_end - now
        if not (self.min_rem <= remaining <= self.max_rem):
            return
        book = pm.orderbook_cache_yes if direction == "UP" else pm.orderbook_cache_no
        if not (book and book.is_valid() and book.best_ask is not None):
            return                    # no usable book yet: wait for the next signal
        fill = book.vwap_fill_price(self.size, "buy")
        if fill is None or fill > MAX_FILL_PRICE:
            return
        decided = time.time()
        v.taken[cid] = True
        for old in list(v.taken)[:-_KEEP_WINDOWS]:
            v.taken.pop(old, None)
        behind = pm.feed_behind_s(decided)
        row = (decided, cid, int(ctrl._loop_end), remaining, direction,
               delta, price, ref_price, ref_age, trade_ts, recv_ts,
               (recv_ts - trade_ts) * 1000, (decided - recv_ts) * 1000,
               fill, book.best_ask, self.size,
               self.size * self.fee_rate * fill * (1 - fill),
               None if behind is None else behind * 1000, v.window_s, v.threshold,
               orjson.dumps(_book_levels(book)).decode())
        v.recorded += 1
        threading.Thread(target=self._write, args=(row,), daemon=True, name="PerpShadowWrite").start()
        logger.info(f"[PERP-SHADOW] {v.label} {direction} perp {delta:+.1f}$ in {ref_age:.1f}s → fill {fill:.4f} "
                    f"({remaining:.0f}s left; exchange→us {(recv_ts - trade_ts) * 1000:.0f} ms, "
                    f"decision {(decided - recv_ts) * 1000:.2f} ms)")

    def _write(self, row):
        # Off the feed thread: a SQLite write can wait on another connection's lock.
        try:
            conn = sqlite3.connect(self.ctrl.config.db_file, timeout=10)
            conn.execute(_INSERT, row)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"[PERP-SHADOW] Could not record row: {e}")

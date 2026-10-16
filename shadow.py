"""
SHADOW MEASUREMENT — what the same strategy would do at smaller edges.

Added 2026-09-29. With both feed-lag guards in place the real edge gate
(MultiFactorConfig.min_edge_up / min_edge: 0.15 / 0.20 when this was written;
UP -0.05 since 2026-09-30, DOWN still 0.20) was met in ~2% of
windows: about one paper trade every seven hours, months of running
before the strategy's edge can be judged at all.

This records, beside the bot's own paper trades and without touching
them, the trade the SAME strategy would have made in each edge band of
Config.shadow_edge_bands (1-cent bands, 0.01-0.02 … 0.49-0.50, then 0.50-1.00 — each band
measured on its own, for UP and DOWN separately, user-specified
2026-09-29): per window, direction and band, the first moment every other
rule passes — the engine's gates (model
probability, ask range, book, depth), both feed-lag guards, the
market's parameters — with the edge inside that band (lo <= edge < hi).
A band therefore answers "is entering at THIS much edge profitable",
which cumulative thresholds (edge >= x) blur. The bands run past the real
gates (MultiFactorConfig.min_edge_up / min_edge), so the range the bot
itself trades is measured too.

It is filled exactly like a real paper order: as a taker, at once, at the
volume-weighted price over the depth the full order walks in the
signal-time book — slippage included, no latency and no price tolerance
(user-specified 2026-09-29). Each attempt therefore fills; slippage is
recorded per row.

Outcomes are not stored here: paper_report.py joins shadow_trades to
market_sessions (session_id = condition_id) for the official resolution.
Rows written under the earlier fill model (a 500 ms wait against the live
book with a limit one tick above the worst level, so some attempts did not
fill) have fill_model NULL and are reported separately.
"""

import logging
import sqlite3
import threading
from typing import Iterable, Tuple

from signal_engine import Signal

logger = logging.getLogger("Shadow")

FILL_MODEL = "instant"

_COLUMNS = (
    ("ts", "REAL"), ("condition_id", "TEXT"), ("window_end", "INTEGER"),
    ("edge_lo", "REAL"), ("edge_hi", "REAL"), ("direction", "TEXT"),
    ("model_prob", "REAL"), ("edge", "REAL"), ("signal_ask", "REAL"),
    ("signal_price", "REAL"), ("fill_price", "REAL"), ("filled", "INTEGER"),
    ("fail_reason", "TEXT"), ("size", "REAL"), ("fee", "REAL"),
    ("feed_behind_ms", "REAL"), ("btc_feed_lag_ms", "REAL"),
    ("fill_feed_behind_ms", "REAL"),
    # added with the taker fill model (2026-09-29)
    ("fill_model", "TEXT"), ("slippage", "REAL"),
    # log-only short-volatility model (2026-10-02, signal_engine.ShortVolatility)
    ("model_prob_short_vol", "REAL"), ("edge_short_vol", "REAL"),
)
_SCHEMA = ("CREATE TABLE IF NOT EXISTS shadow_trades ("
           + ", ".join(f"{name} {sqltype}" for name, sqltype in _COLUMNS) + ")")
# Named columns, so a table created before later columns were added still works.
_INSERT = ("INSERT INTO shadow_trades (" + ", ".join(name for name, _ in _COLUMNS)
           + ") VALUES (" + ",".join("?" * len(_COLUMNS)) + ")")

# Windows whose per-band state is kept; older ones can't recur.
_KEEP_WINDOWS = 4


class ShadowRecorder:
    def __init__(self, controller, bands: Iterable[Tuple[float, float]], fee_rate: float):
        self.ctrl = controller
        self.bands = sorted(set((float(lo), float(hi)) for lo, hi in bands))
        self.fee_rate = fee_rate
        # condition_id -> direction -> bands already recorded in that
        # window: UP and DOWN are separate instances (a window can flip
        # direction), each taking a band at most once. Touched only by
        # consider(), which runs on the evaluator thread.
        self._taken = {}
        self.enabled = bool(self.bands)
        if self.enabled:
            try:
                conn = sqlite3.connect(controller.config.db_file, timeout=10)
                conn.execute(_SCHEMA)
                have = {row[1] for row in conn.execute("PRAGMA table_info(shadow_trades)")}
                for name, sqltype in _COLUMNS:
                    if name not in have:
                        conn.execute(f"ALTER TABLE shadow_trades ADD COLUMN {name} {sqltype}")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_shadow_cond ON shadow_trades(condition_id)")
                conn.commit()
                conn.close()
                logger.info(f"[SHADOW] Recording shadow trades in {len(self.bands)} edge bands "
                            f"{self.bands[0][0]:.2f}-{self.bands[-1][1]:.2f}")
            except Exception as e:
                logger.warning(f"[SHADOW] Disabled: {e}")
                self.enabled = False

    def consider(self, evaluation, current_btc, now: float):
        """
        Called with every evaluation, on the evaluator thread. Returns at
        once unless the edge sits in a band not yet taken this window in
        this direction; then
        checks what the real execution path checks, fills the shadow order
        against the same book, and hands the row to a writer thread.
        """
        if not self.enabled:
            return
        ev = evaluation
        if ev.edge is None or ev.candidate_direction not in ("UP", "DOWN"):
            return
        # Every gate but the edge one passed: a real signal, or a rejection
        # at the edge gate itself (evaluate() checks it last).
        if ev.signal == Signal.NO_SIGNAL and not (ev.rejected_reason or "").startswith("edge "):
            return
        ctrl = self.ctrl
        market, pm = ctrl.market_data, ctrl.polymarket
        if market is None or pm is None or ctrl._market_blocker:
            return
        cid = market.condition_id
        taken = self._taken.get(cid, {}).get(ev.candidate_direction, set())
        due = [b for b in self.bands if b[0] <= ev.edge < b[1] and b not in taken]
        if not due:
            return

        # The checks run.py's _on_market_tick makes before it fills an order.
        signal = Signal.BUY_UP if ev.candidate_direction == "UP" else Signal.BUY_DOWN
        book = pm.orderbook_cache_yes if signal == Signal.BUY_UP else pm.orderbook_cache_no
        if not (book and book.is_valid() and book.best_bid is not None and book.best_ask is not None):
            return
        cfg = ctrl.config
        size = cfg.trade_size
        fill_price = book.vwap_fill_price(size, "buy")
        if fill_price is None:
            return
        best_ask = book.best_ask
        behind = pm.feed_behind_s(now)
        behind_ms = None if behind is None else behind * 1000
        if behind_ms is None or behind_ms > cfg.max_feed_lag_ms:
            return
        btc_lag_ms = (None if current_btc is None or current_btc.feed_lag_s is None
                      else current_btc.feed_lag_s * 1000)
        if btc_lag_ms is None or btc_lag_ms > cfg.max_btc_feed_lag_ms:
            return

        self._taken.setdefault(cid, {}).setdefault(ev.candidate_direction, set()).update(due)
        for old in list(self._taken)[:-_KEEP_WINDOWS]:
            self._taken.pop(old, None)

        # Same fee PositionManager books for a real paper position.
        fee = size * self.fee_rate * fill_price * (1 - fill_price)
        window_end = int(ctrl._loop_end or 0)
        rows = [(now, cid, window_end, lo, hi, ev.candidate_direction, ev.model_prob, ev.edge,
                 ev.candidate_ask, fill_price, fill_price, 1, None, size, fee,
                 behind_ms, btc_lag_ms, None, FILL_MODEL, fill_price - best_ask,
                 ev.model_prob_short_vol, ev.edge_short_vol)
                for lo, hi in due]
        threading.Thread(target=self._write, args=(rows,), daemon=True, name="ShadowWrite").start()
        logger.info(f"[SHADOW] {ev.candidate_direction} edge {ev.edge:+.3f} filled at {fill_price:.4f} "
                    f"(slippage {fill_price - best_ask:+.4f}/share) — band {due[0][0]:.2f}-{due[0][1]:.2f}"
                    + (f" (+{len(due) - 1} more)" if len(due) > 1 else ""))

    def _write(self, rows):
        # Off the evaluator thread: a SQLite write can wait on another
        # connection's lock, and the evaluator must never wait.
        try:
            conn = sqlite3.connect(self.ctrl.config.db_file, timeout=10)
            conn.executemany(_INSERT, rows)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.warning(f"[SHADOW] Could not record {len(rows)} row(s): {e}")

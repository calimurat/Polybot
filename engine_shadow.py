"""
ENGINE SHADOW — the bot's own signal engine (signal_engine.MultiFactorSignalEngine) run on the recorder's 100 ms tape, LOG-ONLY
(added 2026-10-12, user: "K, R1, R2 ve R3 ... botun paper işlemlerine bağlı olmadan").

Until now K, R1, R2 and R3 could only be read off what the bot happened to trade (K = its paper trades, R1/R2 = a filter over
them, R3 = its edge-band shadow), so they existed only while the bot ran and traded. This module runs the very same engine on
every tape sample, so each of them is measured from the continuous record alone:

  K   the bot's gates as they are: the first BUY signal of each window
  R1  UP only with edge >= 0.02 (DOWN unchanged), first such signal of each window — a real replay, so a stricter gate that
      skips a window's first signal and takes a later one is seen, which the filter over the bot's trades could not show
  R2  R1 and model probability <= 0.80
  R3  DOWN with edge >= 0.10 (every other gate as for DOWN), first per window

Inputs, all in the tape: Binance spot last price (the bot's price feed), the 60 s TWAP ending at the window start (opening
price, see lab_shadow.tape_opening_price), the 30-share VWAP of each token (pm_up/down_vwap30), pm_ok (book valid by the bot's own
rules). The one-minute volatility builds itself from the same price over the last hour: after a restart the engine has no
estimate until it has 30 one-minute returns, so prime() feeds it the tape already in the database.

NOT reproduced: the bot's two feed-lag guards (Polymarket book behind <= 500 ms, Binance lag <= 500 ms) and its market-parameter
check. Replayed over 10-06..10-12 against the bot's real paper trades this finds 98% of K's windows on the same side, the signal
within 0.1 s and at the same price; the bot's guards cost it ~4% of windows, so the shadow is slightly more permissive.

Fills: like lab_shadow, 30 shares, the signal-time VWAP is recorded as signal_price (the bot's instant paper fill) and the
fill 170 ms later as fill_price (NULL when no valid book at that sample). Never trades, never touches the bot. One row per
variant and window, in `engine_shadow` (market_tape.db).
"""

import logging
import math
from typing import Callable, List, Optional

import sqlite3
from signal_engine import MultiFactorConfig, MultiFactorSignalEngine, Signal

logger = logging.getLogger("EngineShadow")
logging.getLogger("MultiFactorSignal").setLevel(logging.WARNING)      # evaluate() logs every fire at INFO

REAL_DELAY_S = 0.170
SIZE = 30.0
FEE_RATE = 0.07
MAX_PRICE = 0.97
WINDOW_S = 300
FILL_SLACK_S = 1.0
VARIANTS = ("K", "R1", "R2", "R3")

SCHEMA = ("CREATE TABLE IF NOT EXISTS engine_shadow (id INTEGER PRIMARY KEY AUTOINCREMENT, variant TEXT, window_start INTEGER, "
          "signal_ts REAL, fill_ts REAL, side TEXT, signal_price REAL, fill_price REAL, size REAL, fee REAL, model_prob REAL, "
          "edge REAL, distance_z REAL)")
INSERT = ("INSERT INTO engine_shadow (variant, window_start, signal_ts, fill_ts, side, signal_price, fill_price, size, fee, "
          "model_prob, edge, distance_z) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")


class _TapeBook:
    """The slice of polymarket_production_bot.Orderbook the engine uses, filled from one tape row."""

    def __init__(self, bid, ask, vwap30, ok):
        self.best_bid, self.best_ask, self._vwap30, self._ok = bid, ask, vwap30, ok

    def is_valid(self) -> bool:
        return self._ok

    def vwap_fill_price(self, size, side):
        return self._vwap30 if (size == SIZE and str(side).lower() == "buy") else None


def _r1(ev) -> bool:
    return ev.signal in (Signal.BUY_UP, Signal.BUY_DOWN) and (ev.candidate_direction == "DOWN" or ev.edge >= 0.02)


def _r2(ev) -> bool:
    return _r1(ev) and ev.model_prob <= 0.80


def _r3(ev) -> bool:
    if ev.candidate_direction != "DOWN" or ev.edge is None:
        return False
    # every gate but the edge one passed: a real signal, or the rejection at the edge gate (evaluate() checks it last)
    passed = ev.signal == Signal.BUY_DOWN or (ev.rejected_reason or "").startswith("edge ")
    return passed and ev.edge >= 0.10


_RULES = {"K": lambda ev: ev.signal in (Signal.BUY_UP, Signal.BUY_DOWN), "R1": _r1, "R2": _r2, "R3": _r3}


class EngineShadow:
    def __init__(self, columns, emit: Callable[[tuple], None], opening_price: Callable[[int], Optional[float]]):
        self.ix = {name: i for i, name in enumerate(columns)}
        self.emit = emit
        self.opening_price = opening_price
        self.engine = MultiFactorSignalEngine(MultiFactorConfig(), trade_size=SIZE)
        self.ws = None
        self.op = None
        self.op_next_try = 0.0
        self.taken = set()
        self.pending: List[dict] = []
        self.signals = 0

    def prime_from_db(self, tape_db: str, seconds: float = 3900.0) -> int:
        """Prime the volatility history from the last hour of this tape (one price a second), oldest first."""
        import time
        conn = sqlite3.connect(f"file:{tape_db}?mode=ro", uri=True, timeout=5)
        try:
            rows = conn.execute("SELECT ts, bns_last FROM tape WHERE ts >= ? AND bns_last IS NOT NULL ORDER BY ts",
                                (time.time() - seconds,)).fetchall()
        finally:
            conn.close()
        last, picked = -1, []
        for ts, px in rows:
            if int(ts) != last:
                last = int(ts)
                picked.append((ts, px))
        self.prime(picked)
        return len(picked)

    def prime(self, samples) -> None:
        """Feed (ts, price) pairs, oldest first, to the engine's volatility history (the tape already in the database, so a
        restarted recorder does not wait 30 minutes for its first estimate)."""
        for ts, px in samples:
            if px:
                self.engine.vol.record_tick(ts, px)
                self.engine.short_vol.record_tick(ts, px)

    def _g(self, row, name):
        return row[self.ix[name]]

    def on_sample(self, row) -> None:
        ts, ws = row[0], row[1]
        if ws != self.ws:
            self.ws, self.op, self.op_next_try = ws, None, 0.0
            self.taken, self.pending = set(), []
        end = ws + WINDOW_S
        tau = end - ts
        up = _TapeBook(self._g(row, "pm_up_bid"), self._g(row, "pm_up_ask"), self._g(row, "pm_up_vwap30"), self._g(row, "pm_ok") == 1)
        down = _TapeBook(self._g(row, "pm_down_bid"), self._g(row, "pm_down_ask"), self._g(row, "pm_down_vwap30"), self._g(row, "pm_ok") == 1)

        # fills that are due (same rule as lab_shadow: the first sample at/after the due time decides, no retry)
        still = []
        for p in self.pending:
            if ts < p["due"]:
                still.append(p)
                continue
            book = up if p["side"] == "UP" else down
            price = book._vwap30 if book._ok else None
            ok = price is not None and price <= MAX_PRICE and ts - p["due"] <= FILL_SLACK_S and ts < end
            fee = SIZE * FEE_RATE * price * (1.0 - price) if ok else None
            self.emit((p["variant"], ws, p["ts"], ts if ok else None, p["side"], p["signal_price"], price if ok else None, SIZE, fee,
                       p["model_prob"], p["edge"], p["z"]))
        self.pending = still

        if self.op is None and ts >= self.op_next_try:
            self.op_next_try = ts + 5.0
            try:
                self.op = self.opening_price(ws)
            except Exception:
                self.op = None
        btc = self._g(row, "bns_last")
        if len(self.taken) == len(VARIANTS):
            # nothing left to decide in this window, but the engine's volatility history must stay continuous
            if btc:
                self.engine.vol.record_tick(ts, btc)
                self.engine.short_vol.record_tick(ts, btc)
            return
        ev = self.engine.evaluate(current_btc=btc, opening_btc=self.op, seconds_remaining=tau, current_btc_ts=ts, elapsed=ts - ws,
                                  market_status=None, yes_book=up, no_book=down, trade_flow_yes=None, trade_flow_no=None)
        if ev.candidate_direction not in ("UP", "DOWN") or ev.edge is None:
            return
        for name in VARIANTS:
            if name in self.taken or not _RULES[name](ev):
                continue
            self.taken.add(name)
            self.signals += 1
            self.pending.append({"variant": name, "side": ev.candidate_direction, "ts": ts, "due": ts + REAL_DELAY_S,
                                 "signal_price": ev.candidate_ask, "model_prob": ev.model_prob, "edge": ev.edge, "z": ev.distance_z})

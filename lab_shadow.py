"""
LAB SHADOW — the strategies of the 2026-10-10 tape study, run live inside
the market recorder, LOG-ONLY (added 2026-10-10, user: "bu yazdığın
stratejilerin hepsini gölge olarak ekle").

It never trades and never touches the bot: it reads the recorder's own 100 ms
snapshot (the same row that goes into `tape`), so each strategy here is the
exact rule that was replayed on the tape, and writes one row per simulated
trade into `lab_shadow` (market_tape.db). Outcomes are not stored: a report
joins `windows.official_resolution` (or the bot's market_sessions).

Every strategy takes at most MAX_TRADES_PER_WINDOW trades per window (2 since
2026-10-10, user: "tüm stratejilere göre 2 işlem"; `trade_no` 1 or 2, so the
one-trade results stay readable as trade_no = 1), between 30 and 280 s before
the close (the late-favourite ones once, at their own moment), buying 30
shares of the signalled side. A second trade needs a NEW signal: after a fill
the strategy re-arms only once its condition has been false for a sample, so
one long move is not counted twice. The fill is simulated like the study: the 30-share VWAP of
that side REAL_DELAY_S after the signal, from the recorder's live book, fee
0.07 p (1-p); no book, a book over MAX_PRICE or a fill after the window end =
no trade, and the strategy may fire again later in the window (the late
favourites do not retry).

Prices per exchange are the snapshot's last trade price (Binance perp/spot,
Coinbase, Kraken) or bid/ask mid where no trade price is recorded (OKX, Bybit
— those two were not in the study, whose tape had no last price for them).

Strategies (name in `lab_shadow.strategy`):
  momentum <src> <w>s ±$<x>      src moved more than x in the last w seconds
                                 -> buy that direction (6 sources x 4 settings)
  consensus <w>s ±$<x>           the same on the median move of the 6 sources
  perp <w>s ±$<x> with-lead      perp momentum agreeing with the side that
  perp <w>s ±$<x> against-lead     leads (Binance spot vs the window's opening
                                 price: the 60 s Binance TWAP ending at the
                                 window start, rebuilt from the tape by
                                 tape_opening_price) / only the signals
                                 against it
  stale-book perp 2s ±$<x> mid<m>  perp moved more than x in 2 s while the
                                 Polymarket mid of that side moved less than m
  late-favourite <tau>s <=<cap>  at tau seconds before the close buy the
                                 leading side if its fill is at most cap
"""

import bisect
import logging
import sqlite3
import statistics
from typing import Callable, Dict, List, Optional

logger = logging.getLogger("LabShadow")

REAL_DELAY_S = 0.170
SIZE = 30.0
FEE_RATE = 0.07
MAX_PRICE = 0.97
TAU_MIN, TAU_MAX = 30.0, 280.0
WINDOW_S = 300
HISTORY_S = 12.0
LAG_SLACK_S = 1.0            # the look-back sample must be at most this much older than asked
FILL_SLACK_S = 1.0           # the fill sample must be at most this much after the fill moment
MAX_TRADES_PER_WINDOW = 2
OPEN_TWAP_S = 60             # MultiFactorConfig.settlement_twap_seconds: the bot's opening price is a 60 s TWAP
OPEN_MAX_GAP_S = 15.0        # BinanceSpot.FEED_GAP_SECONDS: a longer silence means the feed was down

SOURCES ={"perp": ("last", "bnp_last"), "spot": ("last", "bns_last"), "coinbase": ("last", "cb_last"),
           "kraken": ("last", "kr_last"), "okx": ("mid", "okx_bid", "okx_ask"), "bybit": ("mid", "byb_bid", "byb_ask")}

SCHEMA = ("CREATE TABLE IF NOT EXISTS lab_shadow (id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, "
          "window_start INTEGER, signal_ts REAL, fill_ts REAL, side TEXT, signal_price REAL, fill_price REAL, "
          "size REAL, fee REAL, detail REAL, trade_no INTEGER DEFAULT 1)")
INSERT = ("INSERT INTO lab_shadow (strategy, window_start, signal_ts, fill_ts, side, signal_price, fill_price, "
          "size, fee, detail, trade_no) VALUES (?,?,?,?,?,?,?,?,?,?,?)")


def _strategies() -> List[dict]:
    out = []
    for src in SOURCES:
        for w, x in ((5, 15), (5, 20), (10, 20), (10, 30)):
            out.append({"name": f"momentum {src} {w}s ±${x}", "kind": "momentum", "src": src, "w": w, "x": x})
    for w, x in ((5, 15), (10, 20)):
        out.append({"name": f"consensus {w}s ±${x}", "kind": "consensus", "w": w, "x": x})
    for w, x in ((5, 15), (10, 20)):
        out.append({"name": f"perp {w}s ±${x} with-lead", "kind": "lead", "w": w, "x": x, "with": True})
        out.append({"name": f"perp {w}s ±${x} against-lead", "kind": "lead", "w": w, "x": x, "with": False})
    for x, m in ((10, 0.005), (15, 0.01)):
        out.append({"name": f"stale-book perp 2s ±${x} mid<{m}", "kind": "stale", "x": x, "m": m})
    for tau, cap in ((60, 0.85), (60, 0.92), (120, 0.85)):
        out.append({"name": f"late-favourite {tau}s <={cap}", "kind": "late", "tau": tau, "cap": cap})
    return out


STRATEGIES = _strategies()


class LabShadow:
    def __init__(self, columns, emit: Callable[[tuple], None],
                 opening_price: Callable[[int], Optional[float]]):
        """columns: the tape column names in row order; emit(params) queues one
        INSERT; opening_price(window_start) returns the window's opening price
        or None (looked up again a few seconds later)."""
        self.ix = {name: i for i, name in enumerate(columns)}
        self.emit = emit
        self.opening_price = opening_price
        self.hist_ts: List[float] = []
        self.hist: List[dict] = []
        self.ws = None
        self.op = None
        self.op_next_try = 0.0
        self.taken = set()          # strategies done for this window
        self.count: Dict[str, int] = {}   # fills this window, per strategy
        self.armed: Dict[str, bool] = {}  # False after a fill until the condition is off once
        self.pending: List[dict] = []
        self.trades = 0

    # ---- inputs -----------------------------------------------------------------

    def _values(self, row) -> dict:
        g = lambda name: row[self.ix[name]]
        v = {}
        for src, spec in SOURCES.items():
            if spec[0] == "last":
                v[src] = g(spec[1])
            else:
                b, a = g(spec[1]), g(spec[2])
                v[src] = (b + a) / 2.0 if b is not None and a is not None else None
        for side in ("up", "down"):
            b, a = g(f"pm_{side}_bid"), g(f"pm_{side}_ask")
            v[f"{side}_mid"] = (b + a) / 2.0 if b is not None and a is not None else None
            v[f"{side}_vwap"] = g(f"pm_{side}_vwap30")
        v["ok"] = g("pm_ok") == 1
        return v

    def _past(self, ts: float, w: float) -> Optional[dict]:
        """The sample at or before ts - w, if it is not much older than that."""
        i = bisect.bisect_right(self.hist_ts, ts - w) - 1
        if i < 0 or self.hist_ts[i] < ts - w - LAG_SLACK_S:
            return None
        return self.hist[i]

    @staticmethod
    def _delta(now: dict, past: Optional[dict], key: str) -> Optional[float]:
        if past is None or now.get(key) is None or past.get(key) is None:
            return None
        return now[key] - past[key]

    # ---- one 100 ms sample ------------------------------------------------------------

    def on_sample(self, row) -> None:
        ts, ws = row[0], row[1]
        if ws != self.ws:
            self.ws, self.op, self.op_next_try = ws, None, 0.0
            self.taken, self.pending, self.count, self.armed = set(), [], {}, {}
        v = self._values(row)
        self.hist_ts.append(ts)
        self.hist.append(v)
        cut = bisect.bisect_left(self.hist_ts, ts - HISTORY_S)
        if cut > 0:
            del self.hist_ts[:cut]
            del self.hist[:cut]
        end = ws + WINDOW_S
        tau = end - ts

        # 1) fills that are due
        still = []
        for p in self.pending:
            if ts < p["due"]:
                still.append(p)
                continue
            price = v[f"{p['side'].lower()}_vwap"]
            cap = p["cap"]
            if (v["ok"] and price is not None and price <= cap and ts - p["due"] <= FILL_SLACK_S and ts < end):
                fee = SIZE * FEE_RATE * price * (1.0 - price)
                n = self.count.get(p["name"], 0) + 1
                self.count[p["name"]] = n
                self.emit((p["name"], ws, p["ts"], ts, p["side"], p["signal_price"], price, SIZE, fee, p["detail"], n))
                self.armed[p["name"]] = False
                if p["once"] or n >= MAX_TRADES_PER_WINDOW:
                    self.taken.add(p["name"])
                self.trades += 1
            elif p["once"]:
                self.taken.add(p["name"])
        self.pending = still
        busy = self.taken | {p["name"] for p in self.pending}

        # 2) the window's opening price, for the strategies that need the leading side
        if self.op is None and ts >= self.op_next_try and tau <= TAU_MAX:
            self.op_next_try = ts + 5.0
            try:
                self.op = self.opening_price(ws)
            except Exception:
                self.op = None
        lead = None
        if self.op is not None and v["spot"] is not None:
            lead = "UP" if v["spot"] > self.op else "DOWN"

        # 3) new signals
        if not v["ok"]:
            return
        in_range = TAU_MIN <= tau <= TAU_MAX
        cache: Dict[float, Optional[dict]] = {}
        past = lambda w: cache[w] if w in cache else cache.setdefault(w, self._past(ts, w))
        for s in STRATEGIES:
            name = s["name"]
            if name in busy:
                continue
            kind = s["kind"]
            side, detail, cap, once = None, None, MAX_PRICE, False
            if kind == "late":
                # its own moment only: not later than 5 s after it (e.g. after a restart)
                if tau > s["tau"] or tau < s["tau"] - 5.0 or lead is None:
                    continue
                side, cap, once, detail = lead, s["cap"], True, v["spot"] - self.op
            elif not in_range:
                continue
            elif kind == "momentum":
                d = self._delta(v, past(s["w"]), s["src"])
                if d is not None and abs(d) > s["x"]:
                    side, detail = ("UP" if d > 0 else "DOWN"), d
            elif kind == "consensus":
                pw = past(s["w"])
                ds = [d for d in (self._delta(v, pw, src) for src in SOURCES) if d is not None]
                if ds:
                    d = statistics.median(ds)
                    if abs(d) > s["x"]:
                        side, detail = ("UP" if d > 0 else "DOWN"), d
            elif kind == "lead":
                d = self._delta(v, past(s["w"]), "perp")
                if d is not None and abs(d) > s["x"] and lead is not None:
                    sd = "UP" if d > 0 else "DOWN"
                    if (sd == lead) == s["with"]:
                        side, detail = sd, d
            elif kind == "stale":
                p2 = past(2.0)
                d = self._delta(v, p2, "perp")
                if d is not None and abs(d) > s["x"]:
                    sd = "UP" if d > 0 else "DOWN"
                    dm = self._delta(v, p2, f"{sd.lower()}_mid")
                    if dm is not None and abs(dm) < s["m"]:
                        side, detail = sd, d
            if side is None:
                if in_range or kind == "late":
                    self.armed[name] = True     # condition off: a later signal is a new one
                continue
            if not self.armed.get(name, True):
                continue
            self.pending.append({"name": name, "side": side, "ts": ts, "due": ts + REAL_DELAY_S, "cap": cap,
                                 "once": once, "detail": detail, "signal_price": v[f"{side.lower()}_vwap"]})
            busy.add(name)


def twap_before(samples: List[tuple], end: float, seconds: int = OPEN_TWAP_S,
                max_gap: float = OPEN_MAX_GAP_S) -> Optional[float]:
    """Mean of the 1-second closes over the `seconds` seconds before `end`,
    built the way the bot builds its opening price from WS ticks
    (BinanceSpot.twap_before): each second's close is the last print before
    that second ends, carried forward through seconds without one. samples are
    (ts, price), ascending, starting at least one print before the span. None
    if they do not reach back to its start or leave a hole longer than
    max_gap (the feed was down)."""
    end = int(end)
    start = end - seconds
    before = [s for s in samples if s[0] < start]
    if not before:
        return None
    span = [before[-1]] + [s for s in samples if start <= s[0] < end]
    if any(b[0] - a[0] > max_gap for a, b in zip(span, span[1:])):
        return None
    if end - span[-1][0] > max_gap:
        return None
    closes, i, last = [], 0, None
    for second in range(start, end):
        while i < len(span) and span[i][0] < second + 1:
            last = span[i][1]
            i += 1
        closes.append(last)
    return sum(closes) / seconds


def tape_opening_price(tape_db: str) -> Callable[[int], Optional[float]]:
    """opening_price(window_start) rebuilt from this recorder's own tape: the
    60 s Binance spot TWAP ending at the window start. The bot works out the
    same number when the window opens (run.py _settlement_price) but saves it
    in market_sessions only when the window ends, too late for a strategy that
    needs the leading side while the window is open: until 2026-10-12 the
    with-lead, against-lead and late-favourite strategies read it from there
    and never traded. Read-only; None while the tape does not cover the span
    (a recorder that started less than a minute before the window), so it is
    simply asked again."""
    def lookup(ws: int) -> Optional[float]:
        conn = sqlite3.connect(f"file:{tape_db}?mode=ro", uri=True, timeout=0.5)
        try:
            # A last price older than the gap limit is a feed that stopped, not a quiet market
            # (the tape keeps repeating the last value): left out, it shows up as a hole.
            rows = conn.execute(
                "SELECT ts, bns_last FROM tape WHERE window_start IN (?, ?) AND ts >= ? AND ts < ? "
                "AND bns_last IS NOT NULL AND bns_trade_age_ms <= ? ORDER BY ts",
                (ws - WINDOW_S, ws, ws - OPEN_TWAP_S - OPEN_MAX_GAP_S, ws, OPEN_MAX_GAP_S * 1000)).fetchall()
        finally:
            conn.close()
        return twap_before(rows, ws)
    return lookup

"""
FORWARD RULE EVALUATION — the five candidate rules written down on
2026-10-08, judged only on data recorded after FORWARD_START, never refitted
(added 2026-10-08, user: "ekle").

Why it exists: the rules below came from looking at 9 days of paper trades,
so those days say little about them (the edge cut-offs were picked after
seeing the results). The honest test is data nobody has looked at yet, split
into weekly blocks, priced the way a real order would be filled. Nothing
here changes what the bot trades: the gates stay frozen; this only measures.

The rules (the control is the bot as it is):
  R1  UP: edge >= 0.02 (the bot trades UP from -0.05); DOWN unchanged
      — low-edge trades averaged zero over the 9 days (+$180, then -$191).
  R2  R1 and model probability <= 0.80
      — the model said 82% and won 73% above 0.80.
  R3  DOWN: edge >= 0.10 (the bot trades DOWN from 0.20), edge shadow rows,
      one trade per window — the bot opened only 28 DOWN trades in 9 days.
      0.10, not the best-looking 0.09, so the value is not cherry-picked.
  R4  perp momentum, 5 s +-$15, one trade per window (perp_shadow)
  R5  perp momentum, 10 s +-$20, one trade per window (perp_shadow)
  R6  R4, but only the signals AGAINST the side that is leading at that
      moment (BTC vs the window's opening price) — added 2026-10-08 after
      the 9 days showed those signals earning $1.66 a trade at real-order
      pricing against $0.62 for the ones that follow the lead. That split was
      found in the same data, so it is a hypothesis; only data from
      FORWARD_START on counts.
  R7  (added 2026-10-12, user: "bunları tamamla"; its own clock starts on
      2026-10-13 00:00 Istanbul, R1-R6 keep FORWARD_START) the lab strategy
      "stale-book perp 2s +-$15 mid<0.01" (lab_shadow.py): the Binance perp's
      last trade price moved more than $15 in the last 2 s while the Polymarket
      mid of that side moved less than 0.01 -> buy that side, 30 shares, 30-280 s
      before the close. ONLY THE FIRST TRADE of each window counts (trade_no = 1),
      the lab's second trades do not. Why it is on the list: it came out on top
      of 41 candidates on the 10-02..10-10 data and held up on the 2 days after
      the lab went live (134 trades, +$2.48 a trade at 170 ms), but it was picked
      from those very data, and the best of 41 on the 2 live days alone is not
      significant (adjusted p = 0.15). None of that counts; only data from
      R7_START on. "No retuning" is enforced, not just promised: R7_FINGERPRINT
      is a hash of everything in lab_shadow.py that decides this strategy's
      first trade, and if it changes the report says TANIM DEGISTI and gives no
      verdict. The lab runs inside the market recorder, a separate process, so
      the report also counts the windows the recorder actually recorded; below
      MIN_COVERAGE there is no verdict either.
R1/R2 are read off the bot's own trades (a tighter gate would also have
skipped a window's first signal and maybe traded a later one; that is not
visible in the data, so R1/R2 are an estimate, not a replay).

Pass criteria, fixed in advance (all must hold, on real-order pricing):
  - at least MIN_TRADES trades (MIN_TRADES_DOWN for R3);
  - at least MIN_WEEKS completed weekly blocks, and a positive P&L in at
    least WEEK_POSITIVE_SHARE of them;
  - t-value of the per-trade P&L >= MIN_T;
  - P&L per trade still > 0 with a 250 ms fill instead of 170 ms.
A rule that fails is dropped, not retuned.

"Real-order pricing": the fill price is the 30-share VWAP of the recorded
Polymarket book (market_tape, 100 ms) REAL_DELAY_S after the signal:
~7 ms signing + ~12 ms to Polymarket + 150 ms taker delay (latency_check.py
measured the first two; the delay is from public reports, not verified in
Polymarket's docs). The matching engine's own time is not included, so this
still flatters a real order a little.
"""

import glob
import hashlib
import inspect
import math
import os
import sqlite3
import statistics
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

try:
    import duckdb
except ImportError:           # report still prints the instant-fill columns
    duckdb = None

HERE = os.path.dirname(os.path.abspath(__file__))

REGISTERED_ON = "2026-10-08"
# Istanbul midnight, so the weekly blocks are the same whatever TZ the machine runs in.
FORWARD_START = datetime(2026, 10, 9, tzinfo=timezone(timedelta(hours=3))).timestamp()
WEEK_S = 7 * 86400

REAL_DELAY_S = 0.170
STRESS_DELAY_S = 0.250
FEE_RATE = 0.07               # PositionManager.TAKER_FEE_RATE_CRYPTO
PERP_MAX_FILL_PRICE = 0.97    # perp_shadow.MAX_FILL_PRICE: dearer fills were skipped there too

MIN_TRADES = 150
MIN_TRADES_DOWN = 100
MIN_WEEKS = 4
WEEK_POSITIVE_SHARE = 0.75
MIN_T = 2.0

# R7 — the lab strategy added on 2026-10-12 (see the docstring); its clock is not FORWARD_START.
R7_REGISTERED_ON = "2026-10-12"
R7_NAME = "stale-book perp 2s ±$15 mid<0.01"
R7_START = datetime(2026, 10, 13, tzinfo=timezone(timedelta(hours=3))).timestamp()
RULE_START = {"R7": R7_START}
R7_FINGERPRINT = "4f1b0061e1a19e0026981dafb5cf82b1c2fd47562280682cbd5b3cf78c996452"
LAB_MAX_FILL_PRICE = 0.97     # lab_shadow.MAX_PRICE: dearer fills were not taken there either
MIN_COVERAGE = 0.90           # share of complete windows in which the recorder (and so the lab) must have been up
MIN_TAPE_ROWS = 2700          # a window has ~3000 tape rows (every 100 ms); 90% of them counts as "recorded"


# ---- the rules --------------------------------------------------------------

def _bot_r1(t: dict) -> bool:
    return t["dir"] == "DOWN" or (t["edge"] is not None and t["edge"] >= 0.02)


def _bot_r2(t: dict) -> bool:
    return _bot_r1(t) and t["model_prob"] is not None and t["model_prob"] <= 0.80


# (key, label, source, bot-trade filter, minimum trades)
RULES = [
    ("K", "Kontrol — mevcut ayar, ek filtre yok (botun kendi işlemleri)", "bot", lambda t: True, MIN_TRADES),
    ("R1", "Kural 1 — UP: edge ≥ 0,02 (DOWN değişmez; botun işlemlerinden tahmin)", "bot", _bot_r1, MIN_TRADES),
    ("R2", "Kural 2 — Kural 1 ve model olasılığı ≤ 0,80 (botun işlemlerinden tahmin)", "bot", _bot_r2, MIN_TRADES),
    ("R3", "Kural 3 — DOWN: edge ≥ 0,10, pencere başına 1 işlem (edge gölgesi)", "shadow", None, MIN_TRADES_DOWN),
    ("R4", "Kural 4 — perp momentum 5 sn ±$15, pencere başına 1 işlem", "perp", (5.0, 15.0), MIN_TRADES),
    ("R5", "Kural 5 — perp momentum 10 sn ±$20, pencere başına 1 işlem", "perp", (10.0, 20.0), MIN_TRADES),
    ("R6", "Kural 6 — perp 5 sn ±$15, yalnızca o an ÖNDE OLAN TARAFIN TERSİNE sinyaller (dönüş)", "perp_reversal",
     (5.0, 15.0), MIN_TRADES),
    ("R7", "Kural 7 — lab: stale-book perp 2 sn ±$15 (Polymarket ortası <0,01 oynamış), yalnızca pencerenin 1. işlemi",
     "lab", R7_NAME, MIN_TRADES),
]


# ---- turning the report's rows into one trade shape ------------------------

def _pnl(won: bool, price: float, size: float, fee: float) -> float:
    return size * ((1.0 if won else 0.0) - price) - fee


def _from_bot(rows: List[dict], start: float, keep: Callable[[dict], bool]) -> List[dict]:
    out = []
    for r in rows:
        if r["status"] not in ("win", "loss") or r["entry_time"] < start:
            continue
        sig = r.get("signal_type") or ""
        direction = "UP" if sig.endswith("UP") else "DOWN" if sig.endswith("DOWN") else None
        t = {"ts": r["entry_time"], "dir": direction, "won": r["status"] == "win", "price": r["entry_price"],
             "size": r["size"], "pnl": r["pnl"], "edge": r.get("edge"), "model_prob": r.get("model_prob"),
             "max_price": None}
        if direction and keep(t):
            out.append(t)
    return out


def _first_per_window(rows: List[dict], keep: Callable[[dict], bool]) -> List[dict]:
    seen, out = set(), []
    for r in sorted(rows, key=lambda r: r["ts"]):
        if r["condition_id"] in seen or not keep(r):
            continue
        seen.add(r["condition_id"])
        out.append(r)
    return out


def _from_shadow(rows: List[dict], start: float) -> List[dict]:
    pick = _first_per_window(
        [r for r in rows if r["ts"] >= start and r["filled"] and r["fill_price"] is not None
         and r["outcome"] in ("UP", "DOWN")],
        lambda r: r["direction"] == "DOWN" and r["edge"] is not None and r["edge"] >= 0.10)
    return [{"ts": r["ts"], "dir": "DOWN", "won": r["outcome"] == "DOWN", "price": r["fill_price"],
             "size": r["size"], "pnl": _pnl(r["outcome"] == "DOWN", r["fill_price"], r["size"], r["fee"] or 0.0),
             "edge": r["edge"], "model_prob": None, "max_price": None} for r in pick]


def _from_perp(rows: List[dict], start: float, variant) -> List[dict]:
    pick = _first_per_window(
        [r for r in rows if r["ts"] >= start and r["fill_price"] is not None and r["outcome"] in ("UP", "DOWN")],
        lambda r: (r["window_s"], r["threshold_usd"]) == variant)
    return [{"ts": r["ts"], "dir": r["direction"], "won": r["direction"] == r["outcome"], "price": r["fill_price"],
             "size": r["size"], "pnl": _pnl(r["direction"] == r["outcome"], r["fill_price"], r["size"], r["fee"] or 0.0),
             "edge": None, "model_prob": None, "max_price": PERP_MAX_FILL_PRICE,
             "opening_price": r.get("opening_price")} for r in pick]


def _from_lab(rows: List[dict], start: float, name: str) -> List[dict]:
    """First trade of each window of one lab strategy (lab_shadow rows with trade_no = 1) from `start` on, with a known
    outcome. The 'instant' price is the 30-share VWAP at the signal; the lab's own fill is already 170 ms later, so the
    real-order figure is re-priced from the tape like every other rule's."""
    out = []
    for r in sorted(rows, key=lambda r: r["signal_ts"]):
        if (r["strategy"] != name or (r["trade_no"] or 1) != 1 or r["signal_ts"] < start
                or r["outcome"] not in ("UP", "DOWN") or r["fill_price"] is None):
            continue
        price = r["signal_price"] if r["signal_price"] is not None else r["fill_price"]
        won = r["side"] == r["outcome"]
        out.append({"ts": r["signal_ts"], "dir": r["side"], "won": won, "price": price, "size": r["size"],
                    "pnl": _pnl(won, price, r["size"], r["size"] * FEE_RATE * price * (1 - price)),
                    "edge": None, "model_prob": None, "max_price": LAB_MAX_FILL_PRICE})
    return out


# ---- real-order pricing from the recorded tape -----------------------------------

def _tape_glob(parquet_dir: str) -> Optional[str]:
    pattern = os.path.join(parquet_dir, "market_tape", "tape", "date=*", "*.parquet")
    return pattern if glob.glob(pattern) else None


def _replay(trades: List[dict], pattern: Optional[str], start: float, delays) -> Dict[float, Dict[int, float]]:
    """{delay: {trade index: 30-share VWAP of the bought token `delay` s after
    the signal}} — only where the book was valid and the recorded row is
    within 1 s of that moment. Empty if there is no Parquet tape or duckdb."""
    out = {d: {} for d in delays}
    if not trades or duckdb is None or pattern is None:
        return out
    con = duckdb.connect()
    try:
        con.execute("CREATE TEMP TABLE sig (i INTEGER, ts DOUBLE, ws BIGINT, dir VARCHAR)")
        con.executemany("INSERT INTO sig VALUES (?,?,?,?)",
                        [(i, t["ts"], int(t["ts"] // 300 * 300), t["dir"]) for i, t in enumerate(trades)])
        src = pattern.replace("'", "''")
        for d in delays:
            for i, tape_ts, ok, px, ts in con.execute(f"""
                SELECT s.i, t.ts, t.pm_ok,
                       CASE WHEN s.dir = 'UP' THEN t.pm_up_vwap30 ELSE t.pm_down_vwap30 END, s.ts
                FROM (SELECT i, ts, ts + {float(d)} AS tt, ws, dir FROM sig) s
                ASOF JOIN (SELECT ts, window_start, pm_ok, pm_up_vwap30, pm_down_vwap30
                           FROM read_parquet('{src}', hive_partitioning = true, union_by_name = true)
                           WHERE ts >= {start - 60}) t
                  ON s.ws = t.window_start AND s.tt <= t.ts""").fetchall():
                cap = trades[i]["max_price"]
                if ok == 1 and px is not None and tape_ts - (ts + d) <= 1.0 and (cap is None or px <= cap):
                    out[d][i] = px
    finally:
        con.close()
    return out


def _leaders(trades: List[dict], pattern: Optional[str], start: float) -> Dict[int, float]:
    """{trade index: Binance spot price at the signal} from the recorded tape
    (last row at or before the signal, same window)."""
    if not trades or duckdb is None or pattern is None:
        return {}
    con = duckdb.connect()
    try:
        con.execute("CREATE TEMP TABLE sig (i INTEGER, ts DOUBLE, ws BIGINT)")
        con.executemany("INSERT INTO sig VALUES (?,?,?)",
                        [(i, t["ts"], int(t["ts"] // 300 * 300)) for i, t in enumerate(trades)])
        src = pattern.replace("'", "''")
        return {i: btc for i, btc, tape_ts, ts in con.execute(f"""
            SELECT s.i, t.bns_last, t.ts, s.ts
            FROM sig s ASOF JOIN (SELECT ts, window_start, bns_last
                                  FROM read_parquet('{src}', hive_partitioning = true, union_by_name = true)
                                  WHERE ts >= {start - 60} AND bns_last IS NOT NULL) t
              ON s.ws = t.window_start AND s.ts >= t.ts""").fetchall() if ts - tape_ts <= 5.0}
    finally:
        con.close()


def _against_the_lead(trades: List[dict], btc: Dict[int, float]) -> List[dict]:
    """Keep the trades whose side is NOT the one leading at the signal."""
    out = []
    for i, t in enumerate(trades):
        op = t.get("opening_price")
        if i in btc and op is not None and t["dir"] != ("UP" if btc[i] > op else "DOWN"):
            out.append(t)
    return out


def load_lab_rows(bot_db: Optional[str], parquet_dir: Optional[str] = None, names=(R7_NAME,)) -> Optional[List[dict]]:
    """lab_shadow rows of the given strategies, each with its window's outcome: windows.official_resolution, else the
    bot's own market_sessions result (the same order lab_report.py uses). From the Parquet copy when it is there, else from
    market_tape.db beside the bot database. None if neither can be read."""
    parquet_dir = parquet_dir or os.path.join(HERE, "parquet")
    lab_src = os.path.join(parquet_dir, "market_tape", "lab_shadow", "block-*.parquet")
    win_src = os.path.join(parquet_dir, "market_tape", "windows", "block-*.parquet")
    cols = ("strategy", "window_start", "signal_ts", "side", "signal_price", "fill_price", "size", "fee", "trade_no")
    marks = ", ".join("?" for _ in names)
    try:
        if duckdb is not None and glob.glob(lab_src) and glob.glob(win_src):
            con = duckdb.connect()
            try:
                con.execute("SET memory_limit='400MB'")
                con.execute("SET threads=1")
                rows = [dict(zip(cols, r)) for r in con.execute(
                    "SELECT strategy, window_start, signal_ts, side, signal_price, fill_price, size, fee, COALESCE(trade_no, 1) "
                    f"FROM read_parquet('{lab_src.replace(chr(39), chr(39) * 2)}', union_by_name = true) "
                    f"WHERE strategy IN ({marks})", list(names)).fetchall()]
                official = dict(con.execute(
                    f"SELECT window_start, official_resolution FROM read_parquet('{win_src.replace(chr(39), chr(39) * 2)}', "
                    "union_by_name = true) WHERE official_resolution IN ('UP', 'DOWN')").fetchall())
            finally:
                con.close()
        else:
            tape_db = os.path.join(os.path.dirname(os.path.abspath(bot_db)) if bot_db else HERE, "market_tape.db")
            if not os.path.exists(tape_db):
                return None
            conn = sqlite3.connect(f"file:{tape_db}?mode=ro", uri=True)
            try:
                if not conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'lab_shadow'").fetchone():
                    return None
                has_no = "trade_no" in {r[1] for r in conn.execute("PRAGMA table_info(lab_shadow)")}
                rows = [dict(zip(cols, r)) for r in conn.execute(
                    "SELECT strategy, window_start, signal_ts, side, signal_price, fill_price, size, fee, "
                    f"{'COALESCE(trade_no, 1)' if has_no else '1'} FROM lab_shadow WHERE strategy IN ({marks})",
                    list(names)).fetchall()]
                official = dict(conn.execute("SELECT window_start, official_resolution FROM windows "
                                             "WHERE official_resolution IN ('UP', 'DOWN')").fetchall())
            finally:
                conn.close()
    except Exception:
        return None
    own = {}
    if bot_db and os.path.exists(bot_db):
        try:
            b = sqlite3.connect(f"file:{bot_db}?mode=ro", uri=True)
            try:
                for close_time, res, off in b.execute("SELECT close_time, resolution, official_resolution FROM market_sessions"):
                    if close_time and (off or res) in ("UP", "DOWN"):
                        own[int(round(close_time / 300.0) * 300) - 300] = off or res
            finally:
                b.close()
        except Exception:
            pass
    for r in rows:
        r["outcome"] = official.get(r["window_start"]) or own.get(r["window_start"])
    return rows


def lab_fingerprint(lab=None) -> Optional[str]:
    """SHA-256 over everything in lab_shadow.py that decides R7's first trade: the strategy's own entry, the constants of
    the price/time/fill rules, the perp price source and the code that reads a sample and fires a signal. A change to
    anything else in the file (the second-trade cap, the lead strategies' opening price) leaves it alone. None if
    lab_shadow cannot be imported (the report copied away from the project)."""
    try:
        if lab is None:
            import lab_shadow as lab
        parts = [repr([s for s in lab.STRATEGIES if s["name"] == R7_NAME])]
        parts += [f"{n}={getattr(lab, n)!r}" for n in ("REAL_DELAY_S", "SIZE", "FEE_RATE", "MAX_PRICE", "TAU_MIN", "TAU_MAX",
                                                        "WINDOW_S", "HISTORY_S", "LAG_SLACK_S", "FILL_SLACK_S")]
        parts.append(repr(lab.SOURCES["perp"]))
        parts += [inspect.getsource(f) for f in (lab.LabShadow._values, lab.LabShadow._past, lab.LabShadow._delta,
                                                 lab.LabShadow.on_sample)]
        return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
    except Exception:
        return None


def lab_coverage(pattern: Optional[str], start: float, now: float) -> Optional[tuple]:
    """(recorded, elapsed): of the complete 5-minute windows since `start`, how many have at least MIN_TAPE_ROWS tape rows,
    i.e. the recorder (which carries the lab) was up for most of them. The last 10 minutes are left out because the Parquet
    copy trails the recorder by up to 5. None if there is no Parquet tape or no duckdb."""
    if duckdb is None or pattern is None:
        return None
    first = int(math.ceil(start / 300.0)) * 300
    last = int((now - 600) // 300) * 300
    if last <= first:
        return 0, 0
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='400MB'")
        con.execute("SET threads=1")
        rows = con.execute(f"SELECT window_start, count(*) FROM read_parquet('{pattern.replace(chr(39), chr(39) * 2)}', "
                           f"hive_partitioning = true, union_by_name = true) WHERE ts >= {first} AND ts < {last} "
                           "GROUP BY 1").fetchall()
    finally:
        con.close()
    return sum(1 for ws, n in rows if first <= ws < last and n >= MIN_TAPE_ROWS), (last - first) // 300


def _real_pnl(t: dict, price: float) -> float:
    return _pnl(t["won"], price, t["size"], t["size"] * FEE_RATE * price * (1 - price))


def _t_value(values: List[float]) -> Optional[float]:
    if len(values) < 3:
        return None
    sd = statistics.stdev(values)
    return None if sd == 0 else statistics.mean(values) / (sd / math.sqrt(len(values)))


# ---- evaluation ------------------------------------------------------------------

def evaluate_rule(trades: List[dict], prices: Dict[float, Dict[int, float]], start: float, now: float,
                  min_trades: int) -> dict:
    """Numbers and the verdict for one rule. `prices` is _replay()'s output
    for exactly these trades."""
    real = {i: _real_pnl(trades[i], p) for i, p in prices.get(REAL_DELAY_S, {}).items()}
    stress = {i: _real_pnl(trades[i], p) for i, p in prices.get(STRESS_DELAY_S, {}).items()}
    elapsed_weeks = int(max(0.0, now - start) // WEEK_S)
    weeks = {}
    for i, t in enumerate(trades):
        w = weeks.setdefault(int((t["ts"] - start) // WEEK_S), {"n": 0, "wins": 0, "instant": 0.0, "real": 0.0, "n_real": 0})
        w["n"] += 1; w["wins"] += t["won"]; w["instant"] += t["pnl"]
        if i in real:
            w["real"] += real[i]; w["n_real"] += 1
    done = [w for k, w in sorted(weeks.items()) if k < elapsed_weeks and w["n_real"]]
    positive_share = sum(w["real"] > 0 for w in done) / len(done) if done else None
    t_real = _t_value(list(real.values()))
    stress_mean = statistics.mean(stress.values()) if stress else None
    checks = {
        "n": (len(real), len(real) >= min_trades),
        "weeks": (positive_share, elapsed_weeks >= MIN_WEEKS and positive_share is not None
                  and positive_share >= WEEK_POSITIVE_SHARE),
        "t": (t_real, t_real is not None and t_real >= MIN_T),
        "stress": (stress_mean, stress_mean is not None and stress_mean > 0),
    }
    if len(real) < min_trades or elapsed_weeks < MIN_WEEKS:
        verdict = "HENÜZ KARAR YOK"
    elif all(ok for _, ok in checks.values()):
        verdict = "GEÇTİ"
    else:
        verdict = "KALDI"
    return {"n": len(trades), "wins": sum(t["won"] for t in trades), "instant": sum(t["pnl"] for t in trades),
            "n_real": len(real), "real": sum(real.values()), "weeks": weeks, "elapsed_weeks": elapsed_weeks,
            "checks": checks, "verdict": verdict, "done_weeks": len(done)}


def _mark(ok: bool) -> str:
    return "✓" if ok else "✗"


def forward_report(bot_trades: List[dict], shadow_rows: Optional[List[dict]], perp_rows: Optional[List[dict]],
                   parquet_dir: Optional[str] = None, start: float = FORWARD_START,
                   now: Optional[float] = None, out=print, lab_rows: Optional[List[dict]] = None) -> None:
    now = time.time() if now is None else now
    parquet_dir = parquet_dir or os.path.join(HERE, "parquet")
    stamp = lambda ts: datetime.fromtimestamp(ts, timezone(timedelta(hours=3))).strftime("%Y-%m-%d")
    out(f"\n{'=' * 72}\nİLERİYE DÖNÜK KURAL DEĞERLENDİRMESİ — {REGISTERED_ON} tarihinde önceden belirlendi, "
        f"{stamp(start)} tarihinden beri\n{'=' * 72}")
    if now < start:
        out(f"Henüz başlamadı (ileriye dönük dönemin başlangıcı: {stamp(start)}). Bu tarihten önceki veri kuralların "
            f"seçildiği veridir, sınama sayılmaz.")
        return
    pattern = _tape_glob(parquet_dir)
    if pattern is None or duckdb is None:
        out("(Gerçek emir fiyatı hesaplanamadı: " + ("duckdb yok" if duckdb is None else "Parquet tape bulunamadı")
            + " — yalnızca anlık dolum sütunu dolu, karar verilmez.)")
    out(f"Dolum: sinyalden {REAL_DELAY_S * 1000:.0f} ms sonraki Polymarket kitabı (≈7 ms imzalama + ≈12 ms ağ + 150 ms taker "
        f"beklemesi; eşleştirme süresi dahil değil), stres {STRESS_DELAY_S * 1000:.0f} ms. "
        f"Geçme şartı: ≥{MIN_TRADES} işlem (Kural 3: ≥{MIN_TRADES_DOWN}), ≥{MIN_WEEKS} tamamlanmış hafta ve "
        f"%{WEEK_POSITIVE_SHARE * 100:.0f}'inde artı, t ≥ {MIN_T:g}, stres dolumunda işlem başı > 0.")
    for key, label, source, selector, min_trades in RULES:
        # A rule added later keeps its own clock; a test run (--forward-start) moves every clock to the date given.
        rule_start = start if start != FORWARD_START else RULE_START.get(key, start)
        fp = cov = None
        out(f"\n--- {label} ---")
        if rule_start != start:
            out(f"  (kendi saati: {stamp(rule_start)} tarihinden beri; {R7_REGISTERED_ON} tarihinde eklendi)")
        if now < rule_start:
            out(f"  Henüz başlamadı (başlangıç: {stamp(rule_start)}). Bu tarihten önceki veri seçim verisidir, sınama sayılmaz.")
            continue
        if source == "bot":
            trades = _from_bot(bot_trades, rule_start, selector)
        elif source == "shadow":
            trades = _from_shadow(shadow_rows or [], rule_start)
        elif source == "lab":
            if lab_rows is None:
                out("  (lab verisi okunamadı: Parquet lab_shadow / market_tape.db bulunamadı — karar verilmez)")
                continue
            trades = _from_lab(lab_rows, rule_start, selector)
            fp = lab_fingerprint()
            if fp is None:
                out("  tanım parmak izi: kontrol edilemedi (lab_shadow.py içe aktarılamadı)")
            elif fp == R7_FINGERPRINT:
                out(f"  tanım parmak izi: ✓ kayıttakiyle aynı ({fp[:12]})")
            else:
                out(f"  ⚠ TANIM DEĞİŞTİ: lab_shadow.py'de R7'nin çalışma kodu/parametreleri kayıt tarihindekinden farklı "
                    f"(kayıtlı {R7_FINGERPRINT[:12]}, şimdi {fp[:12]}). Değişiklik bu stratejiyi etkiliyorsa saat sıfırlanmalı; "
                    f"etkilemiyorsa fark incelenip parmak izi gerekçesiyle yeniden kaydedilmeli. O zamana kadar karar verilmez.")
            cov = lab_coverage(pattern, rule_start, now)
            if cov is None:
                out("  kapsama: hesaplanamadı (Parquet tape ya da duckdb yok)")
            elif cov[1] == 0:
                out("  kapsama: henüz tamamlanmış (10 dk'dan eski) pencere yok")
            else:
                share_cov = cov[0] / cov[1]
                out(f"  kapsama: kaydedicinin açık olduğu {cov[0]}/{cov[1]} pencere (%{share_cov * 100:.0f}) "
                    f"{_mark(share_cov >= MIN_COVERAGE)} (alt sınır %{MIN_COVERAGE * 100:.0f}; altında karar verilmez)")
        else:
            trades = _from_perp(perp_rows or [], rule_start, selector)
            if source == "perp_reversal":
                trades = _against_the_lead(trades, _leaders(trades, pattern, rule_start))
        if not trades:
            out("  Bu dönemde henüz işlem yok.")
            continue
        trades.sort(key=lambda t: t["ts"])
        prices = _replay(trades, pattern, rule_start, (REAL_DELAY_S, STRESS_DELAY_S))
        res = evaluate_rule(trades, prices, rule_start, now, min_trades)
        out(f"  {'hafta':<22}{'işlem':>6}{'win%':>7}{'anlık P&L':>12}{'gerçek emir P&L (n)':>26}")
        for k, w in sorted(res["weeks"].items()):
            first = rule_start + k * WEEK_S
            span = f"{stamp(first)} → {stamp(first + WEEK_S - 1)}"
            tag = "" if k < res["elapsed_weeks"] else " (sürüyor)"
            out(f"  {span:<22}{w['n']:>6}{100 * w['wins'] / w['n']:>6.1f}%{w['instant']:>12.2f}"
                f"{w['real']:>20.2f} ({w['n_real']}){tag}")
        out(f"  toplam: {res['n']} işlem, win %{100 * res['wins'] / res['n']:.1f}, anlık P&L ${res['instant']:+.2f}; "
            f"gerçek emir P&L ${res['real']:+.2f} ({res['n_real']} işlem)")
        c = res["checks"]
        share = c["weeks"][0]
        t_txt = "—" if c["t"][0] is None else f"{c['t'][0]:.2f}"
        s_txt = "—" if c["stress"][0] is None else f"${c['stress'][0]:+.2f}"
        out(f"  ölçütler: işlem {c['n'][0]}/{min_trades} {_mark(c['n'][1])} | "
            f"artı hafta {'—' if share is None else f'%{share * 100:.0f}'} "
            f"({res['done_weeks']}/{MIN_WEEKS} hafta) {_mark(c['weeks'][1])} | t={t_txt} {_mark(c['t'][1])} | "
            f"{STRESS_DELAY_S * 1000:.0f} ms işlem başı {s_txt} {_mark(c['stress'][1])}")
        if key == "K":
            out("  (karşılaştırma satırı: karar verilmez)")
        else:
            verdict = res["verdict"]
            if source == "lab" and verdict != "HENÜZ KARAR YOK":
                # a verdict is only worth giving if the definition is the registered one and the recorder was up
                if fp is not None and fp != R7_FINGERPRINT:
                    verdict = "TANIM DEĞİŞTİ — karar verilmez"
                elif cov is not None and cov[1] and cov[0] / cov[1] < MIN_COVERAGE:
                    verdict = "KAPSAMA YETERSİZ — karar verilmez"
            out(f"  karar: {verdict}")
    n_rules = sum(1 for r in RULES if r[0] != "K")
    z = statistics.NormalDist().inv_cdf(1 - 0.05 / n_rules)
    out(f"\nNot (bilgi, ölçüt değil): {n_rules} kural birlikte sınanıyor. Tek bir kural için t ≥ {MIN_T:g} yeterli sayılır; "
        f"{n_rules} kuralın en iyisinin şansla çıkmaması için tek yönlü %5 düzeyinde t ≳ {z:.2f} gerekir.")

"""
STRATEGY REPLAY — three strategies from an outside design document, replayed
on the recorded 100 ms tape (stage 1 of adding them; added 2026-10-08).

Nothing here touches the bot, the recorder or any database: it reads the
Parquet copy (parquet_sync.py) and prints a report. Stage 2 (a live,
log-only version in the recorder) is only worth building if this shows
something.

The strategies, as the document specifies them (it says itself that its
parameters are first guesses, not optimised):
  fair_value   tau 20-270 s, q >= 0.55, edge >= 0.05, take the side with the
               larger edge, hold to settlement
  zscore_mr    series mid_up - q_up (one sample a second, last 60, at least
               30); enter when |z| >= 2 and |dev| >= 0.03 (z > 0 -> buy DOWN),
               edge >= 0.02, tau 30-270 s; leave when |z| < 0.5 with a taker
               sell at best bid - 0.02, 300 ms later (if it does not fill,
               hold)
  end_window   tau 3-30 s, |ln(S/K)| > 3 sigma sqrt(tau), side = S vs K,
               VWAP <= 0.99, edge >= 0.01
  (v84_baseline is the bot itself, so it is not replayed.)

Shared: q_up = Phi(ln(S/K) / (sigma sqrt(tau))); edge = q - VWAP20 - fee(VWAP20)
with fee(p) = 0.07 p (1-p); sigma = EWMA of 1 s Binance spot mid returns
(half-life 600 s, start 8.9e-5, floor 1.5e-5, at least 60 returns, gaps over
30 s skipped); evaluated every 0.25 s; Chainlink no older than 3 s; spread of
the bought token <= 0.04; one position per strategy per window, at most 3
entry attempts. Fill: limit = VWAP20 + 0.02 (cap 0.99), the book 300 ms later
is consumed up to that limit, fee on every level, under 5 shares = missed.
Size: Kelly (0.25 x kelly, at most 2% of a $1000 bankroll that follows its
own P&L, 5-100 shares) or a fixed 30 shares, which is what the bot's own
results use, so the two can be compared.

Two readings of S and K are run side by side, because the document and the
bot disagree: "doc" (S = Chainlink, K = the first Chainlink tick at or after
the window start, at most 3 s late) and "bot" (S = Binance spot mid, K = the
bot's 60 s TWAP opening price from market_sessions).

What the tape cannot give, and what this does instead:
  - no 20-share VWAP: the book is rebuilt as a cost curve from the best ask
    and its size plus the 10/30/50/100/200-share VWAPs, and VWAP20 and the
    limit-consuming fill are read off that curve (levels in between are
    approximated);
  - no bid depth: an exit sells at the best bid for at most the best bid's
    size, the rest is held to settlement;
  - no priceToBeat fallback: a window without a Chainlink tick near its
    start is skipped in the "doc" reading.
These make the replay a fair test of the idea, not an exact copy of a live fill.

Usage:
    python strategy_replay.py --parquet parquet            # on the server
    python strategy_replay.py --parquet /path/to/copy --delay 0.170
"""

import argparse
import bisect
import glob
import math
import os
import statistics
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import duckdb

FEE_RATE = 0.07
LAMBDA = 0.5 ** (1.0 / 600.0)
SIGMA_INIT = 8.9e-5
SIGMA_FLOOR = 1.5e-5
MIN_RETURNS = 60
EVAL_STEP_S = 0.25
CL_MAX_AGE_MS = 3000.0
MAX_SPREAD = 0.04
VWAP_SHARES = 20
LIMIT_SLIP = 0.02
LIMIT_CAP = 0.99
MIN_SHARES = 5
MAX_SHARES = 100
KELLY_FRACTION = 0.25
KELLY_CAP = 0.02
BANKROLL0 = 1000.0
MAX_ATTEMPTS = 3
FIXED_SHARES = 30
SIZES = (10, 30, 50, 100, 200)
TZ = timezone(timedelta(hours=3))

# row layout of the tape query
COLS = ("ts, window_start, cl_price, cl_age_ms, bns_bid, bns_ask, pm_ok, "
        "pm_up_bid, pm_up_ask, pm_up_bid_qty, pm_up_ask_qty, pm_up_vwap10, pm_up_vwap30, pm_up_vwap50, pm_up_vwap100, pm_up_vwap200, "
        "pm_down_bid, pm_down_ask, pm_down_bid_qty, pm_down_ask_qty, pm_down_vwap10, pm_down_vwap30, pm_down_vwap50, pm_down_vwap100, pm_down_vwap200")
TS, WS, CL, CLAGE, BB, BA, OK = range(7)
SIDE_IDX = {"UP": (7, 8, 9, 10, 11), "DOWN": (16, 17, 18, 19, 20)}   # bid, ask, bid_qty, ask_qty, first vwap

STRATEGIES = ("fair_value", "zscore_mr", "end_window")
VARIANTS = ("doc", "bot")


def phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def fee(p: float) -> float:
    return FEE_RATE * p * (1.0 - p)


# ---- the order book as a cost curve ------------------------------------------

def book_curve(row, side):
    """[(shares, cumulative cost), ...] of buying `side`, from the best ask and
    its size plus the 5 recorded VWAPs; monotone, starting at (0, 0)."""
    bid_i, ask_i, bq_i, aq_i, vw_i = SIDE_IDX[side]
    ask, ask_qty = row[ask_i], row[aq_i]
    pts = [(0.0, 0.0)]
    if ask is not None and ask_qty:
        pts.append((float(ask_qty), ask * ask_qty))
    for k, n in enumerate(SIZES):
        v = row[vw_i + k]
        if v is not None:
            pts.append((float(n), n * v))
    pts.sort()
    out = [pts[0]]
    for n, c in pts[1:]:
        if n > out[-1][0] and c >= out[-1][1] - 1e-9:
            out.append((n, c))
    return out


def consume(curve, n_req, limit=None):
    """Buy up to n_req shares along the curve without paying a marginal price
    above `limit`. Returns (shares, cost, fees); fees are charged per segment."""
    filled = cost = fees = 0.0
    for (n0, c0), (n1, c1) in zip(curve, curve[1:]):
        m = (c1 - c0) / (n1 - n0)
        if limit is not None and m > limit + 1e-12:
            break
        take = min(n1 - n0, n_req - filled)
        if take <= 0:
            break
        filled += take
        cost += take * m
        fees += take * fee(m)
        if filled >= n_req - 1e-9:
            break
    return filled, cost, fees


def vwap20(curve):
    filled, cost, _ = consume(curve, VWAP_SHARES)
    return cost / VWAP_SHARES if filled >= VWAP_SHARES - 1e-9 else None


# ---- sigma ----------------------------------------------------------------------

def sigma_series(per_second):
    """{second: (sigma, returns used)} from [(second, spot mid)], the 1 s
    EWMA of the document."""
    var = SIGMA_INIT ** 2
    n = 0
    prev = None
    out = {}
    for s, mid in per_second:
        if prev is not None:
            dt = s - prev[0]
            if 0 < dt <= 30:
                r = math.log(mid / prev[1])
                w = LAMBDA ** dt
                var = w * var + (1.0 - w) * r * r / dt
                n += 1
        out[s] = (max(math.sqrt(var), SIGMA_FLOOR), n)
        prev = (s, mid)
    return out


# ---- one strategy instance --------------------------------------------------------

class Instance:
    def __init__(self, variant, mode, strategy):
        self.variant, self.mode, self.strategy = variant, mode, strategy
        self.bankroll = BANKROLL0
        self.trades = []
        self.missed = defaultdict(int)
        self.reset()

    def reset(self):
        self.attempts = 0
        self.pos = None
        self.blocked_until = 0.0
        self.exit_tried = False

    def shares_for(self, q, price):
        if self.mode == "fixed":
            return FIXED_SHARES
        p1 = price + fee(price)
        if q <= p1:
            return 0
        kelly = (q - p1) / (1.0 - p1)
        return max(MIN_SHARES, min(MAX_SHARES, int(self.bankroll * min(kelly * KELLY_FRACTION, KELLY_CAP) / p1)))


def _signal(strategy, tau, ctx, series):
    """(side, q_side, vwap20) or None for one strategy at one moment."""
    sides = ctx["sides"]
    q_up = ctx["q_up"]
    if strategy == "fair_value":
        if not (20 <= tau <= 270):
            return None
        best = None
        for side, q in (("UP", q_up), ("DOWN", 1.0 - q_up)):
            v = sides[side]["v20"]
            if v is None or q < 0.55 or sides[side]["spread"] > MAX_SPREAD:
                continue
            edge = q - v - fee(v)
            if edge >= 0.05 and (best is None or edge > best[0]):
                best = (edge, side, q, v)
        return best[1:] if best else None
    if strategy == "zscore_mr":
        if not (30 <= tau <= 270) or len(series) < 30:
            return None
        sd = statistics.pstdev(series)
        if sd < 1e-4:
            return None
        dev = ctx["mid_up"] - q_up
        z = dev / sd
        if abs(z) < 2.0 or abs(dev) < 0.03:
            return None
        side = "DOWN" if z > 0 else "UP"
        q = q_up if side == "UP" else 1.0 - q_up
        v = sides[side]["v20"]
        if v is None or sides[side]["spread"] > MAX_SPREAD or q - v - fee(v) < 0.02:
            return None
        return side, q, v
    if strategy == "end_window":
        if not (3 <= tau <= 30):
            return None
        if abs(math.log(ctx["S"] / ctx["K"])) <= 3.0 * ctx["sigma"] * math.sqrt(tau):
            return None
        side = "UP" if ctx["S"] > ctx["K"] else "DOWN"
        q = q_up if side == "UP" else 1.0 - q_up
        v = sides[side]["v20"]
        if v is None or v > 0.99 or sides[side]["spread"] > MAX_SPREAD or q - v - fee(v) < 0.01:
            return None
        return side, q, v
    return None


def run_window(rows, ws, outcome, K, variant, sigmas, instances, delay):
    """Replay one window for every instance of one variant (all modes and
    strategies); updates their positions/trades."""
    ts_list = [r[TS] for r in rows]
    end = ws + 300
    for inst in instances:
        inst.reset()
    series = deque(maxlen=60)
    last_sec = None
    t_next = 0.0
    for idx, r in enumerate(rows):
        t = r[TS]
        if t < t_next:
            continue
        t_next = t + EVAL_STEP_S
        tau = end - t
        if tau <= 0:
            break
        if r[OK] != 1:
            continue
        if variant == "doc":
            S = r[CL] if (r[CLAGE] is not None and r[CLAGE] <= CL_MAX_AGE_MS) else None
        else:
            S = (r[BB] + r[BA]) / 2.0 if r[BB] is not None and r[BA] is not None else None
        sg = sigmas.get(int(t) - 1)
        if S is None or K is None or sg is None or sg[1] < MIN_RETURNS:
            continue
        sigma = sg[0]
        sides = {}
        for side in ("UP", "DOWN"):
            bid_i, ask_i = SIDE_IDX[side][0], SIDE_IDX[side][1]
            if r[bid_i] is None or r[ask_i] is None:
                sides = None
                break
            cv = book_curve(r, side)
            sides[side] = {"curve": cv, "v20": vwap20(cv), "spread": r[ask_i] - r[bid_i]}
        if sides is None:
            continue
        q_up = phi(math.log(S / K) / (sigma * math.sqrt(tau)))
        mid_up = (r[SIDE_IDX["UP"][0]] + r[SIDE_IDX["UP"][1]]) / 2.0
        if int(t) != last_sec:
            last_sec = int(t)
            series.append(mid_up - q_up)
        ctx = {"S": S, "K": K, "sigma": sigma, "q_up": q_up, "mid_up": mid_up, "sides": sides}
        for inst in instances:
            _step(inst, rows, ts_list, idx, t, tau, end, ctx, series, delay)
    # settlement
    for inst in instances:
        pos = inst.pos
        if pos is None:
            continue
        payout = pos["shares_left"] * (1.0 if pos["side"] == outcome else 0.0)
        pnl = pos["proceeds"] + payout - pos["cost"] - pos["fees"]
        inst.trades.append({"ts": pos["ts"], "ws": ws, "side": pos["side"], "shares": pos["shares"],
                            "price": pos["cost"] / pos["shares"], "won": pos["side"] == outcome, "pnl": pnl,
                            "exited": pos["proceeds"] > 0})
        inst.bankroll += pnl


def _step(inst, rows, ts_list, idx, t, tau, end, ctx, series, delay):
    # --- exit of an open zscore_mr position --------------------------------------
    if inst.pos is not None:
        if inst.strategy == "zscore_mr" and not inst.exit_tried and len(series) >= 30:
            sd = statistics.pstdev(series)
            if sd >= 1e-4 and abs((ctx["mid_up"] - ctx["q_up"]) / sd) < 0.5:
                inst.exit_tried = True
                pos = inst.pos
                bid_i, _, bq_i, _, _ = SIDE_IDX[pos["side"]]
                bid0 = rows[idx][bid_i]
                j = bisect.bisect_left(ts_list, t + delay)
                if bid0 is not None and j < len(rows) and ts_list[j] <= end and rows[j][OK] == 1:
                    bid_j, qty_j = rows[j][bid_i], rows[j][bq_i]
                    if bid_j is not None and qty_j and bid_j >= bid0 - LIMIT_SLIP:
                        sold = min(pos["shares_left"], float(qty_j))
                        pos["proceeds"] += sold * bid_j
                        pos["fees"] += sold * fee(bid_j)
                        pos["shares_left"] -= sold
        return
    if inst.attempts >= MAX_ATTEMPTS or t < inst.blocked_until:
        return
    sig = _signal(inst.strategy, tau, ctx, series)
    if sig is None:
        return
    side, q, v20 = sig
    n_req = inst.shares_for(q, v20)
    if n_req < MIN_SHARES:
        return
    inst.attempts += 1
    inst.blocked_until = t + delay
    limit = min(v20 + LIMIT_SLIP, LIMIT_CAP)
    j = bisect.bisect_left(ts_list, t + delay)
    if j >= len(rows) or ts_list[j] > end:
        inst.missed["pencere bitti"] += 1
        return
    row2 = rows[j]
    if row2[OK] != 1:
        inst.missed["bağlantı koptu"] += 1
        return
    ask_i = SIDE_IDX[side][1]
    if row2[ask_i] is None:
        inst.missed["defter boş"] += 1
        return
    filled, _, _ = consume(book_curve(row2, side), n_req, limit)
    n_f = int(math.floor(filled + 1e-9))
    if n_f < MIN_SHARES:
        inst.missed["limit içinde likidite yok"] += 1
        return
    _, cost, fees = consume(book_curve(row2, side), n_f, limit)
    inst.pos = {"side": side, "shares": n_f, "shares_left": float(n_f), "cost": cost, "fees": fees,
                "proceeds": 0.0, "ts": t}


# ---- data ----------------------------------------------------------------------------

def _glob(parquet_dir, db, table):
    return os.path.join(parquet_dir, db, table, "block-*.parquet")


def load(parquet_dir, days=None):
    tape_glob = os.path.join(parquet_dir, "market_tape", "tape", "date=*", "*.parquet")
    if not glob.glob(tape_glob):
        sys.exit(f"{tape_glob}: Parquet tape bulunamadı")
    src = f"read_parquet('{tape_glob}', hive_partitioning = true, union_by_name = true)"
    flt = "" if not days else " AND date IN (" + ",".join(f"'{d}'" for d in days) + ")"
    con = duckdb.connect()
    per_second = con.execute(f"""SELECT CAST(floor(ts) AS BIGINT) AS s, arg_max((bns_bid + bns_ask) / 2, ts)
                                 FROM {src} WHERE bns_bid IS NOT NULL AND bns_ask IS NOT NULL
                                 GROUP BY s ORDER BY s""").fetchall()
    sigmas = sigma_series(per_second)
    wins = {r[0]: r[1:] for r in con.execute(
        f"""SELECT w.window_start, coalesce(w.official_resolution, m.official_resolution, m.resolution), m.opening_price
            FROM read_parquet('{_glob(parquet_dir, 'market_tape', 'windows')}', union_by_name = true) w
            LEFT JOIN read_parquet('{_glob(parquet_dir, 'bot_trades', 'market_sessions')}', union_by_name = true) m
              ON m.session_id = w.condition_id""").fetchall()}
    rows_cur = con.execute(f"SELECT {COLS} FROM {src} WHERE ts > 1790968769{flt} ORDER BY ts")
    return con, rows_cur, sigmas, wins


def windows_stream(rows_cur):
    cur_ws, buf = None, []
    while True:
        chunk = rows_cur.fetchmany(50000)
        if not chunk:
            break
        for r in chunk:
            if r[WS] != cur_ws:
                if buf:
                    yield cur_ws, buf
                cur_ws, buf = r[WS], []
            buf.append(r)
    if buf:
        yield cur_ws, buf


def doc_open(rows, ws):
    """K of the 'doc' reading: Chainlink price of the first tick at or after
    the window start, if it came within 3 s."""
    for r in rows:
        if r[CLAGE] is None or r[CL] is None:
            continue
        tick = r[TS] - r[CLAGE] / 1000.0
        if tick >= ws - 0.001:
            return r[CL] if tick <= ws + 3.0 else None
    return None


# ---- report ------------------------------------------------------------------------------

def t_value(vals):
    if len(vals) < 3:
        return None
    sd = statistics.stdev(vals)
    return None if sd == 0 else statistics.mean(vals) / (sd / math.sqrt(len(vals)))


def report(instances, n_windows, skipped, delay):
    print(f"\nSTRATEJİ TEKRAR OYNATMA — {n_windows} pencere; gecikme {delay * 1000:.0f} ms; "
          f"atlanan pencere (veri yok/eksik): {skipped}")
    for variant in VARIANTS:
        label = ("S = Chainlink, K = penceredeki ilk Chainlink tiki (dokümandaki gibi)" if variant == "doc"
                 else "S = Binance spot, K = botun 60 sn TWAP açılış fiyatı")
        for mode in ("fixed", "kelly"):
            print(f"\n--- {variant}: {label} | boyut: {'sabit 30 hisse' if mode == 'fixed' else 'Kelly (spec)'} ---")
            print(f"{'strateji':<12}{'işlem':>6}{'ıskalanan':>10}{'win%':>7}{'P&L $':>10}{'işlem başı':>12}{'t':>7}{'ort. fiyat':>12}")
            for inst in instances:
                if inst.variant != variant or inst.mode != mode:
                    continue
                tr = inst.trades
                missed = sum(inst.missed.values())
                if not tr:
                    print(f"{inst.strategy:<12}{0:>6}{missed:>10}")
                    continue
                pn = [t["pnl"] for t in tr]
                tv = t_value(pn)
                print(f"{inst.strategy:<12}{len(tr):>6}{missed:>10}{100 * sum(t['won'] for t in tr) / len(tr):>6.1f}%"
                      f"{sum(pn):>10.2f}{statistics.mean(pn):>12.2f}{('—' if tv is None else f'{tv:.2f}'):>7}"
                      f"{statistics.mean(t['price'] for t in tr):>12.3f}")
    print("\nGünlük P&L (sabit 30 hisse):")
    days = sorted({datetime.fromtimestamp(t["ts"], TZ).strftime("%m-%d") for i in instances for t in i.trades})
    head = f"{'gün':<7}" + "".join(f"{(i.variant + '/' + i.strategy):>20}" for i in instances if i.mode == "fixed")
    print(head)
    for d in days:
        line = f"{d:<7}"
        for i in instances:
            if i.mode == "fixed":
                line += f"{sum(t['pnl'] for t in i.trades if datetime.fromtimestamp(t['ts'], TZ).strftime('%m-%d') == d):>20.2f}"
        print(line)
    print("\nIskalanma nedenleri (sabit 30 hisse):")
    for i in instances:
        if i.mode == "fixed" and i.missed:
            print(f"  {i.variant}/{i.strategy}: " + ", ".join(f"{k} {v}" for k, v in sorted(i.missed.items())))


def main():
    parser = argparse.ArgumentParser(description="Replay three outside strategies on the recorded tape")
    parser.add_argument("--parquet", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "parquet"))
    parser.add_argument("--delay", type=float, default=0.3, help="seconds between signal and fill (document: 0.3)")
    parser.add_argument("--days", nargs="*", help="only these days, e.g. 2026-10-07")
    parser.add_argument("--max-windows", type=int, default=0, help="stop after this many windows (for trying it out)")
    args = parser.parse_args()
    con, rows_cur, sigmas, wins = load(args.parquet, args.days)
    instances = [Instance(v, m, s) for v in VARIANTS for m in ("fixed", "kelly") for s in STRATEGIES]
    n_windows = skipped = 0
    for ws, rows in windows_stream(rows_cur):
        info = wins.get(ws)
        if not info or info[0] not in ("UP", "DOWN") or len(rows) < 2500 or rows[0][TS] > ws + 2 or rows[-1][TS] < ws + 297:
            skipped += 1
            continue
        outcome, opening = info[0], info[1]
        n_windows += 1
        for variant, K in (("doc", doc_open(rows, ws)), ("bot", opening)):
            if K is None:
                continue
            run_window(rows, ws, outcome, K, variant, sigmas, [i for i in instances if i.variant == variant], args.delay)
        if args.max_windows and n_windows >= args.max_windows:
            break
    report(instances, n_windows, skipped, args.delay)


if __name__ == "__main__":
    main()

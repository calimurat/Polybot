"""
ENGINE REPORT — P&L of K, R1, R2 and R3 measured from the continuous record alone (engine_shadow.py), read-only.

The bot's own signal engine is run on every tape sample, so these four exist whether or not the bot trades. Per variant and
window the first signal counts; "anlık" is the signal-time 30-share VWAP (the bot's paper fill), "170 ms" the fill 170 ms
later (NULL = no valid book at that sample, not counted there). Outcome: windows.official_resolution, else the bot's own
market_sessions result (the order lab_report.py uses).

    python engine_report.py                 # everything recorded
    python engine_report.py --since 2026-10-13
"""

import argparse
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
TZ = timezone(timedelta(hours=3))
FEE = 0.07


def pnl(size, won, price):
    return size * ((1.0 if won else 0.0) - price) - size * FEE * price * (1 - price)


def main():
    ap = argparse.ArgumentParser(description="P&L of the engine shadow (K, R1, R2, R3)")
    ap.add_argument("--tape-db", default=os.path.join(HERE, "market_tape.db"))
    ap.add_argument("--bot-db", default=os.path.join(HERE, "bot_trades.db"))
    ap.add_argument("--since", help="YYYY-MM-DD (Istanbul)")
    a = ap.parse_args()
    since = datetime.strptime(a.since, "%Y-%m-%d").replace(tzinfo=TZ).timestamp() if a.since else 0.0
    t = sqlite3.connect(f"file:{a.tape_db}?mode=ro", uri=True)
    if not t.execute("SELECT 1 FROM sqlite_master WHERE name='engine_shadow'").fetchone():
        print("engine_shadow tablosu yok — kaydedici henüz yeni sürümle çalışmadı.")
        return
    rows = t.execute("SELECT variant, window_start, signal_ts, side, signal_price, fill_price, size FROM engine_shadow "
                     "WHERE signal_ts >= ? ORDER BY signal_ts", (since,)).fetchall()
    official = dict(t.execute("SELECT window_start, official_resolution FROM windows WHERE official_resolution IN ('UP','DOWN')").fetchall())
    t.close()
    own = {}
    if os.path.exists(a.bot_db):
        b = sqlite3.connect(f"file:{a.bot_db}?mode=ro", uri=True)
        for ct, res, off in b.execute("SELECT close_time, resolution, official_resolution FROM market_sessions"):
            if ct and (off or res) in ("UP", "DOWN"):
                own[int(round(ct / 300.0) * 300) - 300] = off or res
        b.close()
    inst, real, pend = defaultdict(list), defaultdict(list), 0
    byday = defaultdict(lambda: defaultdict(float))
    for v, ws, ts, side, sp, fp, size in rows:
        out = official.get(ws) or own.get(ws)
        if out not in ("UP", "DOWN") or sp is None:
            pend += 1
            continue
        won = side == out
        inst[v].append(pnl(size, won, sp))
        if fp is not None:
            real[v].append(pnl(size, won, fp))
        byday[v][datetime.fromtimestamp(ts, TZ).strftime("%m-%d")] += pnl(size, won, fp) if fp is not None else 0.0
    first = datetime.fromtimestamp(rows[0][2], TZ).strftime("%Y-%m-%d %H:%M") if rows else "—"
    print(f"MOTOR GÖLGESİ — {len(rows)} sinyal (ilk: {first}); sonucu belli olmayan {pend}. Pencere başına varyant başına ilk sinyal, 30 hisse, ücret dahil.")
    print(f"{'varyant':<8}{'işlem':>6}{'anlık P&L $':>13}{'/işlem':>8}{'170 ms n':>10}{'170 ms P&L $':>14}{'/işlem':>8}{'t':>7}")
    for v in ("K", "R1", "R2", "R3"):
        i, r = inst[v], real[v]
        if not i:
            print(f"{v:<8}{0:>6}")
            continue
        sd = statistics.stdev(r) if len(r) > 2 else 0.0
        tv = statistics.mean(r) / (sd / math.sqrt(len(r))) if sd > 0 else float("nan")
        print(f"{v:<8}{len(i):>6}{sum(i):>13.2f}{sum(i) / len(i):>8.2f}{len(r):>10}{sum(r):>14.2f}{(sum(r) / len(r) if r else 0):>8.2f}{tv:>7.2f}")
    days = sorted({d for v in byday.values() for d in v})
    if days:
        print("\n170 ms P&L günlük: " + "  ".join(f"{d}" for d in days))
        for v in ("K", "R1", "R2", "R3"):
            print(f"  {v:<4}" + "  ".join(f"{byday[v][d]:+7.0f}" for d in days))


if __name__ == "__main__":
    main()

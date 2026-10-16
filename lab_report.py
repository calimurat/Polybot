"""
LAB REPORT — P&L of the log-only strategy shadow (lab_shadow.py), read-only.

Joins market_tape.db's lab_shadow to the window's official result (windows),
falling back to the bot's own result (bot_trades.db market_sessions).

    python lab_report.py                 # everything recorded
    python lab_report.py --since 2026-10-11
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


def main():
    parser = argparse.ArgumentParser(description="P&L of the lab strategy shadow")
    parser.add_argument("--tape-db", default=os.path.join(HERE, "market_tape.db"))
    parser.add_argument("--bot-db", default=os.path.join(HERE, "bot_trades.db"))
    parser.add_argument("--since", help="YYYY-MM-DD (Istanbul), only trades from this day on")
    args = parser.parse_args()
    since = (datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=TZ).timestamp() if args.since else 0)

    tape = sqlite3.connect(f"file:{args.tape_db}?mode=ro", uri=True)
    if not tape.execute("SELECT 1 FROM sqlite_master WHERE name='lab_shadow'").fetchone():
        print("lab_shadow tablosu yok — market watch henüz yeni sürümle çalışmadı.")
        return
    has_no = "trade_no" in {r[1] for r in tape.execute("PRAGMA table_info(lab_shadow)")}
    rows = tape.execute("SELECT strategy, window_start, signal_ts, side, fill_price, size, fee, "
                        + ("COALESCE(trade_no, 1)" if has_no else "1") + " FROM lab_shadow "
                        "WHERE signal_ts >= ? ORDER BY signal_ts", (since,)).fetchall()
    official = dict(tape.execute("SELECT window_start, official_resolution FROM windows "
                                 "WHERE official_resolution IN ('UP','DOWN')").fetchall())
    tape.close()
    bot = {}
    if os.path.exists(args.bot_db):
        b = sqlite3.connect(f"file:{args.bot_db}?mode=ro", uri=True)
        for ct, res, off in b.execute("SELECT close_time, resolution, official_resolution FROM market_sessions"):
            if ct and (off or res) in ("UP", "DOWN"):
                bot[int(round(ct / 300.0) * 300) - 300] = off or res
        b.close()

    stats = defaultdict(lambda: {"UP": [], "DOWN": []})
    first_only = defaultdict(list)
    pending = 0
    for name, ws, ts, side, price, size, fee, trade_no in rows:
        outcome = official.get(ws) or bot.get(ws)
        if outcome not in ("UP", "DOWN"):
            pending += 1
            continue
        pnl = size * ((1.0 if side == outcome else 0.0) - price) - fee
        stats[name][side].append(pnl)
        if trade_no == 1:
            first_only[name].append(pnl)

    first = datetime.fromtimestamp(rows[0][2], TZ).strftime("%Y-%m-%d %H:%M") if rows else "—"
    print(f"LAB GÖLGESİ — {len(rows)} işlem (ilk: {first}); sonucu henüz belli olmayan {pending}. "
          f"Dolum: sinyalden 170 ms sonra, 30 hisse VWAP, komisyon dahil. Pencere başına en fazla 2 işlem "
          f"(2. işlem yeni sinyalle); '1. işlem' sütunları yalnız pencerenin ilk işlemi.")
    print(f"{'strateji':<40}{'işlem':>6}{'win%':>7}{'P&L $':>10}{'/işlem':>8}{'t':>7}{'UP P&L':>9}{'DOWN P&L':>10}"
          f"{'1. işlem n':>11}{'1. işlem P&L':>13}")

    def t_of(v):
        return statistics.mean(v) / (statistics.stdev(v) / math.sqrt(len(v))) if len(v) > 2 and statistics.stdev(v) > 0 else float("nan")

    for name in sorted(stats, key=lambda k: -sum(stats[k]["UP"] + stats[k]["DOWN"])):
        allp = stats[name]["UP"] + stats[name]["DOWN"]
        n = len(allp)
        print(f"{name:<40}{n:>6}{100 * sum(x > 0 for x in allp) / n:>6.1f}%{sum(allp):>10.2f}{sum(allp) / n:>8.2f}"
              f"{t_of(allp):>7.2f}{sum(stats[name]['UP']):>9.2f}{sum(stats[name]['DOWN']):>10.2f}"
              f"{len(first_only[name]):>11}{sum(first_only[name]):>13.2f}")


if __name__ == "__main__":
    main()

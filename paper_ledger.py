"""
PAPER LEDGER — the lab strategy "stale-book perp 2s ±$15 mid<0.01" kept like a paper bot's trades (added 2026-10-12, user: "tamam a yı yap").

It lives in the market recorder, beside the lab, and never touches the bot, its positions table or K's calibration. Every first
trade of a window that lab_shadow.py emits for the strategy becomes one ledger row, opened with the same fixed lot (30 shares), and
is closed when the window's official resolution arrives: status win/loss, outcome, P&L. Two fills are kept side by side:
  - the INSTANT fill (signal_price: the 30-share VWAP at the signal — what the bot's own paper fill is), and
  - the fill 170 ms later (fill_price, NULL = no valid book at that sample — the more realistic one).
Fee 0.07 p (1-p) per share on whichever price, as everywhere. The ledger is rebuilt from lab_shadow when the recorder starts
(BACKFILL, idempotent), so it also holds the trades since the lab went live and survives a restart.

    python paper_ledger.py                 # totals, per day, running balance, open positions
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta, timezone

STRATEGY = "stale-book perp 2s ±$15 mid<0.01"
FEE_RATE = 0.07

SCHEMA = ("CREATE TABLE IF NOT EXISTS paper_ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, window_start INTEGER, "
          "signal_ts REAL, fill_ts REAL, side TEXT, signal_price REAL, fill_price REAL, size REAL, fee_instant REAL, fee_fill REAL, "
          "status TEXT, outcome TEXT, pnl_instant REAL, pnl_fill REAL, settled_at REAL)")
INDEX = "CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_ledger_trade ON paper_ledger(strategy, window_start)"
INSERT = ("INSERT OR IGNORE INTO paper_ledger (strategy, window_start, signal_ts, fill_ts, side, signal_price, fill_price, size, fee_instant, "
          "fee_fill, status) VALUES (?,?,?,?,?,?,?,?,?,?, 'open')")
# the lab's first trades since it went live, once each (the unique index makes it idempotent)
BACKFILL = ("INSERT OR IGNORE INTO paper_ledger (strategy, window_start, signal_ts, fill_ts, side, signal_price, fill_price, size, "
            "fee_instant, fee_fill, status) SELECT strategy, window_start, signal_ts, fill_ts, side, COALESCE(signal_price, fill_price), "
            "fill_price, size, size * 0.07 * COALESCE(signal_price, fill_price) * (1 - COALESCE(signal_price, fill_price)), fee, 'open' "
            f"FROM lab_shadow WHERE strategy = '{STRATEGY}' AND COALESCE(trade_no, 1) = 1")
# close what the official resolution has reached; two statements, run in this order
SETTLE_OUTCOME = ("UPDATE paper_ledger SET outcome = (SELECT w.official_resolution FROM windows w WHERE w.window_start = paper_ledger.window_start "
                  "AND w.official_resolution IN ('UP', 'DOWN')) WHERE status = 'open' AND outcome IS NULL AND EXISTS "
                  "(SELECT 1 FROM windows w WHERE w.window_start = paper_ledger.window_start AND w.official_resolution IN ('UP', 'DOWN'))")
SETTLE_PNL = ("UPDATE paper_ledger SET status = CASE WHEN side = outcome THEN 'win' ELSE 'loss' END, "
              "pnl_instant = size * ((CASE WHEN side = outcome THEN 1.0 ELSE 0.0 END) - signal_price) - fee_instant, "
              "pnl_fill = CASE WHEN fill_price IS NULL THEN NULL ELSE size * ((CASE WHEN side = outcome THEN 1.0 ELSE 0.0 END) - fill_price) - fee_fill END, "
              "settled_at = strftime('%s', 'now') WHERE status = 'open' AND outcome IS NOT NULL")


def row_from_lab(params):
    """The ledger INSERT parameters for one lab_shadow emit tuple, or None if it is not this strategy's first trade.
    params = (name, ws, signal_ts, fill_ts, side, signal_price, fill_price, size, fee, detail, trade_no)."""
    name, ws, sig_ts, fill_ts, side, sig_px, fill_px, size, fee, _detail, n = params
    if name != STRATEGY or (n or 1) != 1:
        return None
    px = sig_px if sig_px is not None else fill_px
    return (name, ws, sig_ts, fill_ts, side, px, fill_px, size, size * FEE_RATE * px * (1 - px), fee)


def main():
    ap = argparse.ArgumentParser(description="Paper ledger of the stale-book strategy")
    ap.add_argument("--tape-db", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_tape.db"))
    a = ap.parse_args()
    tz = timezone(timedelta(hours=3))
    con = sqlite3.connect(f"file:{a.tape_db}?mode=ro", uri=True)
    if not con.execute("SELECT 1 FROM sqlite_master WHERE name = 'paper_ledger'").fetchone():
        print("paper_ledger tablosu yok — kaydedici henüz yeni sürümle çalışmadı.")
        return
    rows = con.execute("SELECT signal_ts, side, status, pnl_instant, pnl_fill, fill_price FROM paper_ledger ORDER BY signal_ts").fetchall()
    closed = [r for r in rows if r[2] in ("win", "loss")]
    opened = [r for r in rows if r[2] == "open"]
    print(f"PAPER DEFTER — {STRATEGY}\n  {len(rows)} pozisyon: {len(closed)} kapanmış ({sum(r[2] == 'win' for r in closed)} kazanç), {len(opened)} açık (sonuç bekliyor). "
          f"Sabit lot 30 hisse, ücret dahil.")
    if closed:
        inst = sum(r[3] for r in closed)
        fills = [r[4] for r in closed if r[4] is not None]
        print(f"  anlık dolum (botun paper fiyatı): ${inst:+.2f}  (işlem başı ${inst / len(closed):+.2f})")
        print(f"  170 ms dolum (gerçekçi)         : ${sum(fills):+.2f}  ({len(fills)} dolan, işlem başı ${(sum(fills) / len(fills)) if fills else 0:+.2f}; "
              f"{len(closed) - len(fills)} dolmadı)")
        byday, cum = {}, 0.0
        for r in closed:
            d = datetime.fromtimestamp(r[0], tz).strftime("%m-%d")
            b = byday.setdefault(d, [0, 0.0, 0.0])
            b[0] += 1
            b[1] += r[3]
            b[2] += r[4] or 0.0
        print("  gün     işlem   anlık P&L   170 ms P&L   birikimli (170 ms)")
        for d, (n, i, f) in sorted(byday.items()):
            cum += f
            print(f"  {d}  {n:6d}  {i:+10.2f}  {f:+11.2f}  {cum:+12.2f}")


if __name__ == "__main__":
    main()

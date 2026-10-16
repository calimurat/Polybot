"""
EXPORT TRAINING DATA — Phase 3 data pipeline
Joins closed trades (real outcomes) with the signal_features snapshot each
position was opened with, into one flat table ready for model training.

Every position opened by run.py since the signal_features column was added
(see database.py's positions-table migration) carries a JSON snapshot of
the BTC-deviation signal's diagnostics (deviation_pct, required_pct,
threshold_source) plus the Phase 2 diagnostics (OBI at 1/5/10 levels,
trade_flow, TWAP30/60, spread, best_bid/best_ask) as they were AT THE
MOMENT the position was opened. This script does not compute or guess
anything — it only reads back what was actually recorded, joined against
the real win/loss outcome from the trades table.

Usage:
    python3 export_training_data.py [--db bot_trades.db] [--out training_data.csv]

Output: one CSV row per closed trade that has a signal_features snapshot.
Rows without one (e.g. the resolution-VOID fallback in _cleanup(), which
uses close_all_positions(exit_price=0.5) with no resolution/features) are
skipped — they're not decisions to learn from.
"""

import argparse
import csv
import json
import sqlite3
import sys


def export(db_path: str, out_path: str) -> int:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT
                t.id            AS trade_id,
                t.position_id,
                t.entry_time,
                t.exit_time,
                t.entry_price,
                t.exit_price,
                t.size,
                t.pnl,
                t.pnl_percent,
                t.status        AS outcome,       -- win/loss/void
                t.duration_seconds,
                t.fee,                            -- Polymarket taker fee charged at entry
                t.mid_price,                      -- book mid-price at signal time (pre-slippage)
                p.signal_type,                    -- BUY_UP/BUY_DOWN
                p.signal_features
            FROM trades t
            JOIN positions p ON t.position_id = p.id
            ORDER BY t.entry_time ASC
        """).fetchall()
    finally:
        conn.close()

    records = []
    skipped_no_features = 0
    skipped_bad_json = 0

    for row in rows:
        if not row["signal_features"]:
            skipped_no_features += 1
            continue

        try:
            features = json.loads(row["signal_features"])
            if not isinstance(features, dict):
                raise ValueError(f"signal_features is not a JSON object: {type(features).__name__}")
        except (json.JSONDecodeError, TypeError, ValueError):
            skipped_bad_json += 1
            continue

        fee = row["fee"]          # None for trades recorded before this column existed
        mid_price = row["mid_price"]
        size = row["size"]

        record = {
            "trade_id": row["trade_id"],
            "position_id": row["position_id"],
            "entry_time": row["entry_time"],
            "exit_time": row["exit_time"],
            "signal_type": row["signal_type"],
            "entry_price": row["entry_price"],
            "exit_price": row["exit_price"],
            "size": size,
            "pnl": row["pnl"],                 # net of fee (slippage already priced into entry_price)
            "pnl_percent": row["pnl_percent"],
            "fee": fee,
            "mid_price": mid_price,
            # gross_pnl/slippage_cost are only reconstructible when fee/mid_price were
            # actually recorded (both added in the same change) — leave them unset
            # rather than silently computing from a missing value as if it were 0.
            "gross_pnl": (row["pnl"] + fee) if fee is not None else None,
            "slippage_cost": ((row["entry_price"] - mid_price) * size) if mid_price is not None else None,
            "duration_seconds": row["duration_seconds"],
            "outcome": row["outcome"],
            "label_win": 1 if row["outcome"] == "win" else (0 if row["outcome"] == "loss" else None),
        }
        # Flatten the feature snapshot as its own columns (feat_ prefix
        # keeps them visually distinct from the trade/outcome columns
        # above in the resulting CSV).
        for key, value in features.items():
            record[f"feat_{key}"] = value

        records.append(record)

    if not records:
        print(f"No usable rows found (skipped {skipped_no_features} with no signal_features, "
              f"{skipped_bad_json} with unparseable JSON). Nothing written.")
        return 0

    # Union of all keys across records, since different signal versions
    # over time could add/remove feature columns — don't assume they're
    # all identical.
    fieldnames = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    wins = sum(1 for r in records if r["outcome"] == "win")
    losses = sum(1 for r in records if r["outcome"] == "loss")
    voids = sum(1 for r in records if r["outcome"] not in ("win", "loss"))

    print(f"Wrote {len(records)} rows to {out_path}")
    print(f"  win={wins} loss={losses} void/other={voids}")
    if skipped_no_features or skipped_bad_json:
        print(f"  skipped: {skipped_no_features} with no signal_features, {skipped_bad_json} unparseable")
    if len(records) < 200:
        print(
            f"\nNOTE: only {len(records)} usable rows — too few to train/validate a real "
            f"model on. See the conversation's Phase 3 discussion: a few hundred at minimum "
            f"is a reasonable starting point, and even then, validate with a time-based "
            f"(walk-forward) split, never a random one — this is time-series data."
        )

    return len(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="bot_trades.db", help="Path to the SQLite database")
    parser.add_argument("--out", default="training_data.csv", help="Output CSV path")
    args = parser.parse_args()

    try:
        export(args.db, args.out)
    except Exception as e:
        print(f"Export failed: {e}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)

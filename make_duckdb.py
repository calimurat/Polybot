"""
DUCKDB VIEW — one file, polybot.duckdb, in which every table of bot_trades.db
and market_tape.db can be queried, without a third copy of the data (added
2026-10-05, user: "bu sistemi bir de DuckDB'ye kaydederken yapabilir misin").

Each table is a VIEW over the Parquet copy that parquet_sync.py keeps up to
date, so the file itself is a few hundred KB and is current whenever the
Parquet is: new day files, 5-minute parts and rewritten blocks are picked up
by the glob on every query. Copying the data in instead would have cost
~80 MB a day for nothing new.

Names: every table is a view in a schema named after its database —
bot_trades.trades, market_tape.windows — and also, when no other database
has a table of that name, under its plain name (trades, tape, ...). A name
two databases share gets only the qualified views, and a warning.

No de-duplication in the views (until 2026-10-05 they ran a window function
over every row on every query): parquet_sync.py rewrites a re-exported chunk
under the same file name, holds a lock so two syncs never export at once,
and drops repeated rowids when it compacts a day.

Run once per machine (the views hold absolute paths), and again after a new
table appears in SQLite:
  python make_duckdb.py
Then, e.g.:
  import duckdb; con = duckdb.connect("polybot.duckdb", read_only=True)
  con.sql("SELECT direction, count(*), avg(fill_price) FROM perp_shadow GROUP BY 1")
"""

import argparse
import glob
import os
from typing import Optional

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _source(db_dir: str, table: str) -> Optional[str]:
    """The read_parquet() call for one table, by the layout parquet_sync.py
    wrote it in, or None if it has no files yet."""
    tdir = os.path.join(db_dir, table)
    if glob.glob(os.path.join(tdir, "block-*.parquet")):
        return f"read_parquet('{os.path.join(tdir, 'block-*.parquet')}', union_by_name = true)"
    if glob.glob(os.path.join(tdir, "date=*", "*.parquet")):
        return (f"read_parquet('{os.path.join(tdir, 'date=*', '*.parquet')}', "
                f"hive_partitioning = true, union_by_name = true)")
    legacy = os.path.join(db_dir, f"{table}.parquet")
    if os.path.exists(legacy):          # whole-table file of the layout before 2026-10-05
        return f"read_parquet('{legacy}')"
    return None


def build(parquet_dir: str, db_path: str) -> list:
    parquet_dir = os.path.abspath(parquet_dir)
    con = duckdb.connect(db_path)
    made, owners = [], {}
    try:
        for db_dir in sorted(d for d in glob.glob(os.path.join(parquet_dir, "*")) if os.path.isdir(d)):
            db = os.path.basename(db_dir)
            tables = {os.path.splitext(os.path.basename(f))[0] for f in glob.glob(os.path.join(db_dir, "*.parquet"))}
            tables |= {os.path.basename(d) for d in glob.glob(os.path.join(db_dir, "*")) if os.path.isdir(d)}
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(db)}")
            for table in sorted(tables):
                src = _source(db_dir, table)
                if src is None:
                    continue
                con.execute(f"CREATE OR REPLACE VIEW {_q(db)}.{_q(table)} AS SELECT * FROM {src}")
                owners.setdefault(table, []).append(db)
                made.append(f"{db}.{table}")
        for table, dbs in sorted(owners.items()):
            if len(dbs) == 1:
                con.execute(f"CREATE OR REPLACE VIEW main.{_q(table)} AS SELECT * FROM {_q(dbs[0])}.{_q(table)}")
            else:
                con.execute(f"DROP VIEW IF EXISTS main.{_q(table)}")
                print(f"warning: table '{table}' exists in {', '.join(dbs)} — use "
                      + " / ".join(f"{d}.{table}" for d in dbs))
    finally:
        con.close()
    return made


def main():
    parser = argparse.ArgumentParser(description="DuckDB views over the Parquet copy")
    parser.add_argument("--parquet", default=os.path.join(HERE, "parquet"))
    parser.add_argument("--db", default=os.path.join(HERE, "polybot.duckdb"))
    args = parser.parse_args()
    made = build(args.parquet, args.db)
    print(f"{args.db}: {len(made)} views — {', '.join(made)}")


if __name__ == "__main__":
    main()

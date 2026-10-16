"""
Stable row keys for the append-only tables (tape, book_ticks) — added
2026-10-05.

parquet_sync.py exports these tables by rowid and deletes what it has
exported. A table without an INTEGER PRIMARY KEY has implicit rowids, which
SQLite may reuse (the next row gets max(rowid)+1, so deleting the newest row
hands its number out again) and which VACUUM renumbers. Either would make
rows look "already exported" when they are not. An `id INTEGER PRIMARY KEY
AUTOINCREMENT` column is the rowid itself, never reused, and kept by VACUUM.

ensure_autoincrement_key() converts an existing table once, keeping every
row's rowid as its id, so nothing exported before changes number.
"""

import logging
import sqlite3

logger = logging.getLogger("SqliteKeys")


def ensure_autoincrement_key(conn: sqlite3.Connection, table: str, key: str = "id") -> bool:
    """Give `table` an `id INTEGER PRIMARY KEY AUTOINCREMENT` first column if
    it has no primary key yet. Returns True if the table was rebuilt. Indexes
    on the table are recreated; the rebuild is one transaction."""
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if row is None or "AUTOINCREMENT" in (row[0] or "").upper():
        return False
    info = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
    if any(col[5] for col in info):        # already has a primary key of its own
        return False
    names = [col[1] for col in info]
    coldefs = ", ".join(f'"{col[1]}" {col[2]}'.rstrip() for col in info)
    collist = ", ".join(f'"{n}"' for n in names)
    extras = [r[0] for r in conn.execute(
        "SELECT sql FROM sqlite_master WHERE tbl_name=? AND type IN ('index', 'trigger') AND sql IS NOT NULL",
        (table,))]
    count = conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
    old_isolation = conn.isolation_level
    conn.isolation_level = None            # explicit transaction below
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(f'CREATE TABLE "{table}__keyed" ("{key}" INTEGER PRIMARY KEY AUTOINCREMENT, {coldefs})')
        conn.execute(f'INSERT INTO "{table}__keyed" ("{key}", {collist}) '
                     f'SELECT rowid, {collist} FROM "{table}" ORDER BY rowid')
        conn.execute(f'DROP TABLE "{table}"')
        conn.execute(f'ALTER TABLE "{table}__keyed" RENAME TO "{table}"')
        for sql in extras:
            conn.execute(sql)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.isolation_level = old_isolation
    logger.info(f"[DB] {table}: added {key} INTEGER PRIMARY KEY AUTOINCREMENT ({count:,} rows kept with their rowids)")
    return True

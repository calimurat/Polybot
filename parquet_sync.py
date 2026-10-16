"""
PARQUET SYNC — a Parquet copy of bot_trades.db and market_tape.db, kept up to
date alongside them (added 2026-10-05, user: "aynı anda parquet'e de kaydetsin").

SQLite stays the live store: the bot and the recorder write a row at a time,
which Parquet (write-once files) cannot do. Every SYNC_INTERVAL_S this copies
what is new into parquet/, zstd-compressed — measured 2026-10-04: 619 MB of
SQLite came to ~100 MB of Parquet (tape 6.4x, book_ticks 4.7x smaller).

Layout under parquet/:
  <db>/<table>/date=YYYY-MM-DD/part-<first>-<last>.parquet
        the two big append-only tables (INCREMENTAL: tape, book_ticks), new
        rows by rowid, kept in an `_rowid` column; <first>/<last> are that
        chunk's rowids
  <db>/<table>/date=YYYY-MM-DD/data.parquet
        a finished day's parts merged into one file (compact_finished_days)
  <db>/<table>/block-<n>.parquet
        every other table, in blocks of BLOCK_ROWS consecutive rowids (block
        n = rowids n*BLOCK_ROWS ...). Triggers on the table mark the blocks a
        write touched in _parquet_dirty, and a sync rewrites only those — so
        a result filled in hours later is copied, but old blocks are never
        re-read (until 2026-10-05 these tables were rewritten whole every
        5 minutes, a cost that grew with their history)
  _state.json   last exported rowid per append-only table
  .sync.lock    one sync at a time, across processes (the recorder's thread
                and a hand-run `python parquet_sync.py`)

Dates are local calendar days of the row's `ts`. Columns a table gains later
(ALTER TABLE) are missing from older files; read_table() fills them with
nulls. A chunk exported twice (crash between writing it and saving the
state) is rewritten under the same file name, so it cannot appear twice.

SQLite retention (2026-10-05, user: "3 gün sonra db dosyalarını boşaltarak
devam etsin"): with retention_days set, each sync deletes from tape and
book_ticks the rows older than that whose rowid is at or below the exported
one — only rows Parquet already holds. Those tables carry an
`id INTEGER PRIMARY KEY AUTOINCREMENT` (sqlite_keys.py), so rowids are never
reused or renumbered; the id itself is not exported (it is `_rowid`). Small
tables stay whole in SQLite: the bot reads them and they grow < 1 MB a day.
Deleted space is reused, so the files stop growing instead of shrinking.

Runs as a thread in market_recorder.py (final sync on shutdown), and by hand:
  python parquet_sync.py             sync now (the first run exports everything)
  python parquet_sync.py --compact   also merge finished days
"""

import argparse
import fcntl
import glob
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

logger = logging.getLogger("ParquetSync")

HERE = os.path.dirname(os.path.abspath(__file__))
SYNC_INTERVAL_S = 300
CHUNK_ROWS = 20000            # rows per read; small enough to keep the GIL free for the sampler
COMPRESSION = "zstd"
COMPACT_ROW_GROUP = 200_000   # rows per row group in a finished day's data.parquet
# Big append-only tables, by table name -> their timestamp column.
INCREMENTAL = {
    "tape": "ts",            # market_tape.db
    "book_ticks": "ts",      # bot_trades.db
}
# Indexed column per append-only table, used to find rows past retention
# (window_start / window_end are both unix seconds).
RETENTION_COLUMN = {"tape": "window_start", "book_ticks": "window_end"}
DELETE_CHUNK = 5000           # rows per delete transaction: keeps each lock to milliseconds
BLOCK_ROWS = 1000             # about this many rows per block file for every other table
DIRTY_TABLE = "_parquet_dirty"   # (table, block) a write touched since its last copy
WIDTH_TABLE = "_parquet_blocks"  # table -> block width in rowids, fixed when its triggers are installed
LOCK_FILE = ".sync.lock"
_TYPES = {"REAL": pa.float64(), "INTEGER": pa.int64(), "TEXT": pa.string()}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _columns(conn, table: str, skip_rowid_alias: bool) -> List[tuple]:
    """(name, arrow type) of the columns to export, in table order. With
    skip_rowid_alias, a single INTEGER PRIMARY KEY column — the rowid itself,
    already exported as _rowid — is left out."""
    info = conn.execute(f"PRAGMA table_info({_q(table)})").fetchall()
    pks = [col for col in info if col[5]]
    alias = (pks[0][1] if skip_rowid_alias and len(pks) == 1
             and (pks[0][2] or "").upper() == "INTEGER" else None)
    return [(col[1], _TYPES.get((col[2] or "TEXT").upper(), pa.string()))
            for col in info if col[1] != alias]


def _schema(cols: List[tuple]) -> pa.Schema:
    return pa.schema([pa.field("_rowid", pa.int64())] + [pa.field(n, t) for n, t in cols])


def _select(table: str, cols: List[tuple]) -> str:
    return f"SELECT rowid, {', '.join(_q(n) for n, _ in cols)} FROM {_q(table)}"


def _to_table(rows, schema: pa.Schema, bad: Dict[str, int]) -> pa.Table:
    """Rows (tuples in schema order) -> Arrow table. A value that does not fit
    its column's declared type becomes null and is counted in `bad`."""
    arrays = []
    for i, field in enumerate(schema):
        conv = str if field.type == pa.string() else int if field.type == pa.int64() else float
        out = []
        for row in rows:
            v = row[i]
            if v is None:
                out.append(None)
                continue
            try:
                out.append(conv(v))
            except (TypeError, ValueError):
                out.append(None)
                bad[field.name] = bad.get(field.name, 0) + 1
        arrays.append(pa.array(out, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _write_atomic(table: pa.Table, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    pq.write_table(table, tmp, compression=COMPRESSION)
    os.replace(tmp, path)


def _conform(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Give `table` exactly `schema`'s columns, adding missing ones as nulls."""
    cols = [table.column(f.name).cast(f.type) if f.name in table.column_names
            else pa.nulls(table.num_rows, type=f.type) for f in schema]
    return pa.Table.from_arrays(cols, schema=schema)


class ParquetSync:
    def __init__(self, databases: List[str], out_dir: str, retention_days: Optional[float] = None):
        self.retention_days = retention_days
        self.rows_deleted = 0
        self.databases = [os.path.abspath(p) for p in databases]
        self.out_dir = os.path.abspath(out_dir)
        self.state_path = os.path.join(self.out_dir, "_state.json")
        self.lock_path = os.path.join(self.out_dir, LOCK_FILE)
        self._stop = threading.Event()
        self._lock = threading.Lock()          # one sync at a time in this process
        self.thread: Optional[threading.Thread] = None
        self.last_sync: Optional[float] = None
        self.rows_exported = 0
        self.blocks_written = 0
        os.makedirs(self.out_dir, exist_ok=True)

    # ---- locking ---------------------------------------------------------

    @contextmanager
    def _exclusive(self, timeout: Optional[float], final: bool):
        """Hold this process's lock and parquet/.sync.lock. Yields False if
        either could not be had within `timeout` seconds (None: wait, but
        give up when stop() is called — unless this is the final sync)."""
        got_thread = self._lock.acquire(timeout=-1 if timeout is None else timeout)
        handle, got_file = None, False
        try:
            if got_thread:
                handle = open(self.lock_path, "a+")
                deadline = None if timeout is None else time.time() + timeout
                while True:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        got_file = True
                        break
                    except BlockingIOError:
                        if deadline is not None and time.time() >= deadline:
                            break
                        if self._stop.is_set() and not final:
                            break
                        time.sleep(0.2)
            yield got_file
        finally:
            if handle is not None:
                if got_file:
                    fcntl.flock(handle, fcntl.LOCK_UN)
                handle.close()
            if got_thread:
                self._lock.release()

    def _stopping(self, final: bool) -> bool:
        return self._stop.is_set() and not final

    # ---- state -----------------------------------------------------------

    def _load_state(self) -> Dict[str, int]:
        try:
            with open(self.state_path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_state(self, state: Dict[str, int]) -> None:
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=1, sort_keys=True)
        os.replace(tmp, self.state_path)

    # ---- one sync --------------------------------------------------------

    def sync(self, compact: bool = True, lock_timeout: Optional[float] = None,
             final: bool = False) -> bool:
        """One pass: export, then (optionally) compact finished days, then
        apply retention. Returns False if another sync held the lock."""
        with self._exclusive(lock_timeout, final) as got:
            if not got:
                logger.warning("[PARQUET] another sync holds parquet/.sync.lock — skipped this pass")
                return False
            # Under the lock no other sync can be mid-write, so any .tmp is
            # left over from a run that was killed.
            for tmp in glob.glob(os.path.join(self.out_dir, "**", "*.tmp"), recursive=True):
                os.remove(tmp)
            state = self._load_state()
            for db in self.databases:
                if not os.path.exists(db):
                    continue
                name = os.path.basename(db)
                conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
                try:
                    tables = [t for (t,) in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' "
                        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\\_parquet%' ESCAPE '\\'")]
                finally:
                    conn.close()
                for table in tables:
                    if self._stopping(final):
                        return True
                    if table in INCREMENTAL:
                        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
                        try:
                            self._sync_incremental(conn, name, table, INCREMENTAL[table], state, final)
                        finally:
                            conn.close()
                    else:
                        self._sync_blocks(db, name, table, final)
            if compact:
                self.compact_finished_days(final)
            if self.retention_days and not final:
                self.prune_sqlite(state)
            self.last_sync = time.time()
            return True

    def _consistent(self, conn, db_name: str, table: str, exported: int) -> bool:
        """False if the table's rowids are below what was already exported —
        the .db was replaced or recreated (e.g. deleted by hand) while
        parquet/_state.json was kept. Then export and pruning of that table
        stop, loudly, rather than skip or delete rows that are not in Parquet."""
        top = conn.execute(f"SELECT max(rowid) FROM {_q(table)}").fetchone()[0]
        if top is not None and top < exported:
            logger.error(f"[PARQUET] {db_name}/{table}: SQLite's newest rowid {top} is below the "
                         f"{exported} already exported — the database was replaced. Export and "
                         f"cleanup of this table are paused; fix parquet/_state.json by hand.")
            return False
        return True

    def _sync_incremental(self, conn, db_name: str, table: str, ts_col: str, state, final: bool) -> None:
        key = f"{db_name}/{table}"
        last = int(state.get(key, 0))
        if last and not self._consistent(conn, db_name, table, last):
            return
        cols = _columns(conn, table, skip_rowid_alias=True)
        schema = _schema(cols)
        ts_i = schema.get_field_index(ts_col)
        query = _select(table, cols) + " WHERE rowid > ? ORDER BY rowid LIMIT ?"
        bad: Dict[str, int] = {}
        while not self._stopping(final):
            rows = conn.execute(query, (last, CHUNK_ROWS)).fetchall()
            if not rows:
                break
            by_day: Dict[str, list] = {}
            for row in rows:
                ts = row[ts_i]
                day = datetime.fromtimestamp(ts).strftime("%Y-%m-%d") if ts else "unknown"
                by_day.setdefault(day, []).append(row)
            for day, part in by_day.items():
                path = os.path.join(self.out_dir, db_name.replace(".db", ""), table, f"date={day}",
                                    f"part-{part[0][0]:012d}-{part[-1][0]:012d}.parquet")
                _write_atomic(_to_table(part, schema, bad), path)
            last = rows[-1][0]
            state[key] = last
            self._save_state(state)
            self.rows_exported += len(rows)
            time.sleep(0.01)                   # let the recorder's sampler run
        if bad:
            logger.warning(f"[PARQUET] {key}: values not matching the column type stored as null: {bad}")

    # ---- block tables ----------------------------------------------------

    def _ensure_tracking(self, rw, table: str) -> int:
        """Install the triggers that mark the changed blocks of `table`, and
        return its block width in rowids. The width is chosen once — about
        BLOCK_ROWS rows per block: rowids 1, 2, 3 ... give 1000, the windows
        table (rowid = window start, 300 s apart) gives 300,000 — and kept in
        _parquet_blocks, since the triggers compute block = rowid / width.
        The first time, every existing block is marked, so it is copied once."""
        rw.execute(f"CREATE TABLE IF NOT EXISTS {DIRTY_TABLE} (tbl TEXT NOT NULL, block INTEGER NOT NULL, "
                   f"PRIMARY KEY (tbl, block)) WITHOUT ROWID")
        rw.execute(f"CREATE TABLE IF NOT EXISTS {WIDTH_TABLE} (tbl TEXT PRIMARY KEY, width INTEGER NOT NULL)")
        row = rw.execute(f"SELECT width FROM {WIDTH_TABLE} WHERE tbl=?", (table,)).fetchone()
        names = {f"_pqd_{table}_{k}" for k in ("ins", "upd", "del")}
        have = {r[0] for r in rw.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
                                         (table,))}
        if row and names <= have:
            return row[0]
        if row:
            width = row[0]
        else:
            n, lo, hi = rw.execute(f"SELECT count(*), min(rowid), max(rowid) FROM {_q(table)}").fetchone()
            gap = max(1, round((hi - lo) / (n - 1))) if n and n > 1 else 1
            width = BLOCK_ROWS * gap
            rw.execute(f"INSERT INTO {WIDTH_TABLE} VALUES (?, ?)", (table, width))
        lit = "'" + table.replace("'", "''") + "'"
        mark = lambda ref: f"INSERT OR IGNORE INTO {DIRTY_TABLE} VALUES ({lit}, {ref}.rowid / {width});"
        rw.execute(f"CREATE TRIGGER IF NOT EXISTS {_q('_pqd_' + table + '_ins')} AFTER INSERT ON {_q(table)} "
                   f"BEGIN {mark('NEW')} END")
        rw.execute(f"CREATE TRIGGER IF NOT EXISTS {_q('_pqd_' + table + '_upd')} AFTER UPDATE ON {_q(table)} "
                   f"BEGIN {mark('NEW')} {mark('OLD')} END")
        rw.execute(f"CREATE TRIGGER IF NOT EXISTS {_q('_pqd_' + table + '_del')} AFTER DELETE ON {_q(table)} "
                   f"BEGIN {mark('OLD')} END")
        rw.execute(f"INSERT OR IGNORE INTO {DIRTY_TABLE} SELECT DISTINCT {lit}, rowid / {width} "
                   f"FROM {_q(table)}")
        rw.commit()
        return width

    def _sync_blocks(self, db: str, db_name: str, table: str, final: bool) -> None:
        tdir = os.path.join(self.out_dir, db_name.replace(".db", ""), table)
        legacy = os.path.join(self.out_dir, db_name.replace(".db", ""), f"{table}.parquet")
        block_file = lambda b: os.path.join(tdir, f"block-{b:08d}.parquet")
        rw = sqlite3.connect(db, timeout=30)
        try:
            width = self._ensure_tracking(rw, table)
            dirty = {b for (b,) in rw.execute(f"SELECT block FROM {DIRTY_TABLE} WHERE tbl=?", (table,))}
            # Reconcile with the files too (cheap: these tables are small): a
            # block whose file is missing (folder deleted, a copy that never
            # finished) is copied; a file whose rows are all gone is removed.
            present = {b for (b,) in rw.execute(f"SELECT DISTINCT rowid / {width} FROM {_q(table)}")}
            on_disk = {int(os.path.basename(f)[6:14]) for f in glob.glob(os.path.join(tdir, "block-*.parquet"))}
            todo = sorted(dirty | (present - on_disk))
            for b in sorted(on_disk - present - set(todo)):
                if present or b != 0:
                    os.remove(block_file(b))
            if not present and not os.path.exists(block_file(0)):
                todo = sorted(set(todo) | {0})  # empty table: a schema-only file, so readers see it
            # Claim the blocks before reading them: a write that lands after
            # this marks its block again, and the next sync copies it.
            rw.executemany(f"DELETE FROM {DIRTY_TABLE} WHERE tbl=? AND block=?", [(table, b) for b in todo])
            rw.commit()
            done = []
            try:
                cols = _columns(rw, table, skip_rowid_alias=False)
                schema = _schema(cols)
                query = _select(table, cols) + " WHERE rowid >= ? AND rowid < ? ORDER BY rowid"
                bad: Dict[str, int] = {}
                for b in todo:
                    if self._stopping(final):
                        break
                    rows = rw.execute(query, (b * width, (b + 1) * width)).fetchall()
                    path = block_file(b)
                    if rows or not present:
                        _write_atomic(_to_table(rows, schema, bad), path)
                    elif os.path.exists(path):
                        os.remove(path)        # every row of this block was deleted
                    done.append(b)
                    self.blocks_written += 1
                if bad:
                    logger.warning(f"[PARQUET] {db_name}/{table}: values not matching the column type stored as null: {bad}")
            finally:
                left = [b for b in todo if b not in done]
                if left:                       # not copied (stopped or failed): mark them again
                    rw.executemany(f"INSERT OR IGNORE INTO {DIRTY_TABLE} VALUES (?, ?)", [(table, b) for b in left])
                    rw.commit()
            if not left and os.path.exists(legacy):
                os.remove(legacy)              # the whole-table file of the layout before 2026-10-05
        finally:
            rw.close()

    # ---- retention -------------------------------------------------------

    def prune_sqlite(self, state: Dict[str, int]) -> None:
        """Delete rows older than retention_days from the append-only tables,
        never beyond what _state.json says is already in Parquet."""
        cutoff = time.time() - self.retention_days * 86400
        for db in self.databases:
            if not os.path.exists(db):
                continue
            name = os.path.basename(db)
            conn = sqlite3.connect(db, timeout=30)
            try:
                tables = {t for (t,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                for table, col in RETENTION_COLUMN.items():
                    exported = int(state.get(f"{name}/{table}", 0))
                    if table not in tables or not exported or not self._consistent(conn, name, table, exported):
                        continue
                    deleted = 0
                    while not self._stop.is_set():
                        # The newest row is kept as well: with an AUTOINCREMENT
                        # id (sqlite_keys.py) numbers are never reused anyway,
                        # but a table still waiting for that key would hand the
                        # newest row's number out again if it were deleted.
                        cur = conn.execute(
                            f"DELETE FROM {_q(table)} WHERE rowid IN (SELECT rowid FROM {_q(table)} "
                            f"WHERE {_q(col)} < ? AND rowid <= ? AND rowid < (SELECT max(rowid) FROM {_q(table)}) "
                            f"LIMIT {DELETE_CHUNK})", (cutoff, exported))
                        conn.commit()
                        deleted += cur.rowcount
                        if cur.rowcount < DELETE_CHUNK:
                            break
                        time.sleep(0.05)      # let the bot and recorder write between chunks
                    if deleted:
                        self.rows_deleted += deleted
                        logger.info(f"[PARQUET] {name}/{table}: removed {deleted:,} rows older than "
                                    f"{self.retention_days:g} days from SQLite (kept in Parquet)")
            finally:
                conn.close()

    # ---- compaction ------------------------------------------------------

    def compact_finished_days(self, final: bool = False) -> None:
        """Merge each finished day's parts (plus any earlier data.parquet) into
        one data.parquet, streaming file by file so memory stays small, and
        dropping rowids already written. Stops between files when stop() is
        called, leaving the parts as they were."""
        today = datetime.now().strftime("%Y-%m-%d")
        for table in INCREMENTAL:
            for day_dir in sorted(glob.glob(os.path.join(self.out_dir, "*", table, "date=*"))):
                day = day_dir.rsplit("=", 1)[1]
                parts = sorted(glob.glob(os.path.join(day_dir, "part-*.parquet")))
                if day >= today or not parts:
                    continue
                if self._stopping(final):
                    return
                existing = os.path.join(day_dir, "data.parquet")
                files = ([existing] if os.path.exists(existing) else []) + parts
                schema = pa.unify_schemas([pq.read_schema(f) for f in files])
                tmp = existing + ".tmp"
                writer = pq.ParquetWriter(tmp, schema, compression=COMPRESSION)
                max_written = -1
                pending, pending_rows = [], 0
                aborted = False
                try:
                    for f in files:
                        if self._stopping(final):
                            aborted = True
                            break
                        t = _conform(pq.read_table(f), schema)
                        t = t.filter(pc.greater(t.column("_rowid"), max_written))
                        if t.num_rows:
                            pending.append(t)
                            pending_rows += t.num_rows
                            max_written = pc.max(t.column("_rowid")).as_py()
                        # Large row groups compress ~10% better than the
                        # 20k-row parts; this size keeps memory moderate.
                        if pending_rows >= COMPACT_ROW_GROUP:
                            writer.write_table(pa.concat_tables(pending), row_group_size=COMPACT_ROW_GROUP)
                            pending, pending_rows = [], 0
                    if pending and not aborted:
                        writer.write_table(pa.concat_tables(pending), row_group_size=COMPACT_ROW_GROUP)
                finally:
                    writer.close()
                if aborted:
                    os.remove(tmp)
                    return
                os.replace(tmp, existing)
                for f in parts:
                    os.remove(f)
                logger.info(f"[PARQUET] compacted {table} {day}: {len(files)} files → data.parquet")

    # ---- background thread -----------------------------------------------

    def start(self, interval_s: float = SYNC_INTERVAL_S):
        def loop():
            while not self._stop.is_set():
                try:
                    self.sync()
                except Exception as e:
                    logger.warning(f"[PARQUET] sync failed: {e}")
                self._stop.wait(interval_s)
        self.thread = threading.Thread(target=loop, name="ParquetSync", daemon=True)
        self.thread.start()
        logger.info(f"[PARQUET] syncing {', '.join(os.path.basename(d) for d in self.databases)} "
                    f"to {self.out_dir} every {interval_s:.0f}s")
        return self

    def stop(self, final_sync: bool = True, timeout: float = 8.0, lock_timeout: float = 5.0) -> None:
        """Stop the loop; then one last export of what is new (no compaction,
        no cleanup), so Parquet ends where SQLite does. Bounded: at most
        `timeout` for the running pass to stop and `lock_timeout` to get the
        lock — run.py and Docker give the recorder 20 / 40 s to exit."""
        self._stop.set()
        if self.thread:
            self.thread.join(timeout=timeout)
        if final_sync:
            try:
                self.sync(compact=False, lock_timeout=lock_timeout, final=True)
            except Exception as e:
                logger.warning(f"[PARQUET] final sync failed: {e}")


def read_table(db: str, table: str, out_dir: str = os.path.join(HERE, "parquet"),
               dates: Optional[List[str]] = None, columns: Optional[List[str]] = None) -> pa.Table:
    """Read one table back. db: "bot_trades" or "market_tape". For day-
    partitioned tables, `dates` (["2026-10-04", ...]) limits which days are
    read. Files from before a column existed get it as nulls; rows are in
    rowid order (`_rowid`), each once."""
    legacy = os.path.join(out_dir, db, f"{table}.parquet")
    blocks = sorted(glob.glob(os.path.join(out_dir, db, table, "block-*.parquet")))
    if blocks:
        files = blocks
    elif os.path.exists(legacy):
        return pq.read_table(legacy, columns=columns)
    else:
        files = []
        for day_dir in sorted(glob.glob(os.path.join(out_dir, db, table, "date=*"))):
            if dates is None or day_dir.rsplit("=", 1)[1] in dates:
                files += sorted(glob.glob(os.path.join(day_dir, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"no Parquet data for {db}/{table}")
    schema = pa.unify_schemas([pq.read_schema(f) for f in files])
    if columns:
        schema = pa.schema([schema.field(c) for c in dict.fromkeys(["_rowid"] + list(columns))])
    t = pa.concat_tables([_conform(pq.read_table(f, columns=[c for c in schema.names
                                                            if c in pq.read_schema(f).names]), schema)
                          for f in files])
    t = t.sort_by("_rowid")
    rowids = t.column("_rowid")
    if len(rowids) > 1:
        keep = pc.not_equal(rowids.slice(1), rowids.slice(0, len(rowids) - 1))
        t = t.filter(pa.concat_arrays([pa.array([True]), keep.combine_chunks()]))
    return t


def main():
    parser = argparse.ArgumentParser(description="Parquet copy of the bot's SQLite databases")
    parser.add_argument("--out", default=os.path.join(HERE, "parquet"))
    parser.add_argument("--compact", action="store_true", help="also merge finished days")
    parser.add_argument("--retention-days", type=float, default=None,
                        help="also delete rows older than this from SQLite's tape/book_ticks (only rows already in Parquet)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    s = ParquetSync([os.path.join(HERE, "bot_trades.db"), os.path.join(HERE, "market_tape.db")], args.out,
                    retention_days=args.retention_days)
    t0 = time.time()
    s.sync(compact=args.compact)
    size = sum(os.path.getsize(f) for f in glob.glob(os.path.join(args.out, "**", "*.parquet"), recursive=True))
    print(f"{s.rows_exported:,} new rows exported, {s.blocks_written} blocks written, "
          f"{s.rows_deleted:,} old rows removed from SQLite in {time.time() - t0:.0f}s; "
          f"parquet/ is {size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()

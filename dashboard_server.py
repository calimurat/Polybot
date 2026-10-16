"""
DASHBOARD SERVER — read-only local web UI for bot_trades.db + bot.log

Serves dashboard.html plus a small JSON API the page polls. Stdlib only
(no new dependency): http.server + sqlite3. Never writes to the bot's
database or log — this is purely a viewer.
"""

import json
import logging
import os
import re
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Dashboard")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bot_trades.db")
LOG_PATH = os.path.join(BASE_DIR, "bot.log")
HTML_PATH = os.path.join(BASE_DIR, "dashboard.html")
PORT = 8765
# Where the dashboard listens. 127.0.0.1 (default) = this machine only. In
# Docker / on the AWS server it is 0.0.0.0, so the server's IP can reach it.
# No password (user's choice, 2026-10-05): on AWS, limit port 8765 in the
# security group to your own IP — that rule is the only thing keeping the
# page private. It is read-only: nobody can trade or change settings from it.
HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1")
LOOPBACK = HOST in ("127.0.0.1", "localhost", "::1")


def make_server():
    return ThreadingHTTPServer((HOST, PORT), Handler)

# Log lines this bot actually emits for these events — see polymarket_production_bot.py /
# run.py. Parsed read-only from the tail of bot.log to surface "what's happening right
# NOW" — the in-flight cycle and market, which by definition has no row yet.
#
# (The old note here said run.py never populates market_sessions. It does:
# one row per 5-minute cycle, traded or not, written in _end_market().
# Those completed cycles are queryable from the DB; only the current,
# still-running one has to come from the log tail.)
CYCLE_RE = re.compile(r"\[CYCLE (\d+)\] (?:Loading next|Complete)")
MARKET_RE = re.compile(r"\[MARKET\] (?:Loaded|Using pre-warmed market): (.+)")
TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+")

# bot.log grows ~15 KB a minute (~120 lines) and a cycle / market line is
# written once per 5-minute window, so a short tail regularly holds none of
# them: with 60 KB the page showed "Cycle —" a third of the time (measured on
# 2026-09-30's log; a 400 KB tail never missed). 1 MB spans about an hour at
# that rate. The last values seen are kept for the moments even that comes
# up empty (log rotated, market load failing for a while).
TAIL_BYTES = 1_000_000
_last_seen = {"cycle": None, "market_question": None}


def _row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def get_db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def api_summary() -> dict:
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*)                                   AS total_trades,
                SUM(CASE WHEN status = 'win'  THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status = 'loss' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN status = 'void' THEN 1 ELSE 0 END) AS voids,
                COALESCE(SUM(pnl), 0)                      AS total_pnl,
                COUNT(fee)                                 AS fee_data_count,
                COALESCE(SUM(fee), 0)                      AS total_fee,
                COUNT(mid_price)                           AS slippage_data_count,
                COALESCE(SUM(CASE WHEN mid_price IS NOT NULL
                    THEN (entry_price - mid_price) * size ELSE 0 END), 0) AS total_slippage_cost,
                COALESCE(AVG(pnl), 0)                      AS avg_pnl
            FROM trades
        """).fetchone()
        d = _row_to_dict(row)
        total = d["total_trades"] or 0
        wins = d["wins"] or 0
        d["win_rate"] = (wins / total * 100) if total else 0.0
        # fee/mid_price columns only exist for trades opened after that
        # feature landed — older rows have NULL, not 0. Reporting a SUM
        # over a partial column as if it covered every trade would silently
        # understate the true total, so mark it explicit instead of hiding
        # the gap (see the dashboard's own coverage note in the UI).
        d["fee_coverage_note"] = None if d["fee_data_count"] == total else f"{d['fee_data_count']}/{total} işlemde veri var"
        d["slippage_coverage_note"] = None if d["slippage_data_count"] == total else f"{d['slippage_data_count']}/{total} işlemde veri var"
        d["total_gross_pnl"] = d["total_pnl"] + d["total_fee"]
        return d
    finally:
        conn.close()


def api_trades(limit: int) -> list:
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT
                t.id, t.position_id, t.entry_time, t.exit_time, t.entry_price, t.exit_price,
                t.size, t.pnl, t.pnl_percent, t.status, t.duration_seconds, t.fee, t.mid_price,
                p.signal_type
            FROM trades t
            LEFT JOIN positions p ON t.position_id = p.id
            ORDER BY t.entry_time DESC
            LIMIT ?
        """, (limit,)).fetchall()
        out = []
        for r in rows:
            d = _row_to_dict(r)
            d["gross_pnl"] = (d["pnl"] + d["fee"]) if d["fee"] is not None else None
            d["slippage_cost"] = (
                (d["entry_price"] - d["mid_price"]) * d["size"]
                if d["mid_price"] is not None else None
            )
            out.append(d)
        return out
    finally:
        conn.close()


def api_open_position() -> dict | None:
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT id, order_id, entry_price, entry_time, signal_type, size, fee, mid_price
            FROM positions
            WHERE status = 'open'
            ORDER BY entry_time DESC
            LIMIT 1
        """).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def tail_log_state() -> dict:
    """
    Best-effort read of bot.log's tail for transient state the DB doesn't
    carry (current cycle number, current market's question text) plus a
    freshness check (has anything been logged in the last 30s — a rough
    "is the bot process actually alive and looping" signal, since this
    server doesn't touch the bot's own process).
    """
    state = {"cycle": None, "market_question": None, "last_log_time": None, "seconds_since_log": None}
    try:
        with open(LOG_PATH, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="ignore")
        lines = tail.splitlines()
        for line in reversed(lines):
            if state["cycle"] is None:
                m = CYCLE_RE.search(line)
                if m:
                    state["cycle"] = int(m.group(1))
            if state["market_question"] is None:
                m = MARKET_RE.search(line)
                if m:
                    state["market_question"] = m.group(1).strip()
            if state["cycle"] is not None and state["market_question"] is not None:
                break
        for key in ("cycle", "market_question"):
            if state[key] is None:
                state[key] = _last_seen[key]
            else:
                _last_seen[key] = state[key]
        for line in reversed(lines):
            if line.strip():
                m = TS_RE.match(line)
                if m:
                    ts = time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
                    state["last_log_time"] = m.group(1)
                    state["seconds_since_log"] = max(0, time.time() - ts)
                break
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"tail_log_state failed: {e}")
    return state


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        logger.info("%s - %s" % (self.address_string(), fmt % args))

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        try:
            if path == "/" or path == "/dashboard.html":
                with open(HTML_PATH, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            elif path == "/api/summary":
                self._send_json(api_summary())

            elif path == "/api/trades":
                limit = int(qs.get("limit", ["500"])[0])
                self._send_json(api_trades(limit))

            elif path == "/api/live":
                live = tail_log_state()
                live["open_position"] = api_open_position()
                self._send_json(live)

            else:
                self._send_json({"error": "not found"}, status=404)

        except Exception as e:
            logger.error(f"Request failed for {path}: {e}")
            self._send_json({"error": str(e)}, status=500)


def main():
    if not os.path.exists(DB_PATH):
        logger.warning(f"Database not found at {DB_PATH} — dashboard will show empty data until the bot creates it.")
    server = make_server()
    logger.info(f"Dashboard serving at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

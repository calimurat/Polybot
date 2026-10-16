"""
Application-level WebSocket heartbeats (added 2026-10-02).

docs.polymarket.com/market-data/realtime-data: "The market WebSocket uses an
application-level heartbeat. Send the text frame `PING` every 10 seconds; the
server replies with `PONG`." The bot only ever sent WebSocket protocol pings,
and bot.log had 268 (bot.log.1: 479) "ping/pong timed out" disconnects on
2026-10-02. Earlier investigation put those on the VPN path
(slow-consumer/feed-lag notes); this follows the documented protocol either
way, and the protocol pings stay as they were.

A PONG proves the socket is alive, not that market data is flowing — callers
must not count it as data (PolymarketBot's heartbeat monitor reads
last_ws_message_time as "data arrived").

start_text_ping() is also what ws_feed.Feed uses for the exchanges that want
their own ping text (OKX "ping", Bybit {"op": "ping"}) — one loop, one set of
failure rules (2026-10-05).
"""

import json
import threading
from typing import Callable, Union

PING = "PING"
INTERVAL_S = 10.0


def is_pong(message) -> bool:
    """True for the server's reply to PING (str, or bytes when the client
    skips UTF-8 validation)."""
    return message == "PONG" or message == b"PONG"


def start_text_ping(ws, current_ws: Callable[[], object], keep_running: Callable[[], bool],
                    interval: float = INTERVAL_S, name: str = "WSTextPing",
                    payload: Union[str, dict] = PING,
                    ready: Callable[[], bool] = None) -> threading.Thread:
    """
    Send `payload` (text, or a dict sent as JSON) on `ws` every `interval`
    seconds until `current_ws()` is no longer `ws` (the caller reconnected)
    or `keep_running()` is false. One thread per connection. A send that
    fails — the socket still connecting, or closed and about to be replaced —
    is skipped, never fatal: giving up on the first failure would leave a
    slow-to-open connection without heartbeats for its whole life.
    `ready`, when given, must be true for a ping to go out: Polymarket answers
    a PING on a socket that has no subscription yet with close code 1008
    ("invalid subscription payload").
    """
    text = payload if isinstance(payload, str) else json.dumps(payload)

    def loop():
        stop = threading.Event()
        while not stop.wait(interval):
            if not keep_running() or current_ws() is not ws:
                return
            if ready is not None and not ready():
                continue
            try:
                ws.send(text)
            except Exception:
                continue

    thread = threading.Thread(target=loop, name=name, daemon=True)
    thread.start()
    return thread

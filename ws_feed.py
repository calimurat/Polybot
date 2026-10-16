"""
Reconnecting, stall-watched WebSocket client shared by market_recorder.py and
the bot's perp momentum shadow (perp_shadow.py). Moved out of
market_recorder.py on 2026-10-04 unchanged, so both use the same tested code.
"""

import json
import logging
import random
import threading
import time

import websocket

from ws_heartbeat import start_text_ping

logger = logging.getLogger("WSFeed")

FEED_STALL_S = 30.0           # a feed silent this long is reconnected


class Feed:
    """
    One WebSocket, reconnected with backoff for as long as the recorder runs.
    `urls` rotate on each reconnect (Binance's new futures paths, with the
    legacy path as fallback). A feed silent for `stall_s` is closed and
    reconnected by check_stall(): a connection can stay open and deliver
    nothing (seen 2026-10-02 on the legacy futures aggTrade path).
    """

    def __init__(self, name, urls, handler, subscribe=None, text_ping=None,
                 stall_s=FEED_STALL_S, on_open=None):
        self.name = name
        self.urls = list(urls)
        self.handler = handler            # handler(message_bytes_or_str, receive_ts)
        self.subscribe = subscribe        # dict sent on open, or None
        self.text_ping = text_ping        # (payload, interval_s) for exchanges that want an app-level ping
        self.stall_s = stall_s
        self.on_open_hook = on_open
        self.stop_event = threading.Event()
        self.ws = None
        self.messages = 0
        self.connects = 0
        # Per connection attempt: when it started, when it opened, its last
        # message. All three are reset for every attempt, so a new socket is
        # never judged by the previous one's last message (2026-10-05: that
        # stale value made check_stall() close fresh sockets mid-handshake).
        self.attempt_started = None
        self.last_msg = None
        self.connected_at = None
        self.last_error = None
        self.thread = threading.Thread(target=self._run, name=f"Feed-{name}", daemon=True)

    def start(self):
        self.thread.start()
        return self

    @staticmethod
    def _close(ws):
        # websocket-client waits up to 3 s for the server's close frame by
        # default; nothing here needs a clean close handshake.
        try:
            ws.close(timeout=0.5)
        except Exception:
            pass

    def stop(self):
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            self._close(ws)

    def check_stall(self, now: float):
        """Close the current socket if it has been silent for stall_s: since
        its last message, else since it opened, else since the attempt began
        (a handshake that hangs that long is as dead as a silent socket)."""
        ws = self.ws
        ref = self.last_msg or self.connected_at or self.attempt_started
        if ws is not None and ref is not None and now - ref > self.stall_s:
            logger.warning(f"[{self.name}] no message for {now - ref:.0f}s — reconnecting")
            self.connected_at = None
            # Off the caller's (manager) thread: closing can block briefly.
            threading.Thread(target=self._close, args=(ws,), daemon=True).start()

    def _run(self):
        attempt = 0
        while not self.stop_event.is_set():
            url = self.urls[attempt % len(self.urls)]

            def on_open(ws):
                self.connects += 1
                self.connected_at = time.time()
                self.last_msg = None
                logger.info(f"[{self.name}] connected {url}")
                if self.on_open_hook:
                    self.on_open_hook()
                if self.subscribe is not None:
                    ws.send(json.dumps(self.subscribe))

            def on_message(ws, message):
                rx = time.time()
                self.last_msg = rx
                self.messages += 1
                try:
                    self.handler(message, rx)
                except Exception as e:
                    logger.debug(f"[{self.name}] message error: {e}")

            def on_error(ws, error):
                self.last_error = str(error)[:200]

            def on_close(ws, code, msg):
                if msg and "resolved" in str(msg).lower():
                    # Polymarket closes the market channel once its tokens
                    # resolve; reconnecting would only be closed again.
                    self.stop_event.set()

            self.last_msg = self.connected_at = None
            self.attempt_started = time.time()
            self.ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message,
                                             on_error=on_error, on_close=on_close)
            if self.text_ping:
                payload, interval = self.text_ping
                start_text_ping(self.ws, lambda: self.ws, lambda: not self.stop_event.is_set(),
                                interval, name=f"Ping-{self.name}", payload=payload)
            started = time.time()
            try:
                self.ws.run_forever(ping_interval=20, ping_timeout=10, skip_utf8_validation=True)
            except Exception as e:
                self.last_error = str(e)[:200]
            self.ws = None
            self.connected_at = self.last_msg = self.attempt_started = None
            if self.stop_event.is_set():
                break
            # A connection that held for a while goes straight back to the
            # primary URL; repeated failures back off and rotate URLs.
            held = time.time() - started > 60
            attempt = 0 if held else attempt + 1
            delay = 1.0 if held else min(2 ** min(attempt, 6), 60) * random.uniform(0.8, 1.2)
            if held or attempt <= 3 or attempt % 20 == 0:
                logger.warning(f"[{self.name}] disconnected ({self.last_error or 'closed'}), "
                               f"retry in {delay:.0f}s" + ("" if held else f" (attempt {attempt})"))
            self.stop_event.wait(delay)

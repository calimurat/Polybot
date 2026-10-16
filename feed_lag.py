"""
How far behind real time a feed's messages arrive.

Added 2026-09-28 after measuring the Polymarket market feed arriving
seconds late through the VPN tunnel this bot ran behind: pongs came back
0.1-8.6 s after the ping, and with nothing cutting the connection the
backlog grew to 78 s. Every book the bot built from those messages was
stamped with its local *receive* time, so Orderbook.is_valid's 2 s limit
saw them as fresh — while the Binance price they were compared against
was only 2-5 s behind. A stale book next to a fresher BTC price reads as
edge that the real exchange no longer offers.

Each Polymarket and Binance message carries the server's own timestamp,
so lateness is measurable. But receive_time - server_ts ("raw") mixes
three things:
  * the offset between this machine's clock and the server's (measured
    2026-09-28: the Mac ran ~160 ms behind Polymarket),
  * the path's minimum one-way latency,
  * queueing delay — the part that grows when the pipe backs up.
Only the last one makes data stale. The first two are near-constant, and
the smallest raw lag seen recently is their sum: a message that met no
queue. So: baseline = min(raw) over the last `window_s`, lag = raw -
baseline. The standard min-filter for one-way delay.

Failure modes lean safe. A clock step backwards lowers raw lag and the
baseline follows it at once. A step forwards (or the old minimum ageing
out) raises lag until the window passes — false "stale" verdicts, never
false "fresh" ones.
"""

import threading
from collections import deque
from typing import Optional


class FeedLagEstimator:
    def __init__(self, window_s: float = 600.0, bucket_s: float = 60.0):
        # Per-bucket minima instead of every sample: O(1) per message at
        # the hundreds-to-thousands of messages/s this feed bursts to.
        self._bucket_s = bucket_s
        self._n_buckets = max(1, int(window_s // bucket_s))
        self._buckets: deque = deque()  # (bucket_id, min raw lag in s)
        self._baseline: Optional[float] = None
        # One estimator is shared by every connection to the same server
        # (the clock offset belongs to this machine, not to a socket), and
        # those connections run on separate threads.
        self._lock = threading.Lock()

    def observe(self, server_ts: float, receive_ts: float) -> float:
        """Record one message; return its lag in seconds (>= 0).
        Both arguments are Unix seconds, server's clock and ours."""
        raw = receive_ts - server_ts
        bucket = int(receive_ts // self._bucket_s)
        with self._lock:
            if self._buckets and self._buckets[-1][0] == bucket:
                if raw < self._buckets[-1][1]:
                    self._buckets[-1] = (bucket, raw)
            else:
                self._buckets.append((bucket, raw))
                oldest = bucket - self._n_buckets + 1
                while self._buckets[0][0] < oldest:
                    self._buckets.popleft()
                self._baseline = min(m for _, m in self._buckets)
            if self._baseline is None or raw < self._baseline:
                self._baseline = raw
            return raw - self._baseline

    @property
    def baseline_s(self) -> Optional[float]:
        """Clock offset + minimum path latency, as currently estimated."""
        return self._baseline

    def behind_s(self, latest_server_ts: Optional[float], now: float) -> Optional[float]:
        """
        How far behind the server a view is at `now`, given the newest
        server timestamp it has seen: everything the server did after that
        moment is still unknown to us. Equals the lag of the newest message
        plus the time since it arrived, so a pipe that has stalled keeps
        counting up instead of looking fresh. None if either is unknown.
        """
        base = self._baseline
        if latest_server_ts is None or base is None:
            return None
        return max(0.0, now - latest_server_ts - base)


# Module-level: shared by every PolymarketBot instance (the current market
# and the pre-warmed next one), and by every BinanceSpot connection.
POLYMARKET_FEED_LAG = FeedLagEstimator()
BINANCE_FEED_LAG = FeedLagEstimator()

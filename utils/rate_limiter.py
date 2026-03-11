from __future__ import annotations

import threading
import time


class GlobalRateLimiter:
    """Thread-safe global limiter, e.g. max 2 req/s across all workers."""

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be greater than 0")
        self._min_interval = 1.0 / rate_per_second
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_seconds = self._next_allowed - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
            self._next_allowed = max(self._next_allowed, now) + self._min_interval

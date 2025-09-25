#hub_router_1.0.1/src/simulation/utils/rate_limiter.py

import time
import threading

class RateLimiter:
    """
    Implementação simples de rate limit (token bucket).
    Garante no máximo X chamadas por segundo.
    """

    def __init__(self, max_calls_per_sec: int):
        self.max_calls = max_calls_per_sec
        self.interval = 1.0 / max_calls_per_sec
        self.lock = threading.Lock()
        self.last_call = 0.0

    def wait(self):
        """Bloqueia até respeitar o limite de chamadas por segundo."""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_call = time.time()

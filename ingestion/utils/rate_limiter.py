import time
from collections import deque
from threading import Lock

class RateLimiter:
    def __init__(self, requests_per_second: float):
        self.requests_per_second = requests_per_second
        self.min_interval = 1.0 / requests_per_second
        self.timestamps = deque(maxlen=int(requests_per_second))
        self.lock = Lock()
    
    def wait(self):
        with self.lock:
            now = time.time()
            if len(self.timestamps) == self.timestamps.maxlen:
                elapsed = now - self.timestamps[0]
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
            
            self.timestamps.append(time.time())
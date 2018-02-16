
import time

"""
Simple timer with a constant timeout
"""


class Timer:
    def __init__(self, timeout):
        self.timeout = timeout  # seconds
        self.running = False
        self.start_time = 0

    def start(self):
        self.start_time = time.time()
        self.running = True

    def reset(self):
        self.running = False

    def has_expired(self):
        return self.running and (time.time() - self.start_time > self.timeout)
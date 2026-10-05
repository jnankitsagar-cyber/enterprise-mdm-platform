"""Retry with exponential backoff + jitter for transient failures (network, throttling).

Only *transient* exceptions are retried; data errors go to the dead-letter
queue instead - retrying bad data never makes it good.
"""
from __future__ import annotations

import functools
import random
import time

from mdm.observability.logging import get_logger

log = get_logger("retry")


class TransientError(Exception):
    """Raise for failures that are worth retrying (timeouts, 429, 503 ...)."""


def retry(max_attempts: int = 3, backoff_seconds: float = 0.5, retry_on: tuple = (TransientError, TimeoutError, ConnectionError)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return fn(*args, **kwargs)
                except retry_on as exc:
                    if attempt >= max_attempts:
                        log.error(f"giving up after {attempt} attempts", fn=fn.__name__, error=str(exc))
                        raise
                    sleep = backoff_seconds * (2 ** (attempt - 1)) * (1 + random.random() * 0.1)
                    log.warning(f"transient failure, retrying", fn=fn.__name__, attempt=attempt, sleep_s=round(sleep, 3), error=str(exc))
                    time.sleep(sleep)
        return wrapper
    return deco

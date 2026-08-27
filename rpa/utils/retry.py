"""
Shared retry-and-recover decorator for RPA scrapers.

Why this exists: e-commerce pages are flaky — a page might not finish
loading, a selector might briefly not be there, or the site might rate-limit
you. Rather than let one bad page kill the whole scrape run, we retry with
exponential backoff and only give up after a fixed number of attempts.
"""

import time
import functools
import random
import logging

logger = logging.getLogger("retailpulse.rpa")


def retry_and_recover(max_attempts: int = 3, base_delay: float = 1.5):
    """
    Decorator that retries a function on failure with exponential backoff
    plus jitter (randomized delay) to avoid every retry hammering the site
    at the exact same moment.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    logger.warning(
                        "Attempt %s/%s failed for %s: %s. Retrying in %.1fs",
                        attempt, max_attempts, func.__name__, exc, delay
                    )
                    if attempt < max_attempts:
                        time.sleep(delay)
            logger.error("All %s attempts failed for %s", max_attempts, func.__name__)
            raise last_exception
        return wrapper
    return decorator

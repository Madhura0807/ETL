"""Retry helper with exponential backoff and a predicate for 'transient' errors."""
import logging
import time
from typing import Callable

from sqlalchemy import exc as sa_exc


def retry_call(
    func: Callable,
    *,
    max_attempts: int = 3,
    backoff_seconds: float = 1.0,
    backoff_factor: float = 2.0,
    should_retry: Callable[[Exception], bool] = lambda e: True,
    description: str = "operation",
    logger: logging.Logger | None = None,
    sleep: Callable[[float], None] = time.sleep,
):
    """Call ``func`` until it succeeds, a non-retryable error occurs, or attempts run out.

    The final exception is re-raised unchanged so callers keep the original error type.
    """
    log = logger or logging.getLogger("etl.retry")
    attempt = 1
    delay = backoff_seconds
    while True:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - predicate decides
            if attempt >= max_attempts or not should_retry(exc):
                raise
            log.warning(
                "%s failed (attempt %d/%d): %s. Retrying in %.1fs",
                description, attempt, max_attempts, exc, delay,
            )
            sleep(delay)
            delay *= backoff_factor
            attempt += 1


def is_transient_db_error(exc: Exception) -> bool:
    """Connection-level / timeout style database errors are worth retrying.

    Integrity, data and programming errors are deterministic and are NOT retried.
    """
    if isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError, sa_exc.TimeoutError)):
        return True
    return isinstance(exc, sa_exc.DBAPIError) and bool(exc.connection_invalidated)

"""Token-bucket rate limiter with daily caps and throttle logging.

Each API endpoint gets its own :class:`TokenBucket` configured via
:data:`RATE_LIMIT_CONFIGS`.  All callers that share an endpoint share
the *same* bucket instance, so aggregate request rate is enforced even
when multiple threads or pipeline stages hit the same API.

Usage::

    from prospect_engine.utils.rate_limiter import get_limiter

    limiter = get_limiter("sam_gov")
    limiter.acquire()          # blocks until a token is available
    response = httpx.get(...)
    if response.status_code == 429:
        sleep_sec = limiter.record_429()
        time.sleep(sleep_sec)
"""

from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RateLimitConfig:
    """Per-endpoint rate limit configuration."""

    name: str
    tokens_per_second: float  # refill rate
    max_burst: int            # bucket capacity
    daily_cap: Optional[int]  # None = unlimited
    backoff_initial: float = 2.0
    backoff_max: float = 60.0
    backoff_jitter: float = 0.30  # ±30 %


# Default configs — importable from config.py as well.
RATE_LIMIT_CONFIGS: Dict[str, RateLimitConfig] = {
    "sam_gov": RateLimitConfig(
        name="sam_gov",
        tokens_per_second=8.0,
        max_burst=8,
        daily_cap=1000,
    ),
    "sam_entity": RateLimitConfig(
        name="sam_entity",
        tokens_per_second=8.0,
        max_burst=8,
        daily_cap=800,
    ),
    "sbir": RateLimitConfig(
        name="sbir",
        tokens_per_second=1.0 / 6.0,   # 10 requests per 10 minutes
        max_burst=2,
        daily_cap=None,
    ),
    "usa_spending": RateLimitConfig(
        name="usa_spending",
        tokens_per_second=10.0,
        max_burst=10,
        daily_cap=None,
    ),
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class DailyCapExhausted(Exception):
    """Raised when an endpoint's daily request cap has been reached."""


# ---------------------------------------------------------------------------
# Token Bucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """Thread-safe token-bucket rate limiter with daily cap tracking.

    Args:
        config: Rate limit configuration for the endpoint.
        db_path: Path to the engine SQLite database (for logging).
    """

    def __init__(
        self,
        config: RateLimitConfig,
        db_path: Optional[Path] = None,
    ) -> None:
        self._config = config
        self._db_path = db_path
        self._lock = threading.Lock()

        # Token state
        self._tokens: float = float(config.max_burst)
        self._last_refill: float = time.monotonic()

        # Daily cap state
        self._daily_count: int = 0
        self._daily_reset_date: date = date.today()

        # Consecutive-429 counter for exponential backoff
        self._consecutive_429: int = 0

    # ----- public API -----

    def acquire(self, timeout: float = 300.0) -> None:
        """Block until a token is available.

        Raises:
            DailyCapExhausted: If the daily request cap has been hit.
            TimeoutError: If *timeout* seconds elapse without a token.
        """
        deadline = time.monotonic() + timeout

        while True:
            with self._lock:
                self._maybe_reset_daily()
                if self._config.daily_cap is not None and self._daily_count >= self._config.daily_cap:
                    self._log_event("daily_cap", 0.0, "Daily cap {} reached".format(self._config.daily_cap))
                    raise DailyCapExhausted(
                        "{}: daily cap of {} reached".format(self._config.name, self._config.daily_cap)
                    )

                self._refill()

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    self._daily_count += 1
                    self._consecutive_429 = 0  # successful acquire resets backoff
                    return

                # Compute how long until next token
                wait = (1.0 - self._tokens) / self._config.tokens_per_second

            # Release lock while sleeping
            if time.monotonic() + wait > deadline:
                raise TimeoutError(
                    "{}: timed out waiting for rate limit token".format(self._config.name)
                )

            self._log_event("throttled", wait, "Waiting for token")
            logger.debug("%s: throttled, waiting %.1fs", self._config.name, wait)
            time.sleep(wait)

    def record_429(self, retry_after: Optional[float] = None) -> float:
        """Compute and return the backoff duration after a 429 response.

        The caller is responsible for sleeping the returned number of
        seconds.  This method logs the event to SQLite.

        Args:
            retry_after: Value from the ``Retry-After`` header, if present.

        Returns:
            Number of seconds the caller should sleep.
        """
        with self._lock:
            self._consecutive_429 += 1

            if retry_after is not None and retry_after > 0:
                sleep_sec = retry_after
            else:
                raw = self._config.backoff_initial * (2 ** (self._consecutive_429 - 1))
                raw = min(raw, self._config.backoff_max)
                jitter_range = raw * self._config.backoff_jitter
                sleep_sec = raw + random.uniform(-jitter_range, jitter_range)
                sleep_sec = max(0.1, sleep_sec)

        self._log_event(
            "429_backoff",
            sleep_sec,
            "429 #{}, sleep {:.1f}s".format(self._consecutive_429, sleep_sec),
        )
        logger.warning(
            "%s: 429 backoff #%d — sleeping %.1fs",
            self._config.name,
            self._consecutive_429,
            sleep_sec,
        )
        return sleep_sec

    @property
    def daily_count(self) -> int:
        """Number of requests made today for this endpoint."""
        with self._lock:
            self._maybe_reset_daily()
            return self._daily_count

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    # ----- internal helpers -----

    def _refill(self) -> None:
        """Add tokens based on elapsed time since last refill.  Caller holds lock."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._config.max_burst),
            self._tokens + elapsed * self._config.tokens_per_second,
        )
        self._last_refill = now

    def _maybe_reset_daily(self) -> None:
        """Reset daily counter if the date has rolled over.  Caller holds lock."""
        today = date.today()
        if today > self._daily_reset_date:
            logger.info(
                "%s: daily counter reset (%d requests yesterday)",
                self._config.name,
                self._daily_count,
            )
            self._daily_count = 0
            self._daily_reset_date = today

    def _log_event(self, event_type: str, wait_seconds: float, details: str) -> None:
        """Insert a row into the rate_limit_log table."""
        if self._db_path is None:
            return
        try:
            # Lazy import to avoid circular dependency at module load
            from prospect_engine.utils.db import get_connection

            conn = get_connection(self._db_path)
            conn.execute(
                "INSERT INTO rate_limit_log (timestamp, endpoint, event_type, wait_seconds, details) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    datetime.utcnow().isoformat(),
                    self._config.name,
                    event_type,
                    wait_seconds,
                    details,
                ),
            )
            conn.commit()
        except Exception:
            # Never let logging failures break the pipeline
            logger.debug("Failed to log rate-limit event", exc_info=True)


# ---------------------------------------------------------------------------
# Module-level singleton registry
# ---------------------------------------------------------------------------

_limiters: Dict[str, TokenBucket] = {}
_registry_lock = threading.Lock()


def get_limiter(
    endpoint: str,
    db_path: Optional[Path] = None,
) -> TokenBucket:
    """Return the shared :class:`TokenBucket` for *endpoint*.

    Creates the bucket on first call using :data:`RATE_LIMIT_CONFIGS`.
    All callers that use the same *endpoint* name share one bucket, so
    aggregate request rate is enforced.

    Args:
        endpoint: Logical API name (``"sam_gov"``, ``"sbir"``, etc.).
        db_path: Override the engine.db path (useful for tests).

    Raises:
        KeyError: If *endpoint* has no entry in ``RATE_LIMIT_CONFIGS``.
    """
    with _registry_lock:
        if endpoint not in _limiters:
            if endpoint not in RATE_LIMIT_CONFIGS:
                raise KeyError(
                    "No rate-limit config for endpoint {!r}. "
                    "Available: {}".format(endpoint, list(RATE_LIMIT_CONFIGS))
                )
            _limiters[endpoint] = TokenBucket(
                RATE_LIMIT_CONFIGS[endpoint],
                db_path=db_path,
            )
        return _limiters[endpoint]


def reset_all_limiters() -> None:
    """Clear all cached limiter instances (useful for tests)."""
    with _registry_lock:
        _limiters.clear()

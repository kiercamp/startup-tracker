"""HTTP utilities with exponential backoff retry logic and rate limiting.

When an ``endpoint`` is provided to :func:`get_with_retry` or
:func:`post_with_retry`, the request is throttled through the
corresponding :class:`TokenBucket` from :mod:`rate_limiter` before
hitting the network.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Optional

import httpx

from prospect_engine.config import (
    MAX_RETRIES,
    INITIAL_BACKOFF_SECONDS,
    BACKOFF_MULTIPLIER,
    MAX_BACKOFF_SECONDS,
)

logger = logging.getLogger(__name__)


def _execute_with_retry(
    request_fn: Callable[[], httpx.Response],
    description: str,
    max_retries: Optional[int] = None,
    endpoint: Optional[str] = None,
) -> httpx.Response:
    """Execute an HTTP request with exponential backoff on 429/5xx errors.

    Args:
        request_fn: A callable that performs the HTTP request and returns a Response.
        description: Human-readable description for log messages.
        max_retries: Override for the global MAX_RETRIES setting. Use a lower
            value for APIs with strict rate limits (e.g. SBIR: max_retries=1).
        endpoint: Logical API name (e.g. ``"sam_gov"``). When set, 429 backoff
            is delegated to the rate limiter and the token bucket is consulted
            before each retry.

    Returns:
        The successful httpx.Response.

    Raises:
        httpx.HTTPStatusError: If all retries are exhausted on HTTP errors.
        httpx.RequestError: On network-level failures after all retries.
    """
    retries = max_retries if max_retries is not None else MAX_RETRIES
    backoff = INITIAL_BACKOFF_SECONDS
    last_exception: Optional[Exception] = None
    limiter = None

    if endpoint is not None:
        try:
            from prospect_engine.utils.rate_limiter import get_limiter
            limiter = get_limiter(endpoint)
        except Exception:
            logger.debug("Could not load rate limiter for %s", endpoint, exc_info=True)

    for attempt in range(retries + 1):
        # Acquire a rate-limit token before each attempt
        if limiter is not None:
            limiter.acquire()

        try:
            response = request_fn()
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 429 or status >= 500:
                last_exception = exc
                if attempt < retries:
                    # Compute sleep time
                    if status == 429 and limiter is not None:
                        # Delegate backoff to the rate limiter
                        retry_after_hdr = exc.response.headers.get("Retry-After")
                        retry_after = None
                        if retry_after_hdr:
                            try:
                                retry_after = float(retry_after_hdr)
                            except (ValueError, TypeError):
                                pass
                        sleep_time = limiter.record_429(retry_after)
                    else:
                        # Fall back to local exponential backoff
                        retry_after_hdr = exc.response.headers.get("Retry-After")
                        if retry_after_hdr:
                            try:
                                sleep_time = min(float(retry_after_hdr), MAX_BACKOFF_SECONDS)
                            except (ValueError, TypeError):
                                sleep_time = min(backoff, MAX_BACKOFF_SECONDS)
                        else:
                            sleep_time = min(backoff, MAX_BACKOFF_SECONDS)
                        # Add jitter (±25%) to avoid thundering herd
                        jitter = sleep_time * 0.25 * (2 * random.random() - 1)
                        sleep_time = max(0.5, sleep_time + jitter)

                    logger.warning(
                        "%s: HTTP %d on attempt %d/%d, retrying in %.1fs",
                        description,
                        status,
                        attempt + 1,
                        retries + 1,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
                    backoff *= BACKOFF_MULTIPLIER
                else:
                    logger.error(
                        "%s: HTTP %d after %d attempts, giving up",
                        description,
                        status,
                        retries + 1,
                    )
                    raise
            else:
                raise
        except httpx.RequestError as exc:
            last_exception = exc
            if attempt < retries:
                sleep_time = min(backoff, MAX_BACKOFF_SECONDS)
                logger.warning(
                    "%s: Network error on attempt %d/%d (%s), retrying in %.1fs",
                    description,
                    attempt + 1,
                    retries + 1,
                    str(exc),
                    sleep_time,
                )
                time.sleep(sleep_time)
                backoff *= BACKOFF_MULTIPLIER
            else:
                logger.error(
                    "%s: Network error after %d attempts, giving up",
                    description,
                    retries + 1,
                )
                raise

    # Should not reach here, but just in case
    raise last_exception  # type: ignore[misc]


_DEFAULT_HEADERS = {
    "User-Agent": "ADProspectEngine/1.0",
}


def get_with_retry(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
    max_retries: Optional[int] = None,
    endpoint: Optional[str] = None,
) -> httpx.Response:
    """Perform an HTTP GET with exponential backoff on 429/5xx responses.

    Args:
        url: Target URL.
        params: Query parameters dict.
        headers: Request headers dict.
        timeout: Per-request timeout in seconds.
        max_retries: Override for the global MAX_RETRIES (e.g. 1 for SBIR).
        endpoint: Logical API name (e.g. ``"sam_gov"``).  When provided the
            request is throttled through the rate limiter.

    Returns:
        The successful httpx.Response object.
    """
    # Redact api_key from log output
    log_url = url.split("?")[0] if "?" in url else url
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}

    def _do_request() -> httpx.Response:
        return httpx.get(url, params=params, headers=merged_headers, timeout=timeout)

    return _execute_with_retry(
        _do_request, f"GET {log_url}", max_retries=max_retries, endpoint=endpoint,
    )


def post_with_retry(
    url: str,
    *,
    json: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
    max_retries: Optional[int] = None,
    endpoint: Optional[str] = None,
) -> httpx.Response:
    """Perform an HTTP POST with exponential backoff on 429/5xx responses.

    Args:
        url: Target URL.
        json: JSON request body as a dict.
        headers: Request headers dict.
        timeout: Per-request timeout in seconds.
        max_retries: Override for the global MAX_RETRIES (e.g. 1 for SBIR).
        endpoint: Logical API name (e.g. ``"usa_spending"``).  When provided
            the request is throttled through the rate limiter.

    Returns:
        The successful httpx.Response object.
    """
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}

    def _do_request() -> httpx.Response:
        return httpx.post(url, json=json, headers=merged_headers, timeout=timeout)

    return _execute_with_retry(
        _do_request, f"POST {url}", max_retries=max_retries, endpoint=endpoint,
    )

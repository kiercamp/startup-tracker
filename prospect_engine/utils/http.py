"""HTTP utilities with exponential backoff retry logic."""

from __future__ import annotations

import logging
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
) -> httpx.Response:
    """Execute an HTTP request with exponential backoff on 429/5xx errors.

    Args:
        request_fn: A callable that performs the HTTP request and returns a Response.
        description: Human-readable description for log messages.

    Returns:
        The successful httpx.Response.

    Raises:
        httpx.HTTPStatusError: If all retries are exhausted on HTTP errors.
        httpx.RequestError: On network-level failures after all retries.
    """
    backoff = INITIAL_BACKOFF_SECONDS
    last_exception: Optional[Exception] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = request_fn()
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status == 429 or status >= 500:
                last_exception = exc
                if attempt < MAX_RETRIES:
                    sleep_time = min(backoff, MAX_BACKOFF_SECONDS)
                    logger.warning(
                        "%s: HTTP %d on attempt %d/%d, retrying in %.1fs",
                        description,
                        status,
                        attempt + 1,
                        MAX_RETRIES + 1,
                        sleep_time,
                    )
                    time.sleep(sleep_time)
                    backoff *= BACKOFF_MULTIPLIER
                else:
                    logger.error(
                        "%s: HTTP %d after %d attempts, giving up",
                        description,
                        status,
                        MAX_RETRIES + 1,
                    )
                    raise
            else:
                raise
        except httpx.RequestError as exc:
            last_exception = exc
            if attempt < MAX_RETRIES:
                sleep_time = min(backoff, MAX_BACKOFF_SECONDS)
                logger.warning(
                    "%s: Network error on attempt %d/%d (%s), retrying in %.1fs",
                    description,
                    attempt + 1,
                    MAX_RETRIES + 1,
                    str(exc),
                    sleep_time,
                )
                time.sleep(sleep_time)
                backoff *= BACKOFF_MULTIPLIER
            else:
                logger.error(
                    "%s: Network error after %d attempts, giving up",
                    description,
                    MAX_RETRIES + 1,
                )
                raise

    # Should not reach here, but just in case
    raise last_exception  # type: ignore[misc]


def get_with_retry(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
) -> httpx.Response:
    """Perform an HTTP GET with exponential backoff on 429/5xx responses.

    Args:
        url: Target URL.
        params: Query parameters dict.
        headers: Request headers dict.
        timeout: Per-request timeout in seconds.

    Returns:
        The successful httpx.Response object.
    """
    # Redact api_key from log output
    log_url = url.split("?")[0] if "?" in url else url

    def _do_request() -> httpx.Response:
        return httpx.get(url, params=params, headers=headers, timeout=timeout)

    return _execute_with_retry(_do_request, f"GET {log_url}")


def post_with_retry(
    url: str,
    *,
    json: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 30.0,
) -> httpx.Response:
    """Perform an HTTP POST with exponential backoff on 429/5xx responses.

    Args:
        url: Target URL.
        json: JSON request body as a dict.
        headers: Request headers dict.
        timeout: Per-request timeout in seconds.

    Returns:
        The successful httpx.Response object.
    """

    def _do_request() -> httpx.Response:
        return httpx.post(url, json=json, headers=headers, timeout=timeout)

    return _execute_with_retry(_do_request, f"POST {url}")

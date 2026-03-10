"""Tests for utility modules."""

from unittest.mock import patch, MagicMock
import httpx
import pytest

from prospect_engine.utils.http import get_with_retry, post_with_retry


def test_get_with_retry_success():
    """Successful GET returns response on first try."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("prospect_engine.utils.http.httpx.get", return_value=mock_response):
        resp = get_with_retry("https://example.com/api", timeout=5.0)
        assert resp.status_code == 200


def test_post_with_retry_success():
    """Successful POST returns response on first try."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("prospect_engine.utils.http.httpx.post", return_value=mock_response):
        resp = post_with_retry(
            "https://example.com/api", json={"key": "val"}, timeout=5.0
        )
        assert resp.status_code == 200


@patch("prospect_engine.utils.http.time.sleep")
def test_get_retries_on_429(mock_sleep):
    """GET retries on 429 then succeeds."""
    fail_response = MagicMock(spec=httpx.Response)
    fail_response.status_code = 429
    fail_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError(
            "429", request=MagicMock(), response=fail_response
        )
    )

    ok_response = MagicMock(spec=httpx.Response)
    ok_response.status_code = 200
    ok_response.raise_for_status = MagicMock()

    with patch(
        "prospect_engine.utils.http.httpx.get",
        side_effect=[fail_response, ok_response],
    ):
        resp = get_with_retry("https://example.com/api", timeout=5.0)
        assert resp.status_code == 200
        assert mock_sleep.call_count == 1


@patch("prospect_engine.utils.http.time.sleep")
def test_get_retries_on_500(mock_sleep):
    """GET retries on 500 then succeeds."""
    fail_response = MagicMock(spec=httpx.Response)
    fail_response.status_code = 500
    fail_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError(
            "500", request=MagicMock(), response=fail_response
        )
    )

    ok_response = MagicMock(spec=httpx.Response)
    ok_response.status_code = 200
    ok_response.raise_for_status = MagicMock()

    with patch(
        "prospect_engine.utils.http.httpx.get",
        side_effect=[fail_response, ok_response],
    ):
        resp = get_with_retry("https://example.com/api", timeout=5.0)
        assert resp.status_code == 200


def test_get_raises_on_404():
    """GET does not retry on 404 — raises immediately."""
    fail_response = MagicMock(spec=httpx.Response)
    fail_response.status_code = 404
    fail_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError(
            "404", request=MagicMock(), response=fail_response
        )
    )

    with patch("prospect_engine.utils.http.httpx.get", return_value=fail_response):
        with pytest.raises(httpx.HTTPStatusError):
            get_with_retry("https://example.com/api", timeout=5.0)


def test_logging_setup():
    """configure_logging runs without error."""
    import logging

    from prospect_engine.utils.logging_setup import configure_logging

    # Reset root logger handlers to test fresh setup
    root = logging.getLogger()
    root.handlers.clear()

    configure_logging(level=logging.WARNING)
    assert len(root.handlers) == 2  # file + stream

    # Cleanup
    root.handlers.clear()

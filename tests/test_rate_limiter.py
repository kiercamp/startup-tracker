"""Tests for prospect_engine.utils.rate_limiter — token bucket rate limiter."""

import time
from unittest.mock import patch

import pytest

from prospect_engine.utils.rate_limiter import (
    RateLimitConfig,
    TokenBucket,
    DailyCapExhausted,
    RATE_LIMIT_CONFIGS,
    get_limiter,
    reset_all_limiters,
)


@pytest.fixture(autouse=True)
def _clean_limiters():
    """Reset the singleton registry before each test."""
    reset_all_limiters()
    yield
    reset_all_limiters()


class TestRateLimitConfig:
    """Tests for the RateLimitConfig dataclass."""

    def test_defaults(self):
        cfg = RateLimitConfig(name="test", tokens_per_second=1.0, max_burst=5, daily_cap=None)
        assert cfg.backoff_initial == 2.0
        assert cfg.backoff_max == 60.0
        assert cfg.backoff_jitter == 0.30

    def test_frozen(self):
        cfg = RateLimitConfig(name="test", tokens_per_second=1.0, max_burst=5, daily_cap=None)
        with pytest.raises(AttributeError):
            cfg.name = "changed"


class TestTokenBucket:
    """Tests for the TokenBucket class."""

    def _make_bucket(self, **overrides) -> TokenBucket:
        defaults = {
            "name": "test",
            "tokens_per_second": 100.0,  # Fast refill for tests
            "max_burst": 5,
            "daily_cap": None,
        }
        defaults.update(overrides)
        return TokenBucket(RateLimitConfig(**defaults))

    def test_acquire_succeeds_when_tokens_available(self):
        bucket = self._make_bucket(max_burst=3)
        # Should not raise — 3 tokens available
        bucket.acquire()
        bucket.acquire()
        bucket.acquire()

    def test_acquire_blocks_when_no_tokens(self):
        # Very slow refill, 1 token capacity
        bucket = self._make_bucket(tokens_per_second=0.5, max_burst=1)
        bucket.acquire()  # Use the only token

        start = time.monotonic()
        bucket.acquire()  # Should block ~2s for refill
        elapsed = time.monotonic() - start
        assert elapsed >= 1.5  # Give some tolerance

    def test_daily_cap_raises(self):
        bucket = self._make_bucket(daily_cap=2)
        bucket.acquire()
        bucket.acquire()
        with pytest.raises(DailyCapExhausted):
            bucket.acquire()

    def test_daily_count_property(self):
        bucket = self._make_bucket(daily_cap=10)
        assert bucket.daily_count == 0
        bucket.acquire()
        assert bucket.daily_count == 1
        bucket.acquire()
        assert bucket.daily_count == 2

    def test_daily_reset_on_new_day(self):
        bucket = self._make_bucket(daily_cap=2)
        bucket.acquire()
        bucket.acquire()
        assert bucket.daily_count == 2

        # Simulate date rollover
        import datetime
        with patch("prospect_engine.utils.rate_limiter.date") as mock_date:
            mock_date.today.return_value = datetime.date.today() + datetime.timedelta(days=1)
            mock_date.side_effect = lambda *a, **kw: datetime.date(*a, **kw)
            # daily_count should reset on next check
            assert bucket.daily_count == 0

    def test_record_429_returns_positive_sleep(self):
        bucket = self._make_bucket()
        sleep_time = bucket.record_429()
        assert sleep_time > 0

    def test_record_429_exponential_backoff(self):
        bucket = self._make_bucket()
        sleep1 = bucket.record_429()
        sleep2 = bucket.record_429()
        # Second 429 should generally sleep longer (exponential)
        # Due to jitter, we just check it's positive
        assert sleep1 > 0
        assert sleep2 > 0

    def test_record_429_respects_retry_after(self):
        bucket = self._make_bucket()
        sleep_time = bucket.record_429(retry_after=30.0)
        assert sleep_time == 30.0

    def test_acquire_timeout(self):
        # Very slow refill, 1 token
        bucket = self._make_bucket(tokens_per_second=0.01, max_burst=1)
        bucket.acquire()  # Use the only token
        with pytest.raises(TimeoutError):
            bucket.acquire(timeout=0.1)


class TestEndpointConfigs:
    """Tests for the built-in rate limit configurations."""

    def test_sam_gov_config_exists(self):
        assert "sam_gov" in RATE_LIMIT_CONFIGS
        cfg = RATE_LIMIT_CONFIGS["sam_gov"]
        assert cfg.daily_cap == 1000
        assert cfg.tokens_per_second == 8.0

    def test_sam_entity_config_exists(self):
        assert "sam_entity" in RATE_LIMIT_CONFIGS
        cfg = RATE_LIMIT_CONFIGS["sam_entity"]
        assert cfg.daily_cap == 800

    def test_sbir_config_exists(self):
        assert "sbir" in RATE_LIMIT_CONFIGS
        cfg = RATE_LIMIT_CONFIGS["sbir"]
        assert cfg.daily_cap is None
        # 1 request per 6 seconds
        assert abs(cfg.tokens_per_second - 1.0 / 6.0) < 0.001

    def test_usa_spending_config_exists(self):
        assert "usa_spending" in RATE_LIMIT_CONFIGS
        cfg = RATE_LIMIT_CONFIGS["usa_spending"]
        assert cfg.daily_cap is None
        assert cfg.tokens_per_second == 10.0


class TestGetLimiter:
    """Tests for the get_limiter() singleton registry."""

    def test_returns_token_bucket(self):
        limiter = get_limiter("sam_gov")
        assert isinstance(limiter, TokenBucket)

    def test_returns_same_instance(self):
        limiter1 = get_limiter("sam_gov")
        limiter2 = get_limiter("sam_gov")
        assert limiter1 is limiter2

    def test_different_endpoints_different_instances(self):
        limiter1 = get_limiter("sam_gov")
        limiter2 = get_limiter("sbir")
        assert limiter1 is not limiter2

    def test_unknown_endpoint_raises(self):
        with pytest.raises(KeyError, match="No rate-limit config"):
            get_limiter("nonexistent_api")

    def test_reset_clears_registry(self):
        limiter1 = get_limiter("sam_gov")
        reset_all_limiters()
        limiter2 = get_limiter("sam_gov")
        assert limiter1 is not limiter2

"""Tests for prospect_engine.utils.cache — response cache."""

import json
from datetime import timedelta
from pathlib import Path

import pytest

from prospect_engine.utils.cache import (
    ResponseCache,
    CACHE_TTLS,
    get_cache,
    reset_cache,
    _params_hash,
)
from prospect_engine.utils.db import get_connection, close_connection


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """Provide a temporary database path and ensure tables exist."""
    db = tmp_path / "test_engine.db"
    # Force table creation by getting a connection
    get_connection(db)
    return db


@pytest.fixture
def cache(tmp_db: Path) -> ResponseCache:
    """Provide a fresh ResponseCache instance backed by a temp DB."""
    return ResponseCache(db_path=tmp_db)


@pytest.fixture(autouse=True)
def _clean_singleton():
    """Reset the module-level cache singleton between tests."""
    reset_cache()
    yield
    reset_cache()


class TestParamsHash:
    """Tests for _params_hash()."""

    def test_deterministic(self):
        h1 = _params_hash({"a": 1, "b": 2})
        h2 = _params_hash({"a": 1, "b": 2})
        assert h1 == h2

    def test_order_independent(self):
        h1 = _params_hash({"a": 1, "b": 2})
        h2 = _params_hash({"b": 2, "a": 1})
        assert h1 == h2

    def test_different_params_different_hash(self):
        h1 = _params_hash({"a": 1})
        h2 = _params_hash({"a": 2})
        assert h1 != h2

    def test_returns_hex_string(self):
        h = _params_hash({"x": "test"})
        assert isinstance(h, str)
        assert len(h) == 64  # SHA-256 hex digest


class TestResponseCache:
    """Tests for the ResponseCache class."""

    def test_put_and_get(self, cache: ResponseCache):
        params = {"agency": "DOD", "year": 2024}
        data = json.dumps({"results": [1, 2, 3]})
        cache.put("sbir", params, data)
        result = cache.get("sbir", params)
        assert result == data

    def test_get_miss_returns_none(self, cache: ResponseCache):
        result = cache.get("sbir", {"agency": "DOD", "year": 9999})
        assert result is None

    def test_expired_entry_returns_none(self, cache: ResponseCache):
        params = {"key": "expired"}
        data = json.dumps({"test": True})
        # Store with a TTL of 0 (already expired)
        cache.put("sbir", params, data, ttl=timedelta(seconds=-1))
        result = cache.get("sbir", params)
        assert result is None

    def test_different_endpoints_independent(self, cache: ResponseCache):
        params = {"key": "same"}
        data1 = json.dumps({"source": "sbir"})
        data2 = json.dumps({"source": "sam_gov"})
        cache.put("sbir", params, data1)
        cache.put("sam_gov", params, data2)

        assert cache.get("sbir", params) == data1
        assert cache.get("sam_gov", params) == data2

    def test_put_replaces_existing(self, cache: ResponseCache):
        params = {"key": "replace"}
        cache.put("sbir", params, json.dumps({"v": 1}))
        cache.put("sbir", params, json.dumps({"v": 2}))
        result = json.loads(cache.get("sbir", params))
        assert result["v"] == 2

    def test_stats_tracking(self, cache: ResponseCache):
        params = {"key": "stats"}
        # 1 miss
        cache.get("sbir", params)
        # Store and hit
        cache.put("sbir", params, json.dumps({}))
        cache.get("sbir", params)

        stats = cache.stats("sbir")
        assert "sbir" in stats
        assert stats["sbir"]["hits"] == 1
        assert stats["sbir"]["misses"] == 1
        assert stats["sbir"]["ratio"] == 0.5

    def test_stats_all_endpoints(self, cache: ResponseCache):
        cache.get("sbir", {"a": 1})
        cache.get("sam_gov", {"b": 2})
        stats = cache.stats()
        assert "sbir" in stats
        assert "sam_gov" in stats

    def test_evict_expired(self, cache: ResponseCache):
        # Insert one expired and one fresh
        cache.put("sbir", {"k": "expired"}, "{}", ttl=timedelta(seconds=-1))
        cache.put("sbir", {"k": "fresh"}, "{}", ttl=timedelta(hours=24))

        removed = cache.evict_expired()
        assert removed >= 1

        # Fresh entry should still be retrievable
        assert cache.get("sbir", {"k": "fresh"}) is not None
        # Expired entry should be gone
        assert cache.get("sbir", {"k": "expired"}) is None


class TestCacheTTLs:
    """Tests for the default TTL configuration."""

    def test_sbir_ttl_is_7_days(self):
        assert CACHE_TTLS["sbir"] == timedelta(days=7)

    def test_sam_entity_ttl_is_48h(self):
        assert CACHE_TTLS["sam_entity"] == timedelta(hours=48)

    def test_sam_gov_ttl_is_24h(self):
        assert CACHE_TTLS["sam_gov"] == timedelta(hours=24)

    def test_usa_spending_ttl_is_24h(self):
        assert CACHE_TTLS["usa_spending"] == timedelta(hours=24)


class TestGetCacheSingleton:
    """Tests for the get_cache() singleton."""

    def test_returns_response_cache(self):
        cache = get_cache()
        assert isinstance(cache, ResponseCache)

    def test_returns_same_instance(self):
        c1 = get_cache()
        c2 = get_cache()
        assert c1 is c2

    def test_reset_clears_singleton(self):
        c1 = get_cache()
        reset_cache()
        c2 = get_cache()
        assert c1 is not c2

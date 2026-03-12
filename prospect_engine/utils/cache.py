"""SQLite-backed response cache with per-endpoint TTLs.

Every API response is stored keyed by ``(endpoint, sha256(params))``.
Before making an API call, callers check the cache; if a fresh entry
exists the network call is skipped entirely.

Usage::

    from prospect_engine.utils.cache import get_cache

    cache = get_cache()
    cached = cache.get("sbir", {"agency": "DOD", "year": 2024})
    if cached is not None:
        data = json.loads(cached)
    else:
        response = get_with_retry(url, params=params, endpoint="sbir")
        data = response.json()
        cache.put("sbir", {"agency": "DOD", "year": 2024}, json.dumps(data))
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default TTLs per endpoint
# ---------------------------------------------------------------------------

CACHE_TTLS: Dict[str, timedelta] = {
    "sbir": timedelta(days=7),
    "sam_entity": timedelta(hours=48),
    "sam_gov": timedelta(hours=24),
    "usa_spending": timedelta(hours=24),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _params_hash(params: Dict[str, Any]) -> str:
    """Deterministic SHA-256 hex digest of a params dict."""
    canonical = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# ResponseCache
# ---------------------------------------------------------------------------

class ResponseCache:
    """SQLite-backed response cache with per-endpoint TTLs.

    Args:
        db_path: Path to the engine SQLite database.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"hits": 0, "misses": 0})
        self._lock = threading.Lock()

    # ----- public API -----

    def get(self, endpoint: str, params: Dict[str, Any]) -> Optional[str]:
        """Return cached response JSON if fresh, else ``None``."""
        ph = _params_hash(params)
        now = datetime.utcnow().isoformat()

        conn = self._conn()
        row = conn.execute(
            "SELECT response_json FROM api_cache "
            "WHERE endpoint = ? AND params_hash = ? AND expires_at > ?",
            (endpoint, ph, now),
        ).fetchone()

        with self._lock:
            if row is not None:
                self._stats[endpoint]["hits"] += 1
                logger.debug("Cache HIT %s %s…", endpoint, ph[:12])
                return row["response_json"]
            self._stats[endpoint]["misses"] += 1
            logger.debug("Cache MISS %s %s…", endpoint, ph[:12])
            return None

    def put(
        self,
        endpoint: str,
        params: Dict[str, Any],
        response_json: str,
        ttl: Optional[timedelta] = None,
    ) -> None:
        """Store a response in the cache.

        Args:
            endpoint: Logical API name.
            params: The query parameters used for the request.
            response_json: Raw JSON response body.
            ttl: Override the default TTL for this endpoint.
        """
        ph = _params_hash(params)
        ttl = ttl or CACHE_TTLS.get(endpoint, timedelta(hours=24))
        now = datetime.utcnow()
        expires = now + ttl

        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO api_cache "
            "(endpoint, params_hash, response_json, fetched_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (endpoint, ph, response_json, now.isoformat(), expires.isoformat()),
        )
        conn.commit()
        logger.debug("Cache PUT %s %s… (expires %s)", endpoint, ph[:12], expires.isoformat())

    def stats(self, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """Return hit / miss counts (and ratio) per endpoint.

        If *endpoint* is given, returns stats for that endpoint only.
        """
        with self._lock:
            if endpoint is not None:
                s = dict(self._stats.get(endpoint, {"hits": 0, "misses": 0}))
                total = s["hits"] + s["misses"]
                s["ratio"] = s["hits"] / total if total > 0 else 0.0
                return {endpoint: s}

            result = {}
            for ep, s in self._stats.items():
                entry = dict(s)
                total = entry["hits"] + entry["misses"]
                entry["ratio"] = entry["hits"] / total if total > 0 else 0.0
                result[ep] = entry
            return result

    def evict_expired(self) -> int:
        """Delete expired cache entries.  Returns the number of rows removed."""
        now = datetime.utcnow().isoformat()
        conn = self._conn()
        cursor = conn.execute(
            "DELETE FROM api_cache WHERE expires_at <= ?", (now,)
        )
        conn.commit()
        removed = cursor.rowcount
        if removed:
            logger.info("Evicted %d expired cache entries", removed)
        return removed

    # ----- internal -----

    def _conn(self):
        """Lazy import to avoid circular dependency at module load."""
        from prospect_engine.utils.db import get_connection

        return get_connection(self._db_path)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_cache: Optional[ResponseCache] = None
_cache_lock = threading.Lock()


def get_cache(db_path: Optional[Path] = None) -> ResponseCache:
    """Return the shared :class:`ResponseCache` instance."""
    global _cache
    with _cache_lock:
        if _cache is None:
            _cache = ResponseCache(db_path=db_path)
        return _cache


def reset_cache() -> None:
    """Clear the singleton (useful for tests)."""
    global _cache
    with _cache_lock:
        _cache = None

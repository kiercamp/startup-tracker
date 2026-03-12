"""Shared SQLite connection manager for the engine database.

Provides thread-local connections to ``engine.db`` (separate from the
export-only ``prospects.db``).  All infrastructure tables — api_cache,
rate_limit_log, task_queue, sweep_schedule, sam_entities — are created
on first access.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

from prospect_engine.config import OUTPUT_DIR

logger = logging.getLogger(__name__)

ENGINE_DB_PATH: Path = OUTPUT_DIR / "engine.db"

_local = threading.local()


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    """Return a thread-local SQLite connection, creating tables on first use.

    Args:
        db_path: Override the default engine.db path (useful for tests).
    """
    path = db_path or ENGINE_DB_PATH
    attr = f"conn_{path}"

    conn: sqlite3.Connection | None = getattr(_local, attr, None)
    if conn is not None:
        return conn

    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    _init_tables(conn)
    setattr(_local, attr, conn)
    logger.debug("Opened engine DB at %s", path)
    return conn


def close_connection(db_path: Path | None = None) -> None:
    """Close the thread-local connection (e.g. during shutdown)."""
    path = db_path or ENGINE_DB_PATH
    attr = f"conn_{path}"
    conn: sqlite3.Connection | None = getattr(_local, attr, None)
    if conn is not None:
        conn.close()
        setattr(_local, attr, None)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
-- Response cache for API calls
CREATE TABLE IF NOT EXISTS api_cache (
    endpoint       TEXT    NOT NULL,
    params_hash    TEXT    NOT NULL,
    response_json  TEXT    NOT NULL,
    fetched_at     TEXT    NOT NULL,
    expires_at     TEXT    NOT NULL,
    PRIMARY KEY (endpoint, params_hash)
);
CREATE INDEX IF NOT EXISTS idx_api_cache_expires
    ON api_cache(expires_at);

-- Rate limiter event log
CREATE TABLE IF NOT EXISTS rate_limit_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp      TEXT    NOT NULL,
    endpoint       TEXT    NOT NULL,
    event_type     TEXT    NOT NULL,
    wait_seconds   REAL,
    details        TEXT
);
CREATE INDEX IF NOT EXISTS idx_rate_limit_log_ts
    ON rate_limit_log(timestamp);

-- Persistent task queue for API requests
CREATE TABLE IF NOT EXISTS task_queue (
    task_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint       TEXT    NOT NULL,
    action         TEXT    NOT NULL,
    params_json    TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT 'pending',
    priority       INTEGER DEFAULT 0,
    created_at     TEXT    NOT NULL,
    started_at     TEXT,
    completed_at   TEXT,
    result_hash    TEXT,
    error_message  TEXT,
    retry_count    INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_task_queue_status
    ON task_queue(status, priority DESC);

-- Sweep schedule state
CREATE TABLE IF NOT EXISTS sweep_schedule (
    sweep_name     TEXT    PRIMARY KEY,
    last_run_at    TEXT,
    last_status    TEXT,
    next_run_at    TEXT,
    items_fetched  INTEGER DEFAULT 0,
    error_message  TEXT
);

-- SAM.gov bulk entity data (stretch goal)
CREATE TABLE IF NOT EXISTS sam_entities (
    uei               TEXT PRIMARY KEY,
    legal_name        TEXT NOT NULL,
    dba_name          TEXT DEFAULT '',
    state             TEXT,
    city              TEXT,
    naics_codes       TEXT,
    entity_start_date TEXT,
    registration_status TEXT,
    cage_code         TEXT DEFAULT '',
    loaded_at         TEXT NOT NULL,
    bulk_file_date    TEXT
);
CREATE INDEX IF NOT EXISTS idx_sam_entities_state
    ON sam_entities(state);
CREATE INDEX IF NOT EXISTS idx_sam_entities_name
    ON sam_entities(legal_name);
"""


def _init_tables(conn: sqlite3.Connection) -> None:
    """Create all engine tables if they don't already exist."""
    conn.executescript(_SCHEMA_SQL)

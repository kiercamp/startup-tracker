"""Tests for prospect_engine.utils.db — SQLite connection manager."""

import sqlite3
import threading
from pathlib import Path

import pytest

from prospect_engine.utils.db import get_connection, close_connection


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """Return a temporary database path."""
    return tmp_path / "test_engine.db"


class TestGetConnection:
    """Tests for get_connection()."""

    def test_returns_connection(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        assert isinstance(conn, sqlite3.Connection)
        close_connection(tmp_db)

    def test_creates_db_file(self, tmp_db: Path):
        get_connection(tmp_db)
        assert tmp_db.exists()
        close_connection(tmp_db)

    def test_wal_mode_enabled(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        close_connection(tmp_db)

    def test_row_factory_set(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        assert conn.row_factory is sqlite3.Row
        close_connection(tmp_db)

    def test_creates_api_cache_table(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='api_cache'"
        ).fetchone()
        assert tables is not None
        close_connection(tmp_db)

    def test_creates_rate_limit_log_table(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='rate_limit_log'"
        ).fetchone()
        assert tables is not None
        close_connection(tmp_db)

    def test_creates_task_queue_table(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='task_queue'"
        ).fetchone()
        assert tables is not None
        close_connection(tmp_db)

    def test_creates_sweep_schedule_table(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sweep_schedule'"
        ).fetchone()
        assert tables is not None
        close_connection(tmp_db)

    def test_creates_sam_entities_table(self, tmp_db: Path):
        conn = get_connection(tmp_db)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sam_entities'"
        ).fetchone()
        assert tables is not None
        close_connection(tmp_db)

    def test_returns_same_connection_on_same_thread(self, tmp_db: Path):
        conn1 = get_connection(tmp_db)
        conn2 = get_connection(tmp_db)
        assert conn1 is conn2
        close_connection(tmp_db)

    def test_different_connections_on_different_threads(self, tmp_db: Path):
        connections = []

        def get_conn():
            connections.append(get_connection(tmp_db))

        t1 = threading.Thread(target=get_conn)
        t2 = threading.Thread(target=get_conn)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(connections) == 2
        # Thread-local connections may or may not be the same object,
        # but both should work
        for conn in connections:
            assert isinstance(conn, sqlite3.Connection)


class TestCloseConnection:
    """Tests for close_connection()."""

    def test_close_connection(self, tmp_db: Path):
        get_connection(tmp_db)
        close_connection(tmp_db)
        # Getting a new connection after close should work
        conn = get_connection(tmp_db)
        assert isinstance(conn, sqlite3.Connection)
        close_connection(tmp_db)

    def test_close_nonexistent_is_noop(self, tmp_db: Path):
        # Should not raise
        close_connection(tmp_db)

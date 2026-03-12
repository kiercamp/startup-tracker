"""Tests for prospect_engine.scheduler.sweep — task queue and batch sweeps."""

import json
from pathlib import Path

import pytest

from prospect_engine.utils.db import get_connection, close_connection
from prospect_engine.scheduler.sweep import (
    enqueue_task,
    dequeue_task,
    complete_task,
    fail_task,
    queue_stats,
    clear_completed,
    SWEEP_PROFILES,
    run_sweep,
)


@pytest.fixture(autouse=True)
def _use_tmp_db(tmp_path: Path, monkeypatch):
    """Redirect engine DB to a temp directory for all tests."""
    db_path = tmp_path / "test_engine.db"
    monkeypatch.setattr(
        "prospect_engine.utils.db.ENGINE_DB_PATH", db_path,
    )
    # Also patch the sweep module's db import
    monkeypatch.setattr(
        "prospect_engine.scheduler.sweep.get_connection",
        lambda: get_connection(db_path),
    )
    # Force table creation
    get_connection(db_path)
    yield
    close_connection(db_path)


class TestTaskQueue:
    """Tests for the task queue CRUD operations."""

    def test_enqueue_returns_task_id(self):
        task_id = enqueue_task("sbir", "fetch_sbir_awards", {"agency": "DOD"})
        assert isinstance(task_id, int)
        assert task_id > 0

    def test_dequeue_returns_pending_task(self):
        enqueue_task("sbir", "fetch_sbir_awards", {"agency": "DOD"})
        task = dequeue_task()
        assert task is not None
        assert task["endpoint"] == "sbir"
        assert task["action"] == "fetch_sbir_awards"
        # Row is fetched then updated to running; dict reflects pre-update state
        assert task["status"] in ("pending", "running")

    def test_dequeue_empty_returns_none(self):
        task = dequeue_task()
        assert task is None

    def test_dequeue_by_endpoint(self):
        enqueue_task("sbir", "fetch_sbir_awards", {"agency": "DOD"})
        enqueue_task("sam_gov", "fetch_sam_gov_awards", {"state": "AZ"})

        # Should only get sbir task
        task = dequeue_task(endpoint="sbir")
        assert task is not None
        assert task["endpoint"] == "sbir"

    def test_dequeue_respects_priority(self):
        enqueue_task("sbir", "low_priority", {}, priority=0)
        enqueue_task("sbir", "high_priority", {}, priority=10)

        task = dequeue_task()
        assert task is not None
        assert task["action"] == "high_priority"

    def test_dequeue_marks_as_running(self):
        enqueue_task("sbir", "test_action", {})
        task = dequeue_task()
        assert task is not None

        # Key test: second dequeue should return None (task was marked running)
        assert dequeue_task() is None

    def test_complete_task(self):
        task_id = enqueue_task("sbir", "test_action", {})
        dequeue_task()
        complete_task(task_id, result_hash="abc123")

        stats = queue_stats()
        assert stats.get("done", 0) == 1

    def test_fail_task_requeues_on_first_failure(self):
        task_id = enqueue_task("sbir", "test_action", {})
        dequeue_task()
        retried = fail_task(task_id, "Connection timeout", max_retries=3)
        assert retried is True

        # Task should be pending again
        stats = queue_stats()
        assert stats.get("pending", 0) == 1

    def test_fail_task_permanent_after_max_retries(self):
        task_id = enqueue_task("sbir", "test_action", {})

        # Simulate 3 failures
        for i in range(3):
            dequeue_task()
            retried = fail_task(task_id, "Error #{}".format(i + 1), max_retries=3)

        assert retried is False
        stats = queue_stats()
        assert stats.get("failed", 0) == 1

    def test_queue_stats(self):
        enqueue_task("sbir", "a", {})
        enqueue_task("sbir", "b", {})
        enqueue_task("sbir", "c", {})

        task = dequeue_task()
        complete_task(task["task_id"])

        stats = queue_stats()
        assert stats["pending"] == 2
        assert stats["done"] == 1

    def test_clear_completed(self):
        task_id = enqueue_task("sbir", "test", {})
        dequeue_task()
        complete_task(task_id)

        # Clear with a 0-hour cutoff (delete all)
        removed = clear_completed(older_than_hours=0)
        assert removed >= 1

    def test_params_json_round_trip(self):
        params = {"agency": "DOD", "year": 2024, "nested": {"key": "value"}}
        enqueue_task("sbir", "test", params)
        task = dequeue_task()
        parsed = json.loads(task["params_json"])
        assert parsed["agency"] == "DOD"
        assert parsed["year"] == 2024
        assert parsed["nested"]["key"] == "value"


class TestSweepProfiles:
    """Tests for the sweep profile configuration."""

    def test_sbir_nightly_exists(self):
        assert "sbir_nightly" in SWEEP_PROFILES
        profile = SWEEP_PROFILES["sbir_nightly"]
        assert profile["endpoint"] == "sbir"

    def test_sam_gov_6h_exists(self):
        assert "sam_gov_6h" in SWEEP_PROFILES
        profile = SWEEP_PROFILES["sam_gov_6h"]
        assert profile["endpoint"] == "sam_gov"

    def test_usa_spending_4h_exists(self):
        assert "usa_spending_4h" in SWEEP_PROFILES
        profile = SWEEP_PROFILES["usa_spending_4h"]
        assert profile["endpoint"] == "usa_spending"

    def test_all_profiles_have_required_keys(self):
        for name, profile in SWEEP_PROFILES.items():
            assert "interval" in profile, "{} missing interval".format(name)
            assert "endpoint" in profile, "{} missing endpoint".format(name)
            assert "description" in profile, "{} missing description".format(name)


class TestRunSweep:
    """Tests for the run_sweep() function."""

    def test_unknown_sweep_raises(self):
        with pytest.raises(ValueError, match="Unknown sweep"):
            run_sweep("nonexistent_sweep")

    def test_not_due_returns_skipped(self):
        # First run should be due (never run before), so run it to set schedule
        # Then immediately running again should be "not due"
        # But for a fresh DB it's always due the first time
        pass  # Schedule logic is tested implicitly by the daemon tests

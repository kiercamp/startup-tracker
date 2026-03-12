"""Batch sweep scheduler and persistent task queue.

Sweeps replace reactive per-company API calls with scheduled batch fetches
that run within rate limits.  Each sweep enqueues tasks into the SQLite
``task_queue`` table; the orchestrator dequeues and processes them one at a
time through the rate limiter.

Three sweep profiles are defined:

| Sweep              | Interval | What it fetches                          |
|--------------------|----------|------------------------------------------|
| ``sbir_nightly``   | 24 h     | SBIR awards by agency × year             |
| ``sam_gov_6h``     | 6 h      | SAM.gov contract awards by state × NAICS |
| ``usa_spending_4h``| 4 h      | USASpending recent obligations by state   |

Usage::

    # Run all due sweeps once (then exit):
    python main.py --sweep

    # Run a specific sweep now (ignore schedule):
    python main.py --sweep sbir

    # Start as a background daemon:
    python main.py --sweep-daemon
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

from prospect_engine.config import (
    TARGET_STATES,
    TARGET_NAICS,
    LOOKBACK_YEARS,
    MIN_AWARD_AMOUNT,
)
from prospect_engine.utils.db import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sweep definitions — interval, endpoint, and task generator functions
# ---------------------------------------------------------------------------

SWEEP_PROFILES: Dict[str, Dict[str, Any]] = {
    "sbir_nightly": {
        "interval": timedelta(hours=24),
        "endpoint": "sbir",
        "description": "SBIR/STTR awards by agency and year",
    },
    "sam_gov_6h": {
        "interval": timedelta(hours=6),
        "endpoint": "sam_gov",
        "description": "SAM.gov contract awards by state",
    },
    "usa_spending_4h": {
        "interval": timedelta(hours=4),
        "endpoint": "usa_spending",
        "description": "USASpending obligations by state",
    },
}


# ---------------------------------------------------------------------------
# Task Queue helpers
# ---------------------------------------------------------------------------

def enqueue_task(
    endpoint: str,
    action: str,
    params: Dict[str, Any],
    priority: int = 0,
) -> int:
    """Insert a task into the persistent task queue.

    Args:
        endpoint: Logical API name (``"sbir"``, ``"sam_gov"``, etc.).
        action: Descriptive action tag (``"fetch_sbir_awards"``, etc.).
        params: Parameters dict — serialized as JSON.
        priority: Higher = processed first (default 0).

    Returns:
        The ``task_id`` of the newly created row.
    """
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    cursor = conn.execute(
        "INSERT INTO task_queue (endpoint, action, params_json, status, priority, created_at) "
        "VALUES (?, ?, ?, 'pending', ?, ?)",
        (endpoint, action, json.dumps(params, default=str), priority, now),
    )
    conn.commit()
    task_id = cursor.lastrowid
    logger.debug("Enqueued task %d: %s/%s", task_id, endpoint, action)
    return task_id


def dequeue_task(endpoint: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Dequeue the highest-priority pending task, marking it as running.

    If *endpoint* is given, only tasks for that endpoint are considered.

    Returns:
        Task row as a dict, or ``None`` if the queue is empty.
    """
    conn = get_connection()
    if endpoint:
        row = conn.execute(
            "SELECT * FROM task_queue WHERE status = 'pending' AND endpoint = ? "
            "ORDER BY priority DESC, task_id ASC LIMIT 1",
            (endpoint,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM task_queue WHERE status = 'pending' "
            "ORDER BY priority DESC, task_id ASC LIMIT 1",
        ).fetchone()

    if row is None:
        return None

    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE task_queue SET status = 'running', started_at = ? WHERE task_id = ?",
        (now, row["task_id"]),
    )
    conn.commit()
    return dict(row)


def complete_task(task_id: int, result_hash: str = "") -> None:
    """Mark a task as done."""
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE task_queue SET status = 'done', completed_at = ?, result_hash = ? "
        "WHERE task_id = ?",
        (now, result_hash, task_id),
    )
    conn.commit()


def fail_task(task_id: int, error_message: str, max_retries: int = 3) -> bool:
    """Mark a task as failed, or re-queue it for retry.

    Args:
        task_id: The task row ID.
        error_message: Short description of the failure.
        max_retries: Maximum retry count before permanent failure.

    Returns:
        ``True`` if the task was re-queued, ``False`` if permanently failed.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT retry_count FROM task_queue WHERE task_id = ?", (task_id,)
    ).fetchone()

    if row is None:
        return False

    retry_count = row["retry_count"] + 1
    if retry_count >= max_retries:
        conn.execute(
            "UPDATE task_queue SET status = 'failed', error_message = ?, "
            "retry_count = ?, completed_at = ? WHERE task_id = ?",
            (error_message, retry_count, datetime.utcnow().isoformat(), task_id),
        )
        conn.commit()
        logger.warning("Task %d permanently failed after %d retries: %s",
                        task_id, retry_count, error_message[:80])
        return False
    else:
        conn.execute(
            "UPDATE task_queue SET status = 'pending', error_message = ?, "
            "retry_count = ? WHERE task_id = ?",
            (error_message, retry_count, task_id),
        )
        conn.commit()
        logger.info("Task %d re-queued (retry %d/%d): %s",
                     task_id, retry_count, max_retries, error_message[:80])
        return True


def queue_stats() -> Dict[str, int]:
    """Return counts of tasks grouped by status."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT status, COUNT(*) AS cnt FROM task_queue GROUP BY status"
    ).fetchall()
    return {row["status"]: row["cnt"] for row in rows}


def clear_completed(older_than_hours: int = 24) -> int:
    """Delete completed tasks older than *older_than_hours*.

    Returns:
        Number of rows deleted.
    """
    cutoff = (datetime.utcnow() - timedelta(hours=older_than_hours)).isoformat()
    conn = get_connection()
    cursor = conn.execute(
        "DELETE FROM task_queue WHERE status = 'done' AND completed_at < ?",
        (cutoff,),
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Sweep schedule helpers
# ---------------------------------------------------------------------------

def _is_sweep_due(sweep_name: str) -> bool:
    """Check whether a sweep is due based on its schedule."""
    profile = SWEEP_PROFILES.get(sweep_name)
    if profile is None:
        return False

    conn = get_connection()
    row = conn.execute(
        "SELECT next_run_at FROM sweep_schedule WHERE sweep_name = ?",
        (sweep_name,),
    ).fetchone()

    now = datetime.utcnow()
    if row is None:
        # Never run before — it's due
        return True

    next_run = row["next_run_at"]
    if next_run is None:
        return True

    return now.isoformat() >= next_run


def _update_sweep_schedule(
    sweep_name: str,
    status: str,
    items_fetched: int = 0,
    error_message: str = "",
) -> None:
    """Update the sweep_schedule table after a sweep finishes."""
    profile = SWEEP_PROFILES.get(sweep_name, {})
    interval = profile.get("interval", timedelta(hours=24))

    now = datetime.utcnow()
    next_run = now + interval

    conn = get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO sweep_schedule "
        "(sweep_name, last_run_at, last_status, next_run_at, items_fetched, error_message) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            sweep_name,
            now.isoformat(),
            status,
            next_run.isoformat(),
            items_fetched,
            error_message[:500] if error_message else "",
        ),
    )
    conn.commit()
    logger.info(
        "Sweep '%s' finished: status=%s, items=%d, next_run=%s",
        sweep_name, status, items_fetched, next_run.isoformat(),
    )


# ---------------------------------------------------------------------------
# Task generators — create tasks for each sweep type
# ---------------------------------------------------------------------------

def _generate_sbir_tasks() -> int:
    """Enqueue SBIR fetch tasks: one per agency × year."""
    agencies = ["DOD", "NASA"]
    current_year = date.today().year
    start_year = current_year - min(LOOKBACK_YEARS, 3)

    count = 0
    for agency in agencies:
        for year in range(start_year, current_year + 1):
            enqueue_task(
                endpoint="sbir",
                action="fetch_sbir_awards",
                params={"agency": agency, "year": year},
                priority=1 if year == current_year else 0,
            )
            count += 1
    logger.info("SBIR sweep: enqueued %d tasks", count)
    return count


def _generate_sam_gov_tasks() -> int:
    """Enqueue SAM.gov fetch tasks: one per state."""
    days = LOOKBACK_YEARS * 365
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    date_range = "[{},{}]".format(
        start_date.strftime("%m/%d/%Y"),
        end_date.strftime("%m/%d/%Y"),
    )
    naics_tilde = "~".join(TARGET_NAICS)

    count = 0
    for state in TARGET_STATES:
        enqueue_task(
            endpoint="sam_gov",
            action="fetch_sam_gov_awards",
            params={
                "state": state,
                "naics_tilde": naics_tilde,
                "date_range": date_range,
            },
            priority=0,
        )
        count += 1
    logger.info("SAM.gov sweep: enqueued %d tasks", count)
    return count


def _generate_usa_spending_tasks() -> int:
    """Enqueue USASpending fetch tasks: recent 30-day window."""
    end_date = date.today()
    # For scheduled sweeps, use a 30-day window (not full lookback)
    start_date = end_date - timedelta(days=30)

    enqueue_task(
        endpoint="usa_spending",
        action="fetch_usa_spending",
        params={
            "states": TARGET_STATES,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        priority=0,
    )
    logger.info("USASpending sweep: enqueued 1 task")
    return 1


_TASK_GENERATORS: Dict[str, Callable[[], int]] = {
    "sbir_nightly": _generate_sbir_tasks,
    "sam_gov_6h": _generate_sam_gov_tasks,
    "usa_spending_4h": _generate_usa_spending_tasks,
}


# ---------------------------------------------------------------------------
# Task executors — process individual tasks through the rate limiter
# ---------------------------------------------------------------------------

def _execute_sbir_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single SBIR fetch task.

    Returns:
        Parsed response data dict.
    """
    from prospect_engine.utils.http import get_with_retry
    from prospect_engine.utils.cache import get_cache

    agency = params["agency"]
    year = params["year"]

    cache = get_cache()
    cache_key = {"endpoint": "sbir", "agency": agency, "year": year, "offset": 0}
    cached = cache.get("sbir", cache_key)
    if cached is not None:
        return json.loads(cached)

    base_url = "https://api.www.sbir.gov/public/api/awards"
    from prospect_engine.config import SBIR_PAGE_SIZE
    response = get_with_retry(
        base_url,
        params={"agency": agency, "year": str(year), "rows": SBIR_PAGE_SIZE, "start": 0},
        timeout=30.0,
        max_retries=1,
        endpoint="sbir",
    )
    data = response.json()
    cache.put("sbir", cache_key, json.dumps(data))
    return data


def _execute_sam_gov_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single SAM.gov fetch task.

    Returns:
        Parsed response data dict.
    """
    from prospect_engine.utils.http import get_with_retry
    from prospect_engine.utils.cache import get_cache
    from prospect_engine.config import _get_secret, SAM_GOV_PAGE_SIZE

    api_key = _get_secret("SAM_GOV_API_KEY")
    if not api_key:
        raise ValueError("SAM_GOV_API_KEY not set")

    state = params["state"]
    naics_tilde = params["naics_tilde"]
    date_range = params["date_range"]

    cache = get_cache()
    cache_key = {"endpoint": "sam_gov", "state": state, "naics": naics_tilde,
                 "date_range": date_range, "offset": 0}
    cached = cache.get("sam_gov", cache_key)
    if cached is not None:
        return json.loads(cached)

    base_url = "https://api.sam.gov/contract-awards/v1/search"
    response = get_with_retry(
        base_url,
        params={
            "api_key": api_key,
            "awardeeStateCode": state,
            "naicsCode": naics_tilde,
            "dateSigned": date_range,
            "limit": SAM_GOV_PAGE_SIZE,
            "offset": 0,
        },
        timeout=30.0,
        endpoint="sam_gov",
    )
    data = response.json()
    cache.put("sam_gov", cache_key, json.dumps(data))
    return data


def _execute_usa_spending_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single USASpending fetch task.

    Returns:
        Parsed response data dict.
    """
    from prospect_engine.utils.http import post_with_retry
    from prospect_engine.utils.cache import get_cache
    from prospect_engine.config import USASPENDING_PAGE_SIZE, USASPENDING_AWARD_UPPER_BOUND

    states = params["states"]
    start_date = params["start_date"]
    end_date = params["end_date"]

    cache = get_cache()
    cache_key = {"endpoint": "usa_spending", "states": sorted(states),
                 "start": start_date, "end": end_date, "page": 1}
    cached = cache.get("usa_spending", cache_key)
    if cached is not None:
        return json.loads(cached)

    base_url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
    filters: Dict[str, Any] = {
        "award_type_codes": ["A", "B", "C", "D"],
        "recipient_locations": [{"country": "USA", "state": s} for s in states],
        "time_period": [{"start_date": start_date, "end_date": end_date}],
    }
    if USASPENDING_AWARD_UPPER_BOUND > 0:
        filters["award_amounts"] = [
            {"lower_bound": 0, "upper_bound": USASPENDING_AWARD_UPPER_BOUND}
        ]

    body = {
        "filters": filters,
        "fields": [
            "Award ID", "Recipient Name", "Start Date", "End Date",
            "Award Amount", "Awarding Agency", "Awarding Sub Agency",
            "NAICS Code", "NAICS Description", "generated_internal_id",
        ],
        "page": 1,
        "limit": USASPENDING_PAGE_SIZE,
        "sort": "Start Date",
        "order": "desc",
        "subawards": False,
    }

    response = post_with_retry(
        base_url, json=body, timeout=60.0, endpoint="usa_spending",
    )
    data = response.json()
    cache.put("usa_spending", cache_key, json.dumps(data))
    return data


_TASK_EXECUTORS: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
    "fetch_sbir_awards": _execute_sbir_task,
    "fetch_sam_gov_awards": _execute_sam_gov_task,
    "fetch_usa_spending": _execute_usa_spending_task,
}


# ---------------------------------------------------------------------------
# Orchestrator — dequeue and execute tasks
# ---------------------------------------------------------------------------

def process_queue(
    max_tasks: int = 100,
    endpoint: Optional[str] = None,
) -> Dict[str, int]:
    """Process pending tasks from the queue through rate limiters.

    Dequeues tasks one at a time, executes through the appropriate executor,
    and marks them done or failed.  Stops after *max_tasks* or when the queue
    is empty.

    Args:
        max_tasks: Maximum number of tasks to process in this run.
        endpoint: If set, only process tasks for this endpoint.

    Returns:
        Dict with counts: ``{"processed": N, "succeeded": N, "failed": N}``.
    """
    stats = {"processed": 0, "succeeded": 0, "failed": 0}

    for _ in range(max_tasks):
        task = dequeue_task(endpoint=endpoint)
        if task is None:
            break  # Queue empty

        task_id = task["task_id"]
        action = task["action"]
        params = json.loads(task["params_json"])
        stats["processed"] += 1

        executor = _TASK_EXECUTORS.get(action)
        if executor is None:
            fail_task(task_id, "Unknown action: {}".format(action), max_retries=1)
            stats["failed"] += 1
            continue

        try:
            executor(params)
            complete_task(task_id)
            stats["succeeded"] += 1
        except Exception as exc:
            retried = fail_task(task_id, str(exc)[:200])
            if not retried:
                stats["failed"] += 1
            logger.warning(
                "Task %d (%s) failed: %s", task_id, action, str(exc)[:80],
            )

    logger.info(
        "Queue processing complete: %d processed, %d succeeded, %d failed",
        stats["processed"], stats["succeeded"], stats["failed"],
    )
    return stats


# ---------------------------------------------------------------------------
# Sweep runner — enqueue + process
# ---------------------------------------------------------------------------

def run_sweep(
    sweep_name: str,
    force: bool = False,
) -> Dict[str, Any]:
    """Run a single sweep: enqueue tasks, process them, update schedule.

    Args:
        sweep_name: Name from :data:`SWEEP_PROFILES` (e.g. ``"sbir_nightly"``).
        force: Run even if the sweep isn't due yet.

    Returns:
        Dict with ``status``, ``tasks_enqueued``, and processing ``stats``.

    Raises:
        ValueError: If *sweep_name* is not a known sweep.
    """
    if sweep_name not in SWEEP_PROFILES:
        raise ValueError(
            "Unknown sweep {!r}. Available: {}".format(
                sweep_name, list(SWEEP_PROFILES.keys())
            )
        )

    if not force and not _is_sweep_due(sweep_name):
        logger.info("Sweep '%s' is not due yet — skipping", sweep_name)
        return {"status": "skipped", "tasks_enqueued": 0, "stats": {}}

    profile = SWEEP_PROFILES[sweep_name]
    endpoint = profile["endpoint"]
    generator = _TASK_GENERATORS.get(sweep_name)

    if generator is None:
        raise ValueError("No task generator for sweep {!r}".format(sweep_name))

    logger.info("Starting sweep '%s' — %s", sweep_name, profile["description"])

    try:
        tasks_enqueued = generator()
        stats = process_queue(max_tasks=tasks_enqueued + 10, endpoint=endpoint)

        status = "success" if stats["failed"] == 0 else "partial"
        _update_sweep_schedule(
            sweep_name,
            status=status,
            items_fetched=stats["succeeded"],
        )
        return {"status": status, "tasks_enqueued": tasks_enqueued, "stats": stats}
    except Exception as exc:
        _update_sweep_schedule(
            sweep_name,
            status="failed",
            error_message=str(exc)[:200],
        )
        logger.exception("Sweep '%s' failed", sweep_name)
        return {"status": "failed", "tasks_enqueued": 0, "stats": {}, "error": str(exc)[:200]}


def run_all_due_sweeps() -> Dict[str, Any]:
    """Run all sweeps that are due according to their schedule.

    Returns:
        Dict mapping sweep name to its run result.
    """
    results = {}
    for sweep_name in SWEEP_PROFILES:
        results[sweep_name] = run_sweep(sweep_name, force=False)
    return results


# ---------------------------------------------------------------------------
# Daemon scheduler — background thread
# ---------------------------------------------------------------------------

class SweepDaemon:
    """Background daemon that checks for and runs due sweeps.

    Runs in a daemon thread; checks every *check_interval* seconds whether
    any sweeps are due, then runs them.

    Args:
        check_interval: Seconds between schedule checks (default 60).
    """

    def __init__(self, check_interval: float = 60.0) -> None:
        self._check_interval = check_interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("Sweep daemon already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="sweep-daemon",
            daemon=True,
        )
        self._thread.start()
        logger.info("Sweep daemon started (check every %.0fs)", self._check_interval)

    def stop(self, timeout: float = 10.0) -> None:
        """Signal the daemon to stop and wait for it to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            logger.info("Sweep daemon stopped")

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run_loop(self) -> None:
        """Main loop: check schedule, run due sweeps, sleep."""
        logger.info("Sweep daemon loop started")
        while not self._stop_event.is_set():
            try:
                results = run_all_due_sweeps()
                ran = {k: v for k, v in results.items() if v.get("status") != "skipped"}
                if ran:
                    logger.info("Daemon sweep cycle complete: %s", ran)
            except Exception:
                logger.exception("Error in sweep daemon cycle")

            self._stop_event.wait(timeout=self._check_interval)

        logger.info("Sweep daemon loop exiting")


# Module-level daemon singleton
_daemon: Optional[SweepDaemon] = None


def start_daemon(check_interval: float = 60.0) -> SweepDaemon:
    """Start the global sweep daemon (singleton).

    Returns:
        The :class:`SweepDaemon` instance.
    """
    global _daemon
    if _daemon is None or not _daemon.is_running:
        _daemon = SweepDaemon(check_interval=check_interval)
        _daemon.start()
    return _daemon


def stop_daemon() -> None:
    """Stop the global sweep daemon if running."""
    global _daemon
    if _daemon is not None:
        _daemon.stop()
        _daemon = None

"""Retry policy on the cross-queue / distributed activity path (§6.1.2).

The ``__temporal_activity`` workflow owns the retry loop on the activity worker
(Design A): attempts, durable backoff, ``maximum_attempts`` / non-retryable, and
the schedule-to-close budget all run there, reusing ``activities.retry_decision``
(shared with the local path). These tests cover the happy retry path, the
non-retryable fast-fail, and — the recovery gate — a SIGKILL of the activity
worker mid-backoff that must resume the retry sequence at the right attempt
rather than restarting it.
"""

import time
from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_retry_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str, effects: Path, **extra: str) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "DBOSIFY_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "DBOSIFY_TEST_EFFECTS": str(effects),
        "DBOS__VMID": vmid,
        **extra,
    }


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_queued_activity_retries_until_success(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    activity_worker = PythonProcess(
        WORKER,
        "activity",
        env=_env("dbosify-act", effects, DBOSIFY_TEST_SUCCEED_AT="3"),
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER, "workflow", "start", "xq-retry-ok", env=_env("dbosify-wf", effects)
        )
        workflow_worker.start()
        try:
            line = workflow_worker.wait_for_line("RESULT ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    # Failed on attempts 1 and 2, succeeded on attempt 3 — exactly three runs.
    assert effects.read_text() == "1\n2\n3\n"


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_queued_activity_non_retryable_fails_fast(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    activity_worker = PythonProcess(
        WORKER,
        "activity",
        env=_env("dbosify-act", effects, DBOSIFY_TEST_NON_RETRYABLE="1"),
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER, "workflow", "start", "xq-retry-nr", env=_env("dbosify-wf", effects)
        )
        workflow_worker.start()
        try:
            line = workflow_worker.wait_for_line("FAILED ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("FAILED ", 1)[1].strip() == "ActivityError"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    # A non-retryable failure runs the activity exactly once.
    assert effects.read_text() == "1\n"


@pytest.mark.timeout(180)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_activity_worker_mid_backoff_resumes(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    succeed_at = "5"

    activity_worker = PythonProcess(
        WORKER,
        "activity",
        env=_env("dbosify-act", effects, DBOSIFY_TEST_SUCCEED_AT=succeed_at),
    )
    activity_worker.start()
    workflow_worker = PythonProcess(
        WORKER, "workflow", "start", "xq-retry-recover", env=_env("dbosify-wf", effects)
    )
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker.start()
        # Let a couple of attempts fail, then kill during the (2s) backoff after
        # attempt 2 — a brief pause lands the kill inside the durable sleep.
        activity_worker.wait_for_line("ATTEMPT 2", timeout=90)
        time.sleep(0.6)
        activity_worker.sigkill()
        assert activity_worker.wait(timeout=30) == -9

        revived = PythonProcess(
            WORKER,
            "activity",
            env=_env("dbosify-act", effects, DBOSIFY_TEST_SUCCEED_AT=succeed_at),
        )
        revived.start()
        try:
            revived.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
            line = workflow_worker.wait_for_line("RESULT ", timeout=120)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            revived.terminate_and_wait()
    finally:
        workflow_worker.terminate_and_wait()
        activity_worker.terminate_and_wait()

    # The retry sequence resumed across the kill rather than restarting: it
    # reached attempt 5, and attempt 1 (recorded long before the kill) ran once.
    attempts = [int(x) for x in effects.read_text().split()]
    assert attempts[-1] == 5
    assert attempts.count(1) == 1
    assert max(attempts) == 5

"""Cooperative cancellation of a cross-queue activity (Phase 3, §6.1.2).

The activity runs on a different worker than the workflow, so cancellation must
cross the process boundary: the interpreter natively cancels the internal
``__temporal_activity`` workflow (status-guarded, like a child terminate), which
surfaces inside the async activity as an ``asyncio.CancelledError``. The activity
records ``cancelled`` in its ``except`` block — proof the cancellation reached
it on its own worker — and the workflow observes the activity as cancelled.
"""

from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_cancel_worker.py"
REPO_ROOT = Path(__file__).parents[2]
APP_VERSION = "tdb-xq-cancel"


def _env(vmid: str, effects: Path, **extra: str) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "TDB_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "TDB_TEST_EFFECTS": str(effects),
        "DBOS__APPVERSION": APP_VERSION,
        "DBOS__VMID": vmid,
        **extra,
    }


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("cancel_type", ["try", "wait"])
def test_cross_queue_activity_is_cancelled(tmp_path: Path, cancel_type: str) -> None:
    effects = tmp_path / "effects"
    activity_worker = PythonProcess(WORKER, "activity", env=_env("tdb-act", effects))
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER,
            "workflow",
            "start",
            "xq-cancel-wf",
            env=_env("tdb-wf", effects, TDB_TEST_CANCEL_TYPE=cancel_type),
        )
        workflow_worker.start()
        try:
            # The activity really started on its own worker before we cancel.
            activity_worker.wait_for_line("ACTIVITY_STARTED", timeout=90)
            line = workflow_worker.wait_for_line("RESULT ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "activity-cancelled"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    # The cancellation crossed the process boundary: the activity ran its
    # cancellation cleanup on its worker.
    assert effects.read_text() == "started\ncancelled\n"

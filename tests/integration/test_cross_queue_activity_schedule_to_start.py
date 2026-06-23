"""schedule_to_start on the cross-queue activity path.

schedule_to_start bounds how long a queued activity may sit before a worker
starts it. The test enqueues the activity (workflow worker up) but delays the
activity worker past the budget, so the activity workflow fails with a
SCHEDULE_TO_START timeout before running any attempt.
"""

import time
from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_sts_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "DBOSIFY_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "DBOS__VMID": vmid,
    }


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_schedule_to_start_timeout(tmp_path: Path) -> None:
    # The activity can only be enqueued once its queue is registered in the DB.
    # Register it with a worker, then stop it so the activity dwells unconsumed.
    registrar = PythonProcess(WORKER, "activity", env=_env("dbosify-act"))
    registrar.start()
    registrar.wait_for_line("ACTIVITY_WORKER_READY", timeout=60)
    registrar.terminate_and_wait()

    workflow_worker = PythonProcess(
        WORKER, "workflow", "start", "xq-sts-wf", env=_env("dbosify-wf")
    )
    workflow_worker.start()
    try:
        workflow_worker.wait_for_line("STARTED", timeout=60)
        # Let the activity dwell past the 2s schedule_to_start budget.
        time.sleep(4)

        activity_worker = PythonProcess(WORKER, "activity", env=_env("dbosify-act"))
        activity_worker.start()
        try:
            activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=60)
            line = workflow_worker.wait_for_line("RESULT ", timeout=60)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "timeout:SCHEDULE_TO_START"
        finally:
            activity_worker.terminate_and_wait()
    finally:
        workflow_worker.terminate_and_wait()

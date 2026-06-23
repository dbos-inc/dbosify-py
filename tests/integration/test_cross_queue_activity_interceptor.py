"""Activity interceptors fire on the cross-queue path.

The activity runs on a different worker process than the calling workflow, and
that activity worker carries its own ``Worker(interceptors=[...])``. The
interceptor transforms the activity result, and the transform surfaces in the
workflow result — proving the chain is built and run by whichever worker
executes the activity, not only on the local path.
"""

from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_interceptor_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "DBOSIFY_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "DBOS__VMID": vmid,
    }


@pytest.mark.timeout(120)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_activity_interceptor_runs_on_activity_worker() -> None:
    activity_worker = PythonProcess(WORKER, "activity", env=_env("dbosify-act"))
    activity_worker.start()
    workflow_worker = PythonProcess(
        WORKER, "workflow", "xq-ic-wf", env=_env("dbosify-wf")
    )
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker.start()
        line = workflow_worker.wait_for_line("RESULT ", timeout=120)
        assert workflow_worker.wait(timeout=30) == 0
        # The activity worker's own interceptor transformed the result on the
        # queued path.
        assert line.split("RESULT ", 1)[1].strip() == "xq-intercepted(Hello, Temporal!)"
    finally:
        workflow_worker.terminate_and_wait()
        activity_worker.terminate_and_wait()

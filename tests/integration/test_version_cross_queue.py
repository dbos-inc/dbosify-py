"""Cross-queue (queued) activities under a build id (DEVIATIONS D29). A
cross-queue activity is enqueued in-workflow as a ``__temporal_activity``
workflow, stamped with the workflow worker's build id; only an activity worker
on the same build id dequeues it. With both workers on ``cq-build`` the activity
runs to completion and its workflow row carries that build id.
"""

from pathlib import Path
from typing import Dict

import pytest
from dbos import DBOSClient

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "version_cross_queue_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(build_id: str) -> Dict[str, str]:
    return {"PYTHONPATH": str(REPO_ROOT), "TDB_BUILD_ID": build_id}


@pytest.mark.usefixtures("cleanup_test_databases")
def test_cross_queue_activity_pinned_to_build_id() -> None:
    wf_id = "version-xq-wf"
    activity_worker = PythonProcess(WORKER, "activity", env=_env("cq-build"))
    activity_worker.start()
    workflow_worker = PythonProcess(WORKER, "workflow", wf_id, env=_env("cq-build"))
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker.start()
        result_line = workflow_worker.wait_for_line("RESULT ", timeout=120)
        assert workflow_worker.wait() == 0
    finally:
        workflow_worker.terminate_and_wait()
        activity_worker.terminate_and_wait()

    assert result_line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"

    # The queued __temporal_activity workflow (id "<wf>--a<seq>") was stamped
    # with the workflow worker's build id and dequeued by the matching-version
    # activity worker — that match is what let it run at all.
    probe = DBOSClient(system_database_url=system_database_url())
    try:
        activity_rows = probe.list_workflows(workflow_id_prefix=f"{wf_id}--a")
    finally:
        probe.destroy()
    assert len(activity_rows) == 1
    assert activity_rows[0].app_version == "cq-build"

"""SIGKILL recovery for the cross-queue / distributed activity path (§6.1.2).

The activity runs on a *different* worker process than the workflow that calls
it (via ``execute_activity(..., task_queue=)``), so two failure modes matter and
each gets a test:

  * kill the **activity worker** mid-activity — a fresh activity worker recovers
    the orphaned ``__temporal_activity`` workflow and the parent still
    completes (at-least-once activity execution, Temporal semantics);
  * kill the **workflow worker** while it awaits the activity — the parent
    re-attaches to the *same* activity workflow id on recovery (idempotent
    ``SetWorkflowID`` enqueue, no twin), and the activity ran exactly once.

Both keep the partner worker alive across the kill. Recovery correctness is the
product (CLAUDE.md), so these — not just the happy path — gate the feature.
"""

from pathlib import Path

import pytest
from dbos import DBOSClient

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str, effects: Path) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "TDB_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "TDB_TEST_EFFECTS": str(effects),
        "DBOS__VMID": vmid,
    }


@pytest.mark.timeout(180)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_activity_worker_mid_activity_recovers(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "xq-kill-activity-wf"

    activity_worker = PythonProcess(WORKER, "activity", env=_env("tdb-act", effects))
    activity_worker.start()
    workflow_worker = PythonProcess(
        WORKER, "workflow", "start", wf_id, env=_env("tdb-wf", effects)
    )
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker.start()
        # The activity has begun on the activity worker (mid its sleep).
        activity_worker.wait_for_line("ACTIVITY_RUNNING", timeout=90)
        activity_worker.sigkill()
        assert activity_worker.wait(timeout=30) == -9

        # A fresh activity worker (same vmid) recovers the orphaned activity
        # workflow and re-runs it to completion.
        revived = PythonProcess(WORKER, "activity", env=_env("tdb-act", effects))
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


@pytest.mark.timeout(180)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_workflow_worker_reattaches_to_activity(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "xq-kill-workflow-wf"

    activity_worker = PythonProcess(WORKER, "activity", env=_env("tdb-act", effects))
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)

        first = PythonProcess(
            WORKER, "workflow", "start", wf_id, env=_env("tdb-wf", effects)
        )
        first.start()
        try:
            # Kill the parent while the activity is in flight on the (still
            # alive) activity worker.
            activity_worker.wait_for_line("ACTIVITY_RUNNING", timeout=90)
            first.sigkill()
            assert first.wait(timeout=30) == -9
        finally:
            first.terminate_and_wait()

        # The activity finishes on the surviving activity worker.
        activity_worker.wait_for_line("ACTIVITY_DONE", timeout=90)

        # A new workflow worker re-attaches to the same activity and resolves.
        second = PythonProcess(
            WORKER, "workflow", "resume", wf_id, env=_env("tdb-wf", effects)
        )
        second.start()
        try:
            line = second.wait_for_line("RESULT ", timeout=120)
            assert second.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            second.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    # The activity executed exactly once (re-attach, not re-dispatch).
    assert effects.read_text() == "ran\n"

    # Exactly one activity workflow exists: {parent}--a{seq}, no twin.
    client = DBOSClient(system_database_url=system_database_url())
    try:
        rows = client.list_workflows(workflow_id_prefix=f"{wf_id}--a")
        assert [r.workflow_id for r in rows] == [f"{wf_id}--a1"]
    finally:
        client.destroy()

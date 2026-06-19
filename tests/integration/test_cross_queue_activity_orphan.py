"""A cross-queue activity must not outlive its workflow (§6.1.2).

When a workflow continues-as-new (or completes / is cancelled) with a
fire-and-forget cross-queue activity still in flight, the close path must
deliver cancellation to that activity on its own worker — otherwise the
``__temporal_activity`` workflow keeps running to completion, orphaned, and its
side effects execute after the parent has ended. The same close loop handles
continue-as-new, normal completion, and cooperative cancel; this exercises the
continue-as-new case.
"""

from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "cross_queue_orphan_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env(vmid: str, effects: Path) -> "dict[str, str]":
    return {
        "PYTHONPATH": str(REPO_ROOT),
        "DBOSIFY_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        "DBOSIFY_TEST_EFFECTS": str(effects),
        "DBOS__VMID": vmid,
    }


@pytest.mark.timeout(120)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_continue_as_new_cancels_pending_cross_queue_activity(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    activity_worker = PythonProcess(
        WORKER, "activity", env=_env("dbosify-act", effects)
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER, "workflow", "start", "xq-orphan-wf", env=_env("dbosify-wf", effects)
        )
        workflow_worker.start()
        try:
            activity_worker.wait_for_line("ACTIVITY_STARTED", timeout=90)
            # The CAN close cancels the activity within a few seconds (else it
            # would sleep its full 60s and print ACTIVITY_COMPLETED, orphaned).
            activity_worker.wait_for_line("ACTIVITY_CANCELLED", timeout=30)
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    text = effects.read_text()
    assert "cancelled\n" in text
    assert "completed\n" not in text

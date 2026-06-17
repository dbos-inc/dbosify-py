"""Cooperative cancellation of a cross-queue activity (Phase 3, §6.1.2).

The activity runs on a different worker than the workflow, so cancellation must
cross the process boundary *cooperatively*: the interpreter sets a checkpointed
cancel event on the activity's run (and, for an async-parked activity, a marker
on its completion topic); the activity's attempt step polls that event on its
own worker and delivers an ``asyncio.CancelledError`` into the activity. (No
native DBOS cancel of the ``__temporal_activity`` workflow is involved.) The
activity records ``cancelled`` in its ``except`` block — proof the cancellation
reached it on its own worker — and the workflow observes the activity as
cancelled.
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
            # TRY_CANCEL resolves the workflow without waiting for the activity's
            # cross-process cleanup, so the "cancelled" effect can land after the
            # RESULT. Synchronize on the activity worker actually running its
            # cleanup before tearing it down and reading the effects file.
            activity_worker.wait_for_line("ACTIVITY_CANCELLED", timeout=30)
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    # The cancellation crossed the process boundary: the activity ran its
    # cancellation cleanup on its worker.
    assert effects.read_text() == "started\ncancelled\n"


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("cancel_type", ["try", "wait"])
def test_cross_queue_activity_cancelled_before_dispatch(
    tmp_path: Path, cancel_type: str
) -> None:
    """Regression: cancel a cross-queue activity *before* its dispatch commits
    (no awaiting boundary between start and cancel), so ``queued_dbos_id`` is
    still unset when the cancellation sweep runs.

    The cancel must still take effect rather than being silently dropped:
      * TRY_CANCEL cancels the awaiting future, retiring the exec before the
        dispatch runs — the activity is never enqueued (it leaves no effects);
      * WAIT_CANCELLATION_COMPLETED keeps the exec open, so the cross-process
        cancel is deferred to dispatch time and delivered once the activity
        workflow exists — the activity starts on its worker and unwinds.

    Before the fix, TRY raised ``KeyError`` in the command loop (a workflow-task
    failure that never produces a result) and WAIT dropped the cancel (the
    workflow blocked until the start-to-close timeout), so both would hang past
    this test's wait rather than reporting ``activity-cancelled``.
    """
    effects = tmp_path / "effects"
    activity_worker = PythonProcess(WORKER, "activity", env=_env("tdb-act", effects))
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)
        workflow_worker = PythonProcess(
            WORKER,
            "workflow",
            "start",
            "xq-cancel-predispatch-wf",
            env=_env(
                "tdb-wf",
                effects,
                TDB_TEST_CANCEL_TYPE=cancel_type,
                TDB_TEST_CANCEL_WHEN="immediate",
            ),
        )
        workflow_worker.start()
        try:
            # A regression keeps RESULT from ever arriving within this window
            # (TRY task-failure loop) or delays it ~60s (WAIT start-to-close).
            line = workflow_worker.wait_for_line("RESULT ", timeout=45)
            assert workflow_worker.wait(timeout=30) == 0
            assert line.split("RESULT ", 1)[1].strip() == "activity-cancelled"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

    if cancel_type == "try":
        # Never dispatched, so the activity never ran on its worker.
        assert not effects.exists() or effects.read_text() == ""
    else:
        # Dispatched then cancelled at dispatch: the activity started and unwound.
        assert effects.read_text() == "started\ncancelled\n"

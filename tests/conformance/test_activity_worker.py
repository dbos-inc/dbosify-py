"""Conformance: the samples-python ``activity_worker/`` corpus — part of the
Phase 3 exit gate (DESIGN §9).

That corpus is a single **cross-language** sample (a Go workflow calling a
Python activity over a Temporal server), so it cannot run as written against a
serverless, in-process model. We discharge the gate by proving the capability
it demonstrates: an **activities-only** worker reachable from a workflow on a
**different task queue** — the cross-queue / distributed activity path
(DESIGN §6.1.2). The Go workflow is substituted by a Python ``SayHelloWorkflow``
(the documented migration delta); the activity body is the sample's
``say_hello_activity`` verbatim. See ``activity_worker_workers.py``.

Two real worker processes run concurrently (a workflows-only worker and an
activities-only worker on separate queues), so the activity genuinely executes
in a different process from the workflow — Temporal's "activities run on
different workers".
"""

import os
from pathlib import Path

import pytest

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKERS = Path(__file__).parent / "activity_worker_workers.py"
REPO_ROOT = Path(__file__).parents[2]


@pytest.mark.timeout(150)
@pytest.mark.usefixtures("cleanup_test_databases")
def test_activity_worker_cross_queue() -> None:
    base_env = {
        "PYTHONPATH": str(REPO_ROOT),
        "TDB_TEST_SYSTEM_DATABASE_URL": system_database_url(),
        # Cooperating workers register different function sets but share the
        # Worker's pinned default DBOS app version, so the activity worker
        # dequeues the workflow worker's __temporal_activity enqueue.
    }

    # Distinct executor ids so each worker only ever recovers its own queue's
    # workflows (here there are no crashes, but it keeps the two cleanly
    # separated on the shared database).
    activity_worker = PythonProcess(
        WORKERS, "activity", env={**base_env, "DBOS__VMID": "tdb-activity"}
    )
    activity_worker.start()
    try:
        activity_worker.wait_for_line("ACTIVITY_WORKER_READY", timeout=90)

        workflow_worker = PythonProcess(
            WORKERS, "workflow", env={**base_env, "DBOS__VMID": "tdb-workflow"}
        )
        workflow_worker.start()
        try:
            line = workflow_worker.wait_for_line("RESULT ", timeout=90)
            assert workflow_worker.wait(timeout=30) == 0, (
                "workflow worker exited nonzero\n"
                f"--- transcript ---\n{''.join(workflow_worker.transcript[-40:])}"
            )
            assert line.split("RESULT ", 1)[1].strip() == "Hello, Temporal!"
        finally:
            workflow_worker.terminate_and_wait()
    finally:
        activity_worker.terminate_and_wait()

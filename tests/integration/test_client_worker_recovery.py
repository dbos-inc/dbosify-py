"""Kill-and-recover through the public API: a Worker subprocess is SIGKILLed
mid-workflow; a restarted Worker recovers the execution at launch (Temporal
worker-restart semantics), and the client-side handle still resolves the
result.
"""

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

from temporal_dbos._internal import inbox
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "phase1_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT), "TEMPORAL_DBOS_CLIENT_POLL_SECONDS": "0.05"}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_worker_sigkill_recovery() -> None:
    first = PythonProcess(WORKER, "start", "two-stage-wf", env=ENV)
    first.start()
    try:
        # Stage one's activity ran and its checkpoints committed; the
        # workflow is parked on a wait_condition.
        first.wait_for_line("STAGE_ONE_DONE", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", "two-stage-wf", env=ENV)
    second.start()
    try:
        # Recovery replays stage one (the activity must not re-execute:
        # exactly one fresh ACTIVITY print, from stage two).
        second.wait_for_line("STAGE_ONE_DONE", timeout=60)
        client = DBOSClient(system_database_url=system_database_url())
        try:
            client.send(
                "two-stage-wf", inbox.signal_envelope("go", []), inbox.INBOX_TOPIC
            )
            result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        finally:
            client.destroy()
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == ["one", "two"]
    # The recovered process replayed stage one from its checkpoint: the only
    # activity that actually executed there was stage two.
    activity_lines = [line for line in second.transcript if "ACTIVITY " in line]
    assert (
        len(activity_lines) == 1 and "ACTIVITY two" in activity_lines[0]
    ), activity_lines

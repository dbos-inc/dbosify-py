"""SIGKILL mid-cancellation-unwind: kill after the cancel was consumed and
the cleanup activity checkpointed, but
before the unwind finishes. Recovery must replay the cancel delivery and the
cleanup (exactly once), reconstruct the mid-unwind state, and still record
CANCELED.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

from dbosify._internal import inbox
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "inflight_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_during_cancellation_unwind(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "cancel-unwind-wf"

    first = PythonProcess(WORKER, "cancel-start", wf_id, str(effects), env=ENV)
    first.start()
    client = None
    try:
        first.wait_for_line("STARTED", timeout=60)
        client = DBOSClient(system_database_url=system_database_url())
        client.send(wf_id, inbox.cancel_envelope("chaos test"), inbox.INBOX_TOPIC)
        # CLEANUP_DONE: the cancel was consumed, the unwind ran, and the cleanup
        # checkpoint committed; the workflow is parked mid-unwind. Kill here.
        first.wait_for_line("CLEANUP_DONE", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "cancel-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        # Recovery replays the cancel delivery and the unwind; the cleanup
        # step must replay from its checkpoint, not re-execute.
        second.wait_for_line("CLEANUP_DONE", timeout=60)
        if client is None:
            client = DBOSClient(system_database_url=system_database_url())
        client.send(wf_id, inbox.signal_envelope("go", []), inbox.INBOX_TOPIC)
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()
        if client is not None:
            client.destroy()

    assert result == {"cause": "CancelledError", "status": "CANCELED"}
    # Exactly one cleanup execution across both processes.
    assert effects.read_text() == "cleanup\n"
    executed = [l for l in second.transcript if "CLEANUP_ACTIVITY_EXECUTED" in l]
    assert not executed, "cleanup activity re-executed during replay"

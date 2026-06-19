"""Message-passing chaos: SIGKILL with message handlers mid-flight, plus the
is_replaying() probe.

An update can be *accepted* (durable acceptance event, client already unblocked)
while its handler is still parked. Killing there and recovering must replay inbox
delivery order, re-park the handlers, run their effects exactly once, and still
answer both update results — including to a client that reconnects after the
crash (``get_update_handle``).
"""

import json
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

from dbosify._internal import inbox
from dbosify.client import Client, WorkflowUpdateStage
from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "inflight_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_is_replaying_across_recovery(tmp_path: Path) -> None:
    """unsafe.is_replaying() is False on first execution, True while
    re-executing below the checkpoint horizon after a crash, and False again
    once execution passes the last recorded event."""
    effects = tmp_path / "effects"
    wf_id = "replay-probe-wf"

    first = PythonProcess(WORKER, "replay-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # Parked past the activity: its checkpoint and the delivering wait
        # round are recorded; kill here.
        first.wait_for_line("PROBE_PARKED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "replay-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        second.wait_for_line("PROBE_PARKED", timeout=60)
        client = DBOSClient(system_database_url=system_database_url())
        try:
            client.send(wf_id, inbox.signal_envelope("go", []), inbox.INBOX_TOPIC)
            result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        finally:
            client.destroy()
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # [early, mid, late] from the recovered execution: early replays below the
    # checkpoint horizon; mid is the live frontier; late is a fresh live signal.
    assert result == {"result": [True, False, False], "status": "COMPLETED"}
    # The activity replayed from its checkpoint, it did not re-execute.
    assert effects.read_text() == "cleanup\n"
    assert not [l for l in second.transcript if "CLEANUP_ACTIVITY_EXECUTED" in l]
    # workflow.logger suppresses during replay: the early line is live in the
    # first process, replayed-and-suppressed in the second; the late one is live.
    assert [l for l in first.transcript if "probe-log-early" in l]
    assert not [l for l in second.transcript if "probe-log-early" in l]
    assert [l for l in second.transcript if "probe-log-late" in l]


@pytest.mark.usefixtures("cleanup_test_databases")
async def test_sigkill_with_updates_in_flight(tmp_path: Path) -> None:
    """Two accepted-but-parked updates and an interleaved signal survive a
    SIGKILL: delivery order replays, effects run exactly once, and a fresh
    client collects both update results by id after the crash."""
    effects = tmp_path / "effects"
    wf_id = "update-chaos-wf"

    first = PythonProcess(WORKER, "updates-start", wf_id, str(effects), env=ENV)
    first.start()
    dbos_client = None
    try:
        first.wait_for_line("STARTED", timeout=60)
        dbos_client = DBOSClient(system_database_url=system_database_url())
        client = Client(dbos_client)
        handle = client.get_workflow_handle(wf_id)
        # Both updates reach ACCEPTED (validator passed, acceptance durable,
        # handler parked on `release`), with a signal in between.
        await handle.start_update(
            "slow_update",
            args=[str(effects), 1],
            id="u1",
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )
        await handle.signal("mark", "a")
        await handle.start_update(
            "slow_update",
            args=[str(effects), 2],
            id="u2",
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "updates-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        second.wait_for_line("STARTED", timeout=60)
        assert dbos_client is not None
        # Recovery re-parked both handlers; release them and re-attach to the
        # in-flight updates by id, as a crashed-and-restarted caller would.
        await handle.signal("release_updates")
        assert await handle.get_update_handle("u1").result() == 1
        assert await handle.get_update_handle_for(None, "u2").result() == 2
        await handle.signal("mark", "b")
        await handle.signal("finish")
        history = await handle.result()
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()
        if dbos_client is not None:
            dbos_client.destroy()

    # Inbox delivery order is checkpointed, so the replayed prefix is exact; the
    # two handlers wake together on release, so their completions may interleave.
    assert history[:3] == ["upd-start:1", "sig:a", "upd-start:2"]
    assert sorted(history[3:5]) == ["upd-end:1", "upd-end:2"]
    assert history[5:] == ["sig:b"]
    # Each update's activity effect happened exactly once (post-recovery) and each
    # validator exactly once (pre-kill): the checkpointed verdict skips re-running.
    assert sorted(effects.read_text().splitlines()) == ["u1", "u2", "v1", "v2"]
    assert not [l for l in first.transcript if "UPDATE_EFFECT" in l]
    assert len([l for l in first.transcript if "VALIDATOR_RAN" in l]) == 2
    assert not [l for l in second.transcript if "VALIDATOR_RAN" in l]

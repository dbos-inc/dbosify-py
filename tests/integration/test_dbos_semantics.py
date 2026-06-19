"""Proofs of the DBOS semantics DESIGN.md §7 declares foundational, in
isolation from the interpreter (debugging them inside it would be far harder):

1. function_id assignment stays deterministic when many async steps execute
   concurrently and complete in an order different from launch order, across
   a SIGKILL — and DBOS.asyncio_wait's winner checkpoints replay without
   re-racing (the recorded round winners reappear first after recovery, even
   though the surviving steps re-execute with real durations).
2. recv consumption is checkpointed in order: messages consumed before a
   crash replay identically, and a recv pending at crash time resumes.
"""

import json
import time
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "dbos_semantics_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_concurrent_step_determinism_across_sigkill(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "chaos-wf"

    first = PythonProcess(WORKER, "chaos-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # STEP_START b2 implies: steps d and b completed and checkpointed, the
        # first two asyncio_wait rounds recorded, a/c (and d2/b2) mid-flight.
        first.wait_for_line("STEP_START b2")
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "chaos-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # The recorded rounds replay first, in recorded order — even though after
    # recovery the re-executed steps (a, c) race again in real time.
    assert result["order"][:2] == ["d", "b"]
    assert sorted(result["order"]) == sorted(
        ["a", "b", "c", "d", "a2", "b2", "c2", "d2"]
    )
    assert result["results"] == {
        name: f"value-{name}" for name in ["a", "b", "c", "d", "a2", "b2", "c2", "d2"]
    }

    # Execution counts: steps checkpointed before the kill must not re-execute
    # (exactly one effects entry); interrupted ones may run twice but no more.
    counts: dict[str, int] = {}
    for line in effects.read_text().splitlines():
        counts[line] = counts.get(line, 0) + 1
    assert counts["d"] == 1, counts
    assert counts["b"] == 1, counts
    for name in ["a", "c", "a2", "b2", "c2", "d2"]:
        assert 1 <= counts[name] <= 2, counts


@pytest.mark.usefixtures("cleanup_test_databases")
def test_recv_order_replays_across_sigkill(tmp_path: Path) -> None:
    wf_id = "recv-wf"
    client = None
    try:
        first = PythonProcess(WORKER, "recv-start", wf_id, env=ENV)
        first.start()
        try:
            # Wait for the workflow row to exist: sends to a not-yet-started
            # workflow fail on a foreign-key constraint.
            first.wait_for_line("STARTED")
            client = DBOSClient(system_database_url=system_database_url())
            client.send(wf_id, "m1", "inbox")
            client.send(wf_id, "m2", "inbox")
            first.wait_for_line("RECEIVED 2")
            time.sleep(0.2)  # let the second recv's checkpoint commit
            first.sigkill()
            assert first.wait() == -9
        finally:
            first.terminate_and_wait()

        second = PythonProcess(WORKER, "recv-resume", wf_id, env=ENV)
        second.start()
        try:
            # The recovered workflow replays m1, m2 from checkpoints, then
            # parks on the third recv.
            second.wait_for_line("RECEIVED 2", timeout=60)
            client.send(wf_id, "m3", "inbox")
            result = _result_from(second.wait_for_line("RESULT ", timeout=60))
            assert second.wait() == 0
        finally:
            second.terminate_and_wait()
    finally:
        if client is not None:
            client.destroy()

    assert result == ["m1", "m2", "m3"]

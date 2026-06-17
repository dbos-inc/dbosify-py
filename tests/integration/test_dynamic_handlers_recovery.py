"""SIGKILL mid-run after a dynamic (catch-all) signal handler has consumed a
delivered signal: recovery must replay that checkpointed delivery through the
dynamic handler exactly once.

The kill lands after the ``ping`` signal's inbox recv is checkpointed and the
run is parked. On resume, DBOS recovery re-delivers ``ping`` through the dynamic
handler (rebuilding state), the test releases the run with a second unknown
signal (``finish``), and the recorded deliveries show ``ping`` exactly once.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "dynamic_handlers_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_replays_dynamic_signal_exactly_once() -> None:
    wf_id = "dyn-recovery-wf"

    first = PythonProcess(WORKER, "start", wf_id, env=ENV)
    first.start()
    try:
        # The dynamic handler consumed `ping` and the run parked.
        first.wait_for_line("DELIVERED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", wf_id, env=ENV)
    second.start()
    try:
        out = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # `ping` survived the crash exactly once (replayed from its checkpoint),
    # then `finish` released the run — no duplicate delivery.
    assert out == [["ping", [5]], ["finish", []]]

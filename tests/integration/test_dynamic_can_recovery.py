"""SIGKILL of a continued-as-new chain whose only message handler is dynamic:
recovery must resume the post-CAN run with its carried state and keep
delivering to the catch-all handler exactly once.

The kill lands while the post-CAN run (run 1) is parked, after the hop has
checkpointed. On resume, DBOS recovery resumes run 1 (carried state intact),
the test sends one more unknown-named signal, and the recorded state shows the
pre-CAN datum (carried across the hop) plus the post-recovery datum — each once.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "dynamic_can_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_can_chain_with_dynamic_handler() -> None:
    wf_id = "dyn-can-recovery-wf"

    first = PythonProcess(WORKER, "start", wf_id, env=ENV)
    first.start()
    try:
        # Run 0 hopped (CAN); run 1 is up and parked.
        first.wait_for_line("HOPPED", timeout=60)
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

    # "kept:K" recorded in run 0 and carried across the CAN survived the crash;
    # "added:A" reached the recovered run 1's dynamic handler — no duplicates.
    assert out == ["kept:K", "added:A"]

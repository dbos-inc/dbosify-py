"""SIGKILL-recovery test: an un-deserializable signal stays dropped across a
crash, while well-typed signals survive and are replayed from their checkpoints.
"""

from pathlib import Path
from typing import Dict

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "bad_signal_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env() -> Dict[str, str]:
    return {"PYTHONPATH": str(REPO_ROOT)}


@pytest.mark.usefixtures("cleanup_test_databases")
def test_bad_signal_drop_survives_crash() -> None:
    wf_id = "bad-signal-recovery"
    first = PythonProcess(WORKER, "start", wf_id, env=_env())
    first.start()
    try:
        # PARKED prints only after the good signal is recorded and the bad one
        # dropped, so the state we crash with excludes the bad signal.
        first.wait_for_line("PARKED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "resume", wf_id, env=_env())
    second.start()
    try:
        line = second.wait_for_line("RESULT ", timeout=60)
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # Only the well-typed signals, in order — the bad one never reappears.
    assert line.split("RESULT ", 1)[1].strip() == "good,finish"

"""SIGKILL-recovery test for workflow.set_current_details() (audit item 3):
current details are reconstructed by replay, not read from a checkpoint. A
parked run is crashed mid-flight and resumed in a fresh process; the resumed run
must still return the details its first execution set.
"""

from pathlib import Path
from typing import Dict

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "current_details_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]


def _env() -> Dict[str, str]:
    return {"PYTHONPATH": str(REPO_ROOT)}


@pytest.mark.usefixtures("cleanup_test_databases")
def test_current_details_reconstructed_after_crash() -> None:
    wf_id = "details-recovery"
    first = PythonProcess(WORKER, "start", wf_id, env=_env())
    first.start()
    try:
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

    assert line.split("RESULT ", 1)[1].strip() == "details-from-activity"

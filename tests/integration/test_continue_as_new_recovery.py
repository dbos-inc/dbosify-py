"""SIGKILL mid-continue-as-new-chain: the kill lands while a run of the
chain is in flight. Recovery must resume that run from its checkpoints and
complete the chain — each run's activity executes exactly once and no twin
runs appear (the next run id is deterministic, so a replayed enqueue
re-attaches instead of spawning a duplicate).
"""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "phase3_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_chain(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "chain-wf"

    first = PythonProcess(WORKER, "chain-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # Run 2 has recorded its activity and is somewhere between its sleep
        # and run 3's first checkpoints; kill here, mid-chain.
        first.wait_for_line("CHAIN_RUN 2", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "chain-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # The unbound handle resolves the chain's final run.
    assert result == {"result": "chain-done", "status": "COMPLETED"}
    # Every run executed exactly once across both processes: replayed runs
    # took their activity from its checkpoint, and the replayed
    # continue-as-new enqueue re-attached rather than starting a twin.
    assert sorted(effects.read_text().splitlines()) == [
        "run0",
        "run1",
        "run2",
        "run3",
        "run4",
    ]

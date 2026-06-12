"""SIGKILL mid-chain-hop (cron continuation / workflow retry): the kill
lands while a run is in flight, after its activity checkpointed but before
the run closed — so the chain hop hasn't happened yet. Recovery must resume
the run from its checkpoints (the activity executes exactly once), and the
replayed close must enqueue exactly one successor (the deterministic n+1
run id makes a replayed hop re-attach instead of spawning a twin).
"""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "chain_hop_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT), "TEMPORAL_DBOS_CLIENT_POLL_SECONDS": "0.05"}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_cron_run(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "cron-recovery-wf"

    first = PythonProcess(WORKER, "cron-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # Run 1 has recorded its fire and is mid-sleep; its chain hop does
        # not exist yet. Kill here.
        first.wait_for_line("CRON_RUN 1", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "cron-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        results = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # The counter threaded through last_completion_result is unbroken across
    # the crash: run 1's recovery saw run 0's result, run 2 saw run 1's.
    assert results == [0, 1, 2]
    # Each fire's activity executed exactly once across both processes: the
    # recovered run replayed its record from the checkpoint, and the
    # replayed hop re-attached to run 2 rather than starting a twin.
    fires = sorted(effects.read_text().splitlines())
    assert fires[:3] == ["fire0", "fire1", "fire2"]
    assert len(fires) == len(set(fires))


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_retry_attempt(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "retry-recovery-wf"

    first = PythonProcess(WORKER, "retry-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # Attempt 2 has recorded itself and is mid-sleep, about to fail;
        # the retry hop to attempt 3 does not exist yet. Kill here.
        first.wait_for_line("RETRY_ATTEMPT 2", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "retry-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=60))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == "succeeded on attempt 3"
    # Three attempts, each recorded exactly once: the killed attempt's
    # activity came back from its checkpoint on recovery.
    assert sorted(effects.read_text().splitlines()) == [
        "attempt1",
        "attempt2",
        "attempt3",
    ]

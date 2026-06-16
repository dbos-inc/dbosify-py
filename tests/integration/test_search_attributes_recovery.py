"""SIGKILL after an in-workflow memo/search-attribute upsert: recovery must
preserve the upserted values.

The kill lands after ``upsert_memo``/``upsert_search_attributes`` have durably
written the DBOS attributes column (the write is a checkpointed step) but before
the run closes. Recovery re-decodes the *initial* attributes from the run
envelope and replays the upserts in memory, while the write step replays from
its checkpoint — so the final attributes (changed value, added key, merged memo)
are intact when the recovered run completes.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "search_attr_recovery_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_after_upsert_preserves_attributes() -> None:
    wf_id = "sa-recovery-wf"

    first = PythonProcess(WORKER, "start", wf_id, env=ENV)
    first.start()
    try:
        # The upsert write has checkpointed and the run is parked.
        first.wait_for_line("UPSERTED", timeout=60)
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

    # Search attributes: the upsert (changed CustomKeyword, added CustomInt)
    # survived the crash.
    assert out["sa"] == ["CustomInt=7", "CustomKeyword='survived'"]
    # Memo: the upsert merged (phase replaced, owner from the initial start
    # kept) across recovery.
    assert out["memo"] == {"phase": "upserted", "owner": "ada"}

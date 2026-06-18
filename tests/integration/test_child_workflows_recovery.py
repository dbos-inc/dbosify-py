"""SIGKILL while a child workflow is in flight: the kill takes out the
parent *and* the in-process child mid-execution. Recovery must resume both,
re-attaching the parent to the *same* child (idempotent SetWorkflowID start
— no twin), and the child's work must happen exactly once.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOSClient

from tests.dbconfig import system_database_url
from tests.harness import PythonProcess

WORKER = Path(__file__).parent / "phase2_worker.py"
REPO_ROOT = Path(__file__).parents[2]
ENV = {"PYTHONPATH": str(REPO_ROOT)}


def _result_from(line: str) -> Any:
    return json.loads(line.split("RESULT ", 1)[1])


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_child_reattaches(tmp_path: Path) -> None:
    effects = tmp_path / "effects"
    wf_id = "child-reattach-wf"

    first = PythonProcess(WORKER, "child-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        # The child has started (its start is durable) but is mid-sleep,
        # well before its recording activity.
        first.wait_for_line("CHILD_STARTED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "child-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=120))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == {"result": "parent saw: done", "status": "COMPLETED"}
    # The child's activity executed exactly once across both processes.
    assert effects.read_text() == "child-work\n"

    # Recovery re-attached to the same child: exactly one child row exists.
    client = DBOSClient(system_database_url=system_database_url())
    try:
        children = client.list_workflows(workflow_id_prefix="reattach-child")
        assert [c.workflow_id for c in children] == ["reattach-child"]
    finally:
        client.destroy()


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_child_retry_follows_chain(tmp_path: Path) -> None:
    # The child fails attempt 1, then a SIGKILL takes out the parent and the
    # child mid-attempt-1. Recovery must resume the child (which fails, retries
    # to attempt 2, and records exactly once) AND the parent's child-result
    # step must follow the failure chain to the retry successor — not surface
    # attempt 1's failure. This exercises the child retry_policy wiring plus the
    # child-result step's new failure-follow branch across recovery.
    effects = tmp_path / "effects"
    wf_id = "child-retry-reattach-wf"

    first = PythonProcess(WORKER, "childretry-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        first.wait_for_line("CHILD_ATTEMPT 1", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "childretry-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=120))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    assert result == {"result": "parent saw: done", "status": "COMPLETED"}
    # The successful attempt's activity ran exactly once (attempt 1 failed
    # before recording; recovery did not double-run it).
    assert effects.read_text() == "child-work\n"

    # The child chain advanced past run 0 — the retry produced a successor run.
    client = DBOSClient(system_database_url=system_database_url())
    try:
        runs = client.list_workflows(workflow_id_prefix="reattach-retry-child")
        assert len(runs) >= 2
    finally:
        client.destroy()


@pytest.mark.usefixtures("cleanup_test_databases")
def test_sigkill_mid_child_run_timeout_still_fires(tmp_path: Path) -> None:
    # A child with a run_timeout is SIGKILLed (along with its parent) while
    # mid-sleep, before the timeout would fire. The deadline is durable, so
    # recovery must re-apply it and terminate the child at the original
    # deadline. If run_timeout were dropped on recovery, the recovered child
    # would sleep the full 120s and the parent would never return.
    effects = tmp_path / "effects"
    wf_id = "child-timeout-reattach-wf"

    first = PythonProcess(WORKER, "childtimeout-start", wf_id, str(effects), env=ENV)
    first.start()
    try:
        first.wait_for_line("TIMEOUT_CHILD_STARTED", timeout=60)
        first.sigkill()
        assert first.wait() == -9
    finally:
        first.terminate_and_wait()

    second = PythonProcess(WORKER, "childtimeout-resume", wf_id, str(effects), env=ENV)
    second.start()
    try:
        result = _result_from(second.wait_for_line("RESULT ", timeout=120))
        assert second.wait() == 0
    finally:
        second.terminate_and_wait()

    # The recovered child timed out → TERMINATED → surfaced to the parent.
    assert result == {
        "result": "child-terminated:TerminatedError",
        "status": "COMPLETED",
    }

"""Conformance: run temporalio/samples-python ``hello/`` samples against
dbosify with the mechanical import rewrite plus the connection-setup
adapter (see runner.py). The pass/xfail/skip expectations below record the
conformance status of each sample.
"""

import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pytest

from tests.conformance.samples import ensure_samples, rewrite_sample
from tests.dbconfig import make_dbos_client, system_database_url
from tests.harness import PythonProcess

RUNNER = Path(__file__).parent / "runner.py"
# Generous by default: each subprocess pays full DBOS init plus schema migrations on
# a fresh database before any workflow, eating seconds on CI. Hanging xfails pin timeout=10.
SAMPLE_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class Expectation:
    expect_output: Optional[str] = None  # substring required in stdout
    xfail: Optional[str] = None  # reason this can't pass yet
    skip: Optional[str] = None  # reason this isn't runnable in the harness
    timeout: int = SAMPLE_TIMEOUT_SECONDS  # for samples that legitimately run long
    # Sample never exits by design (e.g. hello_cron): run in background, verify via
    # database (DB_VERIFIERS), tear down. `ready_line` marks boot, waited before verify.
    runs_forever: bool = False
    ready_line: Optional[str] = None
    # Opt this sample's worker into near-immediate queue dispatch (runner.py).
    # Only for samples whose pass/fail hinges on wall-clock dispatch latency.
    fast_queue: bool = False


# Boot budget for runs-forever samples: subprocess start + DBOS init + schema
# migrations on a fresh database + start_workflow, on a loaded CI runner.
READY_TIMEOUT_SECONDS = 90


EXPECTATIONS = {
    "hello_activity": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_async": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_choice": Expectation(expect_output="Order result:"),
    "hello_activity_heartbeat": Expectation(expect_output="Result: Hello, World!"),
    "hello_activity_method": Expectation(expect_output="Database update executed"),
    "hello_activity_multiprocess": Expectation(
        xfail="multiprocess activity executors (SharedStateManager) unsupported",
        timeout=10,
    ),
    "hello_activity_retry": Expectation(expect_output="Result: Hello, World!"),
    "hello_async_activity_completion": Expectation(
        # ~3s of client-side heartbeating before external completion.
        expect_output="Result: Hello, World!",
        timeout=30,
    ),
    "hello_cancellation": Expectation(
        # Waits 2s before cancelling; the sync activity observes the cancel
        # at its next heartbeat and the cleanup activity runs in the unwind.
        expect_output="Got expected exception",
        timeout=30,
    ),
    "hello_change_log_level": Expectation(
        skip="never exits by design: awaits a workflow whose task fails "
        "forever (identical behavior on a real Temporal server)"
    ),
    "hello_child_workflow": Expectation(expect_output="Result: Hello, World!"),
    "hello_continue_as_new": Expectation(
        # 10 chained runs, each sleeping 1s (plus per-run dispatch latency).
        expect_output="Running workflow iteration 9",
        timeout=90,
    ),
    "hello_cron": Expectation(
        # Starts a "* * * * *" cron and waits forever; proof is in the database (no
        # completion line). Timeout covers up to a minute to the next boundary plus run.
        runs_forever=True,
        ready_line="Running workflow once a minute",
        timeout=150,
    ),
    "hello_exception": Expectation(),
    "hello_local_activity": Expectation(expect_output="Result: Hello, World!"),
    "hello_mtls": Expectation(skip="requires mTLS certificates and a TLS endpoint"),
    "hello_parallel_activity": Expectation(expect_output="Result:"),
    "hello_patch": Expectation(
        skip="manual multi-deploy walkthrough (not runnable in one harness pass); "
        "patched()/deprecate_patch() are implemented and covered by "
        "tests/integration/test_patched_recovery.py"
    ),
    "hello_query": Expectation(
        # The second query hits a *completed* workflow; rehydrate-by-replay
        # reconstructs its final state to answer it.
        expect_output="Second greeting result: Goodbye, World!",
    ),
    "hello_search_attributes": Expectation(
        # Upsert at 2s, describe at 3s — a 1s margin DBOS's ~1s dispatch latency would
        # race, so this worker opts into near-immediate dispatch (fast_queue, runner.py).
        expect_output="Second search attribute values:  ['new-value']",
        fast_queue=True,
        timeout=30,
    ),
    "hello_signal": Expectation(expect_output="Result:"),
    "hello_update": Expectation(expect_output="Workflow Result:"),
}


@pytest.fixture(scope="session")
def rewritten_samples(tmp_path_factory: pytest.TempPathFactory) -> Path:
    samples_root = ensure_samples() / "hello"
    dest = tmp_path_factory.mktemp("rewritten_hello")
    for name in EXPECTATIONS:
        source = samples_root / f"{name}.py"
        assert source.exists(), (
            f"sample {name} missing from pinned samples-python checkout — "
            "update EXPECTATIONS for the pinned commit"
        )
        rewrite_sample(source, dest)
    return dest


def _params() -> "list[Any]":
    params = []
    for name, exp in sorted(EXPECTATIONS.items()):
        marks = []
        if exp.skip:
            marks.append(pytest.mark.skip(reason=exp.skip))
        elif exp.xfail:
            marks.append(pytest.mark.xfail(reason=exp.xfail, strict=False))
        params.append(pytest.param(name, id=name, marks=marks))
    return params


def _verify_hello_cron(deadline_seconds: float, process: PythonProcess) -> None:
    """The cron chain's proof: run 0 of `hello-cron-workflow-id` completed
    (the schedule fired and the workflow ran) and run 1 exists (the chain
    hop enqueued the next occurrence).

    On failure this reports everything needed to diagnose remotely (the
    failure mode seen in CI — run 0 parked in PENDING with a clean log —
    has not reproduced locally): a status-transition timeline, the run's
    bookkeeping columns, and a faulthandler all-threads stack dump of the
    sample (SIGABRT; the runner sets PYTHONFAULTHANDLER=1).
    """
    from dbos import DBOSClient

    run_ids = ["hello-cron-workflow-id", "hello-cron-workflow-id--r1"]
    start = time.monotonic()
    deadline = start + deadline_seconds
    statuses: "dict[str, str]" = {}
    timeline: "list[str]" = []
    client: Optional[DBOSClient] = None
    try:
        while time.monotonic() < deadline:
            time.sleep(1.0)
            try:
                if client is None:
                    # The sample subprocess creates the database; until
                    # then, construction/queries fail — keep retrying.
                    client = make_dbos_client()
                polled = {
                    s.workflow_id: s.status
                    for s in client.list_workflows(workflow_ids=run_ids)
                }
            except Exception as poll_error:
                timeline.append(
                    f"t={time.monotonic() - start:.0f}s poll error: {poll_error!r}"
                )
                continue
            if polled != statuses:
                statuses = dict(polled)
                timeline.append(f"t={time.monotonic() - start:.0f}s {statuses!r}")
            if statuses.get(run_ids[0]) == "SUCCESS" and run_ids[1] in statuses:
                return
        run0 = statuses.get(run_ids[0])
        hint = {
            None: "the sample never started the workflow",
            "DELAYED": "the first fire is still pending (waiting for its "
            "cron boundary — consider a larger budget)",
            "ENQUEUED": "the fire was released but no worker dequeued it",
            "PENDING": "the run was claimed for execution and never "
            "finished — see the thread dump below for where it is stuck",
        }.get(run0, "unexpected terminal state — the run should chain")
        bookkeeping = "<unavailable>"
        if client is not None:
            try:
                bookkeeping = " | ".join(
                    f"{s.workflow_id}: status={s.status} created={s.created_at} "
                    f"updated={s.updated_at} attempts={s.recovery_attempts} "
                    f"executor={s.executor_id} appver={s.app_version}"
                    for s in client.list_workflows(workflow_ids=run_ids)
                )
            except Exception as err:
                bookkeeping = f"<query failed: {err!r}>"
        # SIGABRT + PYTHONFAULTHANDLER dumps every thread's stack into the
        # captured output — distinguishing "slow" from "stuck" and where.
        process.sigabrt_for_stacks(grace_seconds=3.0)
        transcript = "".join(process.transcript[-120:]) or "<no output>"
        pytest.fail(
            f"hello_cron: cron did not fire and chain within "
            f"{deadline_seconds}s: {hint}\n"
            f"--- status timeline ---\n" + "\n".join(timeline or ["<empty>"]) + "\n"
            f"--- workflow rows ---\n{bookkeeping}\n"
            f"--- sample output incl. thread dump (tail) ---\n{transcript}"
        )
    finally:
        if client is not None:
            client.destroy()


DB_VERIFIERS = {"hello_cron": _verify_hello_cron}


@pytest.mark.timeout(
    max(
        e.timeout + (READY_TIMEOUT_SECONDS if e.runs_forever else 0)
        for e in EXPECTATIONS.values()
    )
    + 30
)
@pytest.mark.usefixtures("cleanup_test_databases")
@pytest.mark.parametrize("sample_name", _params())
def test_hello_sample(sample_name: str, rewritten_samples: Path) -> None:
    expectation = EXPECTATIONS[sample_name]
    if expectation.runs_forever:
        process = PythonProcess(
            RUNNER,
            str(rewritten_samples / f"{sample_name}.py"),
            env={
                "DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
                # Lets the verifier collect an all-threads stack dump from
                # the live sample (SIGABRT) when verification fails.
                "PYTHONFAULTHANDLER": "1",
            },
        )
        process.start()
        try:
            # Boot first, on its own budget: the verification clock starts only
            # once the sample is up and has started its workflow.
            assert expectation.ready_line is not None
            process.wait_for_line(expectation.ready_line, timeout=READY_TIMEOUT_SECONDS)
            DB_VERIFIERS[sample_name](expectation.timeout, process)
        finally:
            process.terminate_and_wait()
        return
    env = {
        **__import__("os").environ,
        "DBOSIFY_CONFORMANCE_SYSTEM_DATABASE_URL": system_database_url(),
    }
    if expectation.fast_queue:
        env["DBOSIFY_CONFORMANCE_FAST_QUEUE"] = "1"
    result = subprocess.run(
        [sys.executable, str(RUNNER), str(rewritten_samples / f"{sample_name}.py")],
        capture_output=True,
        text=True,
        timeout=expectation.timeout,
        env=env,
    )
    assert result.returncode == 0, (
        f"sample exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr[-4000:]}"
    )
    if expectation.expect_output:
        # Some samples emit their proof via logging (stderr), not print.
        combined = result.stdout + result.stderr
        assert expectation.expect_output in combined, (
            f"expected {expectation.expect_output!r} in output\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr[-2000:]}"
        )

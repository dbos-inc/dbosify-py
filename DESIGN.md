# temporal-dbos: Design Document

A Python package that re-implements the Temporal Python SDK (`temporalio`) API on top of
DBOS Transact, so that applications written against Temporal run on DBOS + Postgres with no
Temporal server — ideally with only an import change, optionally with zero code changes.

This document is the complete spec for the implementing agent. It assumes no prior context.
Read it fully before writing code.

---

## 0. Resources available on this machine

| Resource | Path | Use |
|---|---|---|
| Temporal Python SDK source (v1.28.x) | `/home/peter/competitors/temporal-sdk-python/temporalio/` | **Authoritative signature reference.** Mirror public signatures, enum values, exception constructors, and docstring-visible behavior from here. Read access is pre-granted in `.claude/settings.local.json`. |
| DBOS Transact Python source | `/home/peter/dbos-transact-py/` | The backend. Read `dbos/__init__.py` for exports, `dbos/_dbos.py`, `_queue.py`, `_client.py`, `_context.py`, `_event_loop.py`, `_error.py`. Its `tests/` show idiomatic usage. |
| Temporal docs | https://docs.temporal.io/develop/python/ and https://python.temporal.io/ | Semantics reference. |
| Temporal samples | https://github.com/temporalio/samples-python | Conformance corpus (Phase 1+). |

Especially important file: `/home/peter/competitors/temporal-sdk-python/temporalio/worker/_workflow_instance.py`.
It is Temporal's own deterministic workflow interpreter (a custom object implementing the
asyncio event-loop protocol that hosts workflow coroutines). Our interpreter (§4) has the
same job with a different event source; study that file before building ours. Both projects
are MIT-licensed; adapting design and code with attribution is fine.

Dev conventions for this repo: Python ≥ 3.10, `uv` for environment/scripts (run everything
via `uv run ...`), `pytest`, `mypy --strict`, `black` + `isort`. Integration tests need
Postgres; a server is provisioned externally (CI service container or local install — never
launch one from tests). Tests drop/re-create their own databases for isolation (see
`tests/conftest.py`).

---

## 1. Goals and non-goals

**Goal.** SDK-level, Python-only compatibility: code written against `temporalio` runs
unmodified on DBOS. "Unmodified" means: same decorators, same call signatures, same
exception types raised in the same places, same blocking/async behavior, same durability
guarantees (a workflow survives process crashes and resumes correctly).

**Non-goals** (state these in the README):
- gRPC wire compatibility. Other-language Temporal SDKs cannot connect. This is not a
  Temporal *server* replacement; it is a replacement for server + Python SDK together.
- Temporal Web UI, `temporal` CLI, tctl. Users get DBOS's workflow-management APIs and
  Conductor instead.
- Nexus and advanced visibility (full query language, custom indexed search attributes)
  — partial support only, see §8. Namespaces are supported as per-namespace DBOS system
  schemas; clients for different namespaces coexist in one process, while a worker process
  serves one namespace (its launched runtime is process-global) — DEVIATIONS D1.

**Why this is feasible.** Both systems use deterministic re-execution with checkpointed
effects. Temporal replays workflow code against an event history held by the server; DBOS
re-executes workflow code from the top on recovery, skipping completed steps whose results
are checkpointed in Postgres. The determinism contract Temporal imposes on user workflow
code (no wall-clock, no I/O, no uncontrolled randomness, effects only through SDK calls) is
exactly the contract DBOS needs. Existing Temporal workflow code is therefore already
written the way DBOS requires. The one structural mismatch is that Temporal workflows are
stateful actor-like *classes* (a main coroutine plus signal/update/query handlers
interleaving on a deterministic event loop) while DBOS workflows are *functions*. Bridging
that is the core engineering work: the deterministic interpreter of §4.

---

## 2. Packaging

Two levels of drop-in were planned; only the first ships:

1. **`temporal_dbos` package** mirroring `temporalio`'s module layout exactly:
   `temporal_dbos.workflow`, `.activity`, `.client`, `.worker`, `.common`, `.exceptions`,
   `.converter`, `.testing`. Migration = change the import root. This is the primary —
   and only — supported migration path. (`temporalio.contrib.pydantic` is **not** mirrored
   — see §6.9; configure a custom `DataConverter` for pydantic support.)
2. **Alias shim** for zero-change runs (`temporal_dbos.install()` + a
   `python -m temporal_dbos run app.py` runner serving `temporalio.*` imports from
   `temporal_dbos.*`): **out of scope** (cut from Phase 4 — see §9). The import-root swap
   in (1) is the only supported migration path.

PyPI name `temporal-dbos`; import name `temporal_dbos`. Depends on `dbos` (PyPI pin;
`/home/peter/dbos-transact-py` stays a read-only reference tree). Do **not** depend on
`temporalio` at runtime; it may appear as a dev-dependency for signature-parity tests
only (see §9).

Proposed layout:

```
temporal_dbos/
    __init__.py
    workflow.py            # @defn/@run/@signal/@query/@update, execute_activity, sleep, ...
    activity.py            # @defn, heartbeat, info, raise_complete_async, ...
    client.py              # Client, WorkflowHandle, schedules, async-activity handles
    worker.py              # Worker, Replayer (stub until Phase 4), interceptor bases
    common.py              # RetryPolicy, enums, SearchAttributeKey, RawValue, Priority
    exceptions.py          # full FailureError tree
    converter.py           # DataConverter/PayloadConverter/PayloadCodec + DBOS Serializer adapter
    types.py               # mirrored from temporalio.types as needed
    testing/
        __init__.py        # WorkflowEnvironment, ActivityEnvironment
    # runner.py            # python -m temporal_dbos run — CUT (out of scope, §9)
    # _shim.py             # temporalio alias meta-path finder — CUT (out of scope, §9)
    _internal/
        registry.py        # workflow-type/activity-type name -> definition
        dispatcher.py      # the registered DBOS workflow functions (§3)
        interpreter.py     # deterministic event loop (§4)
        inbox.py           # signal/update/query/cancel message envelope + routing
        activities.py      # activity execution: local steps & queued activity workflows
        handles.py         # WorkflowHandle impl, run-chain resolution
        ids.py             # temporal workflow_id <-> DBOS workflow id, run_id synthesis (§6.4)
        status.py          # DBOS status/error -> WorkflowExecutionStatus mapping (§6.2)
        payloads.py        # envelope formats stored in DBOS inputs/outputs/messages
        visibility.py      # list_workflows query-string parser -> DBOS list filters
        schedules.py       # ScheduleSpec -> cron compilation, ScheduleHandle backing
        runtime.py         # lazy DBOS/DBOSClient lifecycle shared by Client/Worker (§5)
tests/
    unit/                  # no DB needed where possible
    integration/           # real Postgres, crash/recovery tests
    conformance/           # temporalio samples + adapted SDK tests
```

---

## 3. Architecture overview

```
┌──────────────────────────────────────────────────────────────┐
│ API façade — temporalio-shaped modules, types, enums,        │
│ exceptions; pure signature/semantics mirroring               │
├──────────────────────────────────────────────────────────────┤
│ Semantic core                                                │
│   dispatcher: one generic DBOS workflow hosting all Temporal │
│               workflows; one hosting distributed activities  │
│   interpreter: deterministic event loop running the user's   │
│               workflow class (run() + handlers)              │
│   inbox: per-execution ordered durable message stream        │
│          (signals, updates, queries, cancel requests)        │
├──────────────────────────────────────────────────────────────┤
│ DBOS backend — @DBOS.workflow, @DBOS.step, Queue,            │
│ send/recv, set_event/get_event, DBOS.sleep, schedules,       │
│ cancel/resume/fork, DBOSClient                               │
└──────────────────────────────────────────────────────────────┘
```

**The dispatcher pattern.** All Temporal workflows execute through a single registered DBOS
workflow function:

```python
@DBOS.workflow(name="__temporal_workflow")
async def temporal_workflow_dispatcher(workflow_type: str, payload: SerializedInput) -> SerializedOutput:
    defn = registry.lookup_workflow(workflow_type)   # raises clean error if unregistered
    return await Interpreter(defn, payload).run()
```

Rationale: Temporal dispatches workflows by string type name to whatever worker has the
class registered — a generic dispatcher reproduces that, lets `DBOSClient`-backed clients
enqueue by name without importing user code, and gives every Temporal workflow uniform DBOS
metadata. Distributed activities get a parallel `__temporal_activity` dispatcher workflow
(§6.1.2). The Temporal workflow type name is stored inside the payload envelope *and*
queryable: pass it through so `list_workflows` can filter on it (store it in the envelope;
visibility filtering reads inputs — see §6.2).

**Queues.** A Temporal task queue maps to a **database-backed** DBOS queue of the same
name. Workers (§6.1.4) declare their dequeue set with `DBOS.listen_queues([task_queue])`
before launch and persist the queue's configuration (`worker_concurrency` derived from
`Worker(...)` concurrency args) with `DBOS.register_queue` after launch. Workflows started
with `task_queue="X"` are enqueued on DBOS queue `X`; only processes listening on `X`
dequeue them, and queue config is dynamically reloadable from the database. DBOS pins
dequeue to `app_version`, which maps onto Temporal's build-id/versioning story
(application version comes from `DBOSConfig`).

---

## 4. The deterministic interpreter (the load-bearing component)

This is Phase 0. If this works, everything else is plumbing. Build it first, as a spike,
with crash/recovery tests, before writing any façade breadth.

### 4.1 Problem

A Temporal workflow is a class. Its `@workflow.run` coroutine runs concurrently with
signal/update handler coroutines, all multiplexed on a *deterministic* event loop that only
advances when an event arrives (activity completed, timer fired, message received).
`workflow.wait_condition(fn)` parks a coroutine until `fn()` becomes true, re-evaluated
after every event. On replay, the same events must be delivered in the same order so the
same interleaving reconstructs the same state.

Temporal's SDK already does exactly this in
`temporalio/worker/_workflow_instance.py` (`_WorkflowInstance` implements the asyncio
loop protocol; activation jobs from the server drive it). We do the same thing with a
different event source: **DBOS checkpoints instead of server activations.**

### 4.2 Design

The dispatcher is an **async DBOS workflow** running on the real asyncio loop. Inside it,
the interpreter hosts the user's workflow class on a *virtual* deterministic loop (not the
real one). The invariant that makes recovery correct:

> Every nondeterministic fact — what completed, with what result, in what order — must pass
> through a DBOS checkpoint before the virtual loop observes it.

Event sources:

1. **Inbox** — one totally-ordered durable message stream per execution. Anything external
   (signal, update request, query request, cooperative-cancel request) is delivered via
   `DBOS.send(dbos_workflow_id, envelope, topic="__tdb_inbox__")` and consumed with
   `DBOS.recv("__tdb_inbox__", timeout)`. `recv` is a checkpointed step in DBOS: first
   execution records which message arrived; replay returns the same message. Order is
   thereby fixed at first execution.
2. **Activity / child-workflow completions** — activities run as DBOS steps or child
   workflows (§6.1.2); their results are checkpoints.
3. **Timers** — `workflow.sleep` / `asyncio.sleep` on the virtual loop become DBOS durable
   sleeps; checkpointed.

The scheduling core:

```
seq = 0  # deterministic counter; every command (activity, timer, child wf) gets seq++
loop:
    run virtual loop until all coroutines are blocked (no ready callbacks)
    if main run() coroutine finished and all_handlers_finished policy satisfied: exit
    waiters = { pending timers (virtual deadlines), pending activity/child handles, inbox }
    event = next_event(waiters)          # THE checkpoint — see below
    dispatch event into the virtual loop:
        timer fired      -> resolve that timer future, advance virtual clock to its deadline
        activity done    -> resolve handle future with result/raise wrapped error (§6.3)
        inbox message    -> route: signal -> spawn/queue handler coroutine
                                   update -> validate (verdict checkpointed; run-once), reject via set_event, or spawn handler
                                   query  -> answer from current state via set_event (no state change)
                                   cancel -> raise asyncio.CancelledError in main task (§6.5)
    re-evaluate all wait_condition predicates
```

`next_event(waiters)` is the multiplexer. Baseline implementation: a DBOS **step** that, on
first execution, polls all pending sources (child-workflow statuses via `get_status`,
earliest timer deadline vs. checkpoint-able current time, `recv` with a timeout slice) and
returns a serializable record of the first thing that fired
(`{"kind": "timer", "seq": 3}` / `{"kind": "activity", "seq": 5, "ok": true, "result": ...}` /
`{"kind": "message", "envelope": {...}}`). On recovery it returns the recorded value and the
virtual loop replays identically.

**Evaluate before committing to the baseline:** DBOS already exposes
`DBOS.asyncio_wait(fs, timeout=..., return_when=...)` (see `dbos/_dbos.py` and
`dbos/_event_loop.py`) — a deterministic wait primitive over awaitables inside async DBOS
workflows, plus async steps that can run concurrently on the real loop. If `asyncio_wait`'s
checkpointing covers the "which completed first" race (including racing a `recv_async`
against step tasks and sleeps), build `next_event` on top of it instead of hand-rolling
polling. Phase 0 must answer this question explicitly; correctness requirement either way:
the winner and its payload are recorded before delivery, and replay needs no live polling.

Virtual-loop details:

- **Virtual time.** `workflow.now()/time()/time_ns()` return a virtual clock that advances
  only on events (Temporal semantics: the workflow-task timestamp). Initialize from a
  checkpointed start timestamp; advance to each timer's deadline when it fires and to a
  checkpointed observation time when an external event is delivered (record arrival time
  inside the `next_event` checkpoint). Never read the wall clock from workflow code paths.
- **Randomness.** `workflow.random()` / `uuid4()` derive from a per-execution seed
  checkpointed once at start (one step). Replay reseeds identically. This avoids
  per-call DB writes.
- **`asyncio` interception.** User code may call `asyncio.sleep`, `asyncio.gather`,
  `asyncio.Lock`, `wait_for`, etc. Because their coroutines run on *our* loop, standard
  asyncio primitives are deterministic for free (this is exactly how Temporal's SDK works —
  see `_workflow_instance.py`). Provide `workflow.wait` / `workflow.as_completed` mirrors.
- **Handler lifecycle (done).** `workflow.all_handlers_finished()`; the
  `HandlerUnfinishedPolicy` warn-on-exit behavior (in-flight handler records in the
  interpreter; temporalio-style `Unfinished{Signal,Update}HandlersWarning` with the
  TMPRL1102 text on terminal outcomes); signal buffering for handlers not (yet)
  registered — delivery-on-registration becomes meaningful with dynamic handlers
  (Phase 3); `@workflow.init` passes run args to `__init__` first.
- **Replay flag (done).** At interpreter start, read the checkpoint horizon (max recorded
  `function_id`, via `list_workflow_steps` on an executor thread — in-context the call is
  itself checkpointed and would replay its own empty first-execution result); the claim
  cursor below the horizon means replaying. Backs `workflow.unsafe.is_replaying()` and
  `workflow.logger` replay suppression (a temporalio-mirroring `LoggerAdapter`).
- **Workflow-task failure semantics (important, easy to get wrong).** In Temporal, an
  exception raised by workflow code that is *not* a `FailureError` (and not listed in
  `failure_exception_types`) fails the *workflow task*, not the workflow: the task retries
  forever and the workflow stays RUNNING, so users can fix the bug, redeploy, and the
  workflow resumes. Reproduce this: the dispatcher catches non-failure exceptions from the
  interpreter, logs loudly, sleeps with capped backoff (real sleep — this is outside the
  deterministic boundary), and re-runs the interpreter (which replays from checkpoints —
  cheap and exactly Temporal's behavior). The DBOS workflow stays PENDING → maps to RUNNING.
  Honor `@workflow.defn(failure_exception_types=[...])` and
  `Worker(workflow_failure_exception_types=[...])` to convert listed types into workflow
  failures instead. Provide an env-var escape hatch (`TEMPORAL_DBOS_FAIL_FAST=1`) that lets
  dev/test runs fail immediately.

### 4.3 Phase 0 exit criteria (all under real Postgres)

Write these tests first; they define done:

1. Workflow with `run()` awaiting `wait_condition(lambda: self.approved)`; signal arrives;
   workflow completes. Kill the process (SIGKILL) after the signal is recorded but before
   completion; recover; same result, handler not re-applied twice to state.
2. Two concurrent activities + a timer racing; assert completion order observed by user code
   is identical across a forced recovery replay (record order in workflow state, compare).
3. `asyncio.gather` of 3 activities; one fails with retry policy; surfaces as
   `ActivityError(cause=ApplicationError)` after retries.
4. Update with validator: rejected update leaves no trace in workflow state across recovery;
   accepted update returns a value to the caller.
5. 1,000-iteration loop of (sleep 0 + tiny activity): measure recovery replay time; document
   it (this is the perf baseline that motivates later optimization).
6. Non-failure exception in workflow code: workflow stays RUNNING; "fix the bug" by swapping
   the registered class implementation; workflow completes.

---

## 5. Runtime lifecycle: aligned with DBOS machinery

(Revised during Phase 1: rather than mimicking `temporalio`'s connection surface, the
facade takes DBOS machinery directly.)

- `Client(dbos_client)` / `await Client.connect(dbos_client)` wraps a `dbos.DBOSClient`,
  which carries the database URL and system schema (namespacing rides on
  `dbos_system_schema`). Client mode is exactly DBOSClient mode: enqueue-by-name, send,
  get_event, list/cancel/fork — everything a starter needs, with no workflow registration
  or recovery.
- `Worker(config: DBOSConfig, task_queue=..., workflows=[...], activities=[...])` owns the
  process's DBOS lifecycle outright: construction creates `DBOS(config=config)`, registers
  dispatchers + the named queue with mapped concurrency + user definitions; `await
  worker.run()` performs `DBOS.launch()` (recovery of pending workflows happens here,
  mirroring Temporal worker restart semantics) and blocks until `shutdown()`, which
  destroys DBOS. Support `async with Worker(...)` — tests use it constantly.
- **Exactly one Worker per process** (v1): DBOS's launchable runtime is process-global, so
  multiple in-process Workers would share lifecycle/registrations in ways that diverge
  from Temporal's worker-isolation model. Multi-worker support can be revisited later
  (per-queue stop primitives upstream would help).
- Map `Worker` args: `max_concurrent_workflow_tasks` → workflow queue `worker_concurrency`;
  `graceful_shutdown_timeout` → `DBOS.destroy(workflow_completion_timeout_sec=...)`;
  application version comes from `DBOSConfig`. Accept-and-ignore (with a debug log, never
  warning-spam) the tuner/poller/sandbox args.

---

## 6. Feature-by-feature mapping spec

Signatures must mirror `temporalio` 1.28 exactly (copy from the local checkout, including
`arg` + `args=[...]` calling conventions, keyword-only markers, and defaults). Below is the
*backing design* per feature, organized by module, with phases. Fidelity notes marked
**DEVIATION** must end up in the README compatibility table.

### 6.1 Core execution

#### 6.1.1 `workflow.defn / run / signal / query / update / init`
Registration populates `_internal/registry.py` keyed by workflow type name (default:
unqualified class name; `name=` override). **Dynamic *workflows*
(`@workflow.defn(dynamic=True)`) are not supported and raise `NotImplementedError`:**
a catch-all workflow has no per-type `wf:{type}` DBOS registration to dispatch to,
which conflicts with the one-workflow-per-type model (§10.1, DEVIATIONS D25); the
`dynamic` parameter is accepted only for signature parity. Dynamic *signal/query/update
handlers* (`dynamic=True` → a single catch-all per category, invoked as
`(self, name, Sequence[RawValue])` when no exact handler matches), `description=` handler
metadata, and dynamic *activities* (§6.1.2) **are** supported — with
`workflow.payload_converter()` / `activity.payload_converter()` exposed so a dynamic
handler can convert its `RawValue` args. Validate at decoration time like temporalio does
(exactly one `@workflow.run`, async, the dynamic-handler signature, etc. — copy their
error messages where reasonable). `sandboxed=` is accepted and ignored.

#### 6.1.2 Activities: `@activity.defn`, `execute_activity`, `start_activity`, local activities
Two execution paths, chosen automatically:

- **Same-queue / local path** (also used for `execute_local_activity`): run as a DBOS
  **step** in the dispatcher process. The interpreter launches it as a concurrent async
  step task on the real loop (sync activities run via the worker's `activity_executor`
  thread pool, mirrored API) and the completion flows through `next_event`. Retry policy
  maps onto DBOS step retries:
  `initial_interval→interval_seconds`, `backoff_coefficient→backoff_rate`,
  `maximum_attempts→max_attempts` (Temporal `0` = unlimited → use a retry loop, not a magic
  large number), `maximum_interval` → cap inside a custom `should_retry`/wrapper,
  `non_retryable_error_types` + `ApplicationError(non_retryable=True)` +
  `ApplicationError(next_retry_delay=...)` → custom retry wrapper around the step (do the
  retry logic in shim code wrapping a single-attempt step; each attempt is its own
  checkpoint so recovery resumes at the right attempt — mirror Temporal's attempt counting
  in `activity.info().attempt`).
- **Cross-queue / distributed path** (when `task_queue=` differs from the workflow's, or
  worker config requests it): enqueue a `__temporal_activity(activity_type, payload)` DBOS
  workflow on DBOS queue `task_queue`, child workflow id = `{parent_dbos_id}-a{seq}`.
  Completion polled/awaited through `next_event`. This is what gives Temporal's
  "activities run on different workers" behavior.

Timeouts: enforce `start_to_close` per attempt and `schedule_to_close` overall in the shim
(asyncio timeout around the step task / deadline check on the queued workflow — for queued
activities also map to `SetWorkflowTimeout`); `schedule_to_start` only meaningful on the
queued path (check `dequeued_at` vs deadline). Enforce temporalio's rule: at least one of
`start_to_close_timeout` / `schedule_to_close_timeout` required, same error message.
Timeout → `ActivityError` ← cause `TimeoutError(type=...)` with `last_heartbeat_details`.

`activity.heartbeat(*details)`: throttled write of details to a DBOS event/stream on the
activity's execution (queued path) or in-memory + periodic checkpoint (step path), plus a
poll of a cancellation flag; if cancellation was requested, raise `CancelledError` into the
activity per `ActivityCancellationType` (TRY_CANCEL default / WAIT_CANCELLATION_COMPLETED /
ABANDON). `activity.info()` synthesized (task_token encodes the DBOS workflow id + seq).
Heartbeat details must be visible to the next retry attempt via `info().heartbeat_details`.

`activity.raise_complete_async()` + `client.get_async_activity_handle(task_token=...)`:
the queued activity workflow parks on `DBOS.recv("__tdb_async_complete__", timeout=schedule_to_close)`;
`AsyncActivityHandle.complete/fail/heartbeat/report_cancellation` → `DBOSClient.send` to the
decoded workflow id. Phase 3.

#### 6.1.3 Timers and conditions
`workflow.sleep(d, summary=...)` and bare `asyncio.sleep(d)` on the virtual loop → virtual
timers backed by DBOS durable sleep via `next_event`. `workflow.wait_condition(fn, timeout=...)`
as described in §4 (`asyncio.TimeoutError` on timeout, matching temporalio).

#### 6.1.4 Worker
See §5. Also: `Worker(activities=...)`-only workers (no workflows) are common — they must
register the `__temporal_activity` dispatcher + queue and the activity registry only.
`Replayer` (`temporal_dbos.worker.Replayer`) re-executes a run's DBOS step checkpoints
under the currently-registered code to detect non-determinism: it forks the source run one
step past its last checkpoint (copying every recorded step) and re-runs it, mapping DBOS's
`DBOSUnexpectedStepError` (plus an interpreter guard for "new step past the horizon" and
"finished early") to `workflow.NondeterminismError`. Histories come from
`WorkflowHandle.fetch_history()` (DB-bound; no offline JSON in v1 — DEVIATIONS D27).

### 6.2 Client, handles, status, visibility

- `client.start_workflow(...)` → resolve run-chain id (§6.4), `SetWorkflowID(dbos_id)` +
  `SetWorkflowTimeout(run_timeout)` + enqueue `__temporal_workflow` on DBOS queue
  `task_queue` (use `SetEnqueueOptions(delay_seconds=start_delay, priority=...)`).
  `start_signal=` (signal-with-start) → idempotent start (USE_EXISTING semantics) + send.
  `request_id`/`request_eager_start` accepted and ignored. Returns `WorkflowHandle`.
  `execute_workflow` = start + `await handle.result()`.
- `handle.result(follow_runs=True)` → `DBOS.retrieve_workflow(...).get_result()` (async
  variants throughout), following continue-as-new / workflow-retry chain links (§6.4).
  Failure surfacing: raise `WorkflowFailureError` with `.cause` reconstructed from the
  stored failure envelope (`ApplicationError` / `CancelledError` / `TerminatedError` /
  `TimeoutError` / `ChildWorkflowError`...). DBOS stores the original exception; the
  dispatcher must wrap workflow failures into a stable serialized failure format
  (`_internal/payloads.py`) so reconstruction is exact, including `.type`, `details`,
  `non_retryable`.
- `handle.signal/query/execute_update/start_update` → inbox envelopes (§4). Update result
  via `DBOS.get_event(dbos_id, f"__tdb_upd_{update_id}")`; rejection/failure encoded in the
  event payload → `WorkflowUpdateFailedError`. Update IDs: default `uuid4`; dedup via send
  `idempotency_key=update_id` + interpreter-side seen-set (rebuilt on replay).
  `WorkflowUpdateStage.ACCEPTED` vs `COMPLETED`: acceptance event
  (`__tdb_upd_{id}_accepted`) set after the validator passes; the validator's verdict is
  itself a checkpointed step (`__tdb_upd_validate`), so validators run exactly once and
  replay reads the recorded verdict — Temporal's semantics (acceptance lives in history;
  validators are skipped on replay). Query reply via a per-request
  event key. **DEVIATION:** a RUNNING workflow is queried directly; a *closed* workflow is
  served by rehydrate-by-replay — fork the run, replay its checkpoints to reconstruct final
  state, serve the one query against it (reusing the live query path), then discard the
  scratch run. Requires a worker for the type in the querying process (DEVIATIONS D27).
- `handle.cancel(reason)` / `handle.terminate(reason)` → §6.5.
- `handle.describe()` → synthesize `WorkflowExecutionDescription` from DBOS
  `get_workflow_status`. **Status mapping** (`_internal/status.py`):

  | DBOS | Temporal `WorkflowExecutionStatus` |
  |---|---|
  | PENDING / ENQUEUED / DELAYED | RUNNING |
  | SUCCESS (normal envelope) | COMPLETED |
  | ERROR, error is `SerializedContinueAsNew` | CONTINUED_AS_NEW |
  | ERROR, error is `_TemporalCancelledMarker` | CANCELED |
  | ERROR, error is `_TemporalTimedOutMarker` | TIMED_OUT |
  | ERROR (other) | FAILED |
  | CANCELLED (native DBOS cancel = terminate) | TERMINATED |
  | MAX_RECOVERY_ATTEMPTS_EXCEEDED | RUNNING (stuck; document) |

- `client.list_workflows(query)` / `count_workflows`: parse the common visibility-query
  subset — `WorkflowType =/!=/IN`, `ExecutionStatus =`, `WorkflowId =/STARTS_WITH`,
  `StartTime/CloseTime` ranges, `AND` — into DBOS `list_workflows` filters
  (`name`, `status`, `workflow_id_prefix`, `start_time`, `end_time`, ...). Note: the DBOS
  `name` will be `__temporal_workflow` for everything, so the Temporal workflow type must be
  filterable another way — store the temporal type name in the DBOS workflow's
  *deduplication-free* metadata: simplest v1 is filtering client-side on the input envelope
  (`load_input=True`); investigate whether DBOS supports a custom name per dispatched call
  (e.g., `DBOS.workflow(name=...)` registration per type at registration time — i.e.,
  register one thin dispatcher *per workflow type*, named `wf:{type}`, instead of one
  global dispatcher; this keeps by-name filtering and per-type listing native and is the
  **recommended** approach; same for activities `act:{type}`). Reject unsupported query
  constructs with a clear error listing what is supported. Phase 3.
- Memo & search attributes: carried in the input envelope; `upsert_memo`/
  `upsert_search_attributes` checkpoint into workflow events (`__tdb_memo`, `__tdb_sa`);
  readable via describe; **DEVIATION:** not indexed/queryable in v1.

### 6.3 Exceptions (`temporal_dbos.exceptions`)

Re-create the full tree with exact constructors (copy from
`/home/peter/competitors/temporal-sdk-python/temporalio/exceptions.py`):
`TemporalError → FailureError → ApplicationError / CancelledError / TerminatedError /
TimeoutError / ServerError / ActivityError / ChildWorkflowError /
ActivityAlreadyStartedError / WorkflowAlreadyStartedError`, plus enums `TimeoutType`,
`RetryState`, `ApplicationErrorCategory`, helper `is_cancelled_exception`.

Wrapping conventions to honor everywhere:
- Activity failure in a workflow surfaces as `ActivityError` whose `__cause__` is the
  converted original error — an `ApplicationError` with `.type` set to the original
  exception class name and args in `details`. Build a bidirectional failure
  serializer in `_internal/payloads.py` (exception → JSON envelope → exception) and use it
  for activity results, workflow results, and child-workflow errors. Map DBOS's
  `DBOSMaxStepRetriesExceeded` into the proper `ActivityError(retry_state=MAXIMUM_ATTEMPTS_REACHED)`.
- Child workflow failure → `ChildWorkflowError` ← cause as above.
- Awaiting a cancelled workflow (`DBOSAwaitedWorkflowCancelledError`) →
  `WorkflowFailureError(cause=CancelledError)` at the client, `ChildWorkflowError` in a parent.
- Client `start_workflow` on a running duplicate with conflict policy FAIL →
  `WorkflowAlreadyStartedError` (DBOS signal for this: `DBOSConflictingWorkflowError` /
  status check; note DBOS `SetWorkflowID` default behavior is idempotent-return =
  Temporal `USE_EXISTING`, so FAIL needs an explicit pre-check + race-safe verify; see §6.4).

### 6.4 Workflow IDs, run IDs, reuse/conflict policies, continue-as-new, workflow retries, cron

DBOS allows exactly one execution per DBOS workflow id, ever; Temporal allows reusing a
workflow id across *closed* runs, each run having a distinct `run_id`, with chains formed by
continue-as-new / retries / cron.

Scheme (`_internal/ids.py`):
- DBOS id for run *n* of Temporal id `W`: `W` for n=0, else `W--r{n}` (pick a separator
  unlikely to collide; reject user workflow ids containing it, or escape). `run_id` exposed
  to users = the DBOS id of that run (stable, unique — synthesizing UUIDs adds nothing).
- "Current run" lookup = exact-id probing (`ids.resolve_latest_run`): run ids are
  deterministic and dense, so resolution is "largest n where `W--r{n}` exists" — one
  batched `list_workflows(workflow_ids=[...])` primary-key lookup for chains ≤16 runs,
  O(log) batches beyond. **Never** `workflow_id_prefix` (unindexed scan in DBOS; unsafe
  on the send/query/result critical path). Used by `get_workflow_handle(W)` without
  run_id and by the interpreter's external-handle resolution (where each probe is a
  checkpointed management call, so the adaptive sequence replays deterministically).
- **Conflict policies** (vs a RUNNING run): `USE_EXISTING` → DBOS's natural idempotent
  start. `FAIL` (Temporal default behavior) → check current run status; if running, raise
  `WorkflowAlreadyStartedError`. There is an inherent TOCTOU window — accepted for v1
  (document); a `__tdb_chain` claim row via a tiny step can close it later.
  `TERMINATE_EXISTING` → native cancel then start next run. **Reuse policies** (vs closed
  runs): `ALLOW_DUPLICATE` → next n. `ALLOW_DUPLICATE_FAILED_ONLY` / `REJECT_DUPLICATE` →
  check last run's terminal status first. Phase 3 for the exotic ones; `USE_EXISTING`+
  `FAIL`+`ALLOW_DUPLICATE` in Phase 1.
- **Continue-as-new (done)**: `workflow.continue_as_new(...)` raises `ContinueAsNewError`
  (a `BaseException`, same as temporalio); the interpreter treats it as a terminal
  outcome. At close it enqueues run n+1 (same task queue unless overridden, deterministic
  next run id so a crash replays into an idempotent re-attach, fresh empty checkpoint
  history — this is what resets DBOS step-history growth, the same problem CAN solves in
  Temporal), forwards carryover messages (buffered signals, an outstanding cancel
  request, then checkpointed `recv(0)` drains of unconsumed inbox messages — all
  replay-stable), and records the `SerializedContinueAsNew` marker as the run's "error"
  (mirroring the cancellation marker; status maps to CONTINUED_AS_NEW).
  `handle.result(follow_runs=True)` follows markers; `follow_runs=False` raises
  `WorkflowContinuedAsNewError`; the child-result step follows chains so parents see the
  final run's result, and ParentClosePolicy sweeps, in-flight child cancellation, and
  `ChildWorkflowHandle.signal` all resolve each child's *current* run (a child that
  continued as new must be reached at its live run, not its closed first run).
  `terminate()` is chain-aware: a run-bound terminate on a closed run raises instead of
  clobbering its recorded marker (DBOS cancel overwrites terminal statuses), and an
  unbound terminate follows continue-as-new hops — successors are identified by their
  same-chain parent link (a reuse-created successor is a new logical execution and is
  left alone) — so terminating a CAN-looping entity workflow converges instead of
  silently missing. Accepted updates abandoned at any terminal outcome get a failure
  reply (`AcceptedUpdateCompletedWorkflow`, as in Temporal) instead of leaving callers to
  time out. Client reply waits (update acceptance/result, query replies) walk
  the chain on a miss: a forwarded update/query is answered under the *new* run's id, not
  the one the client originally targeted (reply keys are globally unique, so a chain
  sweep is unambiguous). Handles from `start_workflow` are not run-bound (temporalio
  semantics): signals/queries/updates resolve the chain's current run per call, which is
  what keeps them routing correctly across continue-as-new; `result()` anchors on
  `result_run_id` (the started run) and follows forward. `is_continue_as_new_suggested()` is a threshold on the checkpoint
  cursor (`TEMPORAL_DBOS_CAN_SUGGESTION_THRESHOLD`, default 10000, mirroring Temporal's
  ~10k-events scale). `workflow.info().continued_run_id` is the run's DBOS parent link
  when it points within the same chain (DBOS threads `parent_workflow_id` for in-workflow
  starts, which a CAN enqueue is; real parents point at a different chain base and
  client-side reuse starts carry no link).
- **Workflow retry_policy (done)** (workflows do NOT retry by default — match that): on
  failure, the dispatcher consults the policy (serialized into the run's input
  meta-envelope) and enqueues run n+1 with attempt+1 (visible in
  `workflow.info().attempt`), honoring backoff via `SetEnqueueOptions(delay_seconds=...)`.
  The failed run's failure envelope records `new_run_id`, so
  `result(follow_runs=True)` follows retries exactly as temporalio follows
  `new_execution_run_id` on a failure event; the next attempt sees the failure via
  `workflow.get_last_failure()`. Cancelled/terminated-classified failures are never
  retried, and timeout failures retry only for start-to-close/heartbeat types
  (Temporal's isRetryable). Retries trigger on workflow *failures*; run-timeout
  retries land with the TIMED_OUT marker work. A cancel requested around the close
  suppresses further attempts. `run_timeout` is carried in the meta-envelope and
  re-applied per successor run (an explicit timeout on an enqueued workflow gets its
  deadline at dequeue) — without that, DBOS propagates the closing run's *absolute*
  deadline to in-workflow-started children, and a backed-off attempt could be born
  expired. The same carry applies across cron hops and continue-as-new (whose own
  run_timeout/retry_policy arguments override the carried values).
- **Cron (done)** (`start_workflow(cron_schedule=...)`): implemented as **delayed-enqueue
  chain hops**, not DBOS schedule rows (revising the original sketch — the
  continue-as-new chain machinery made this strictly simpler and more faithful): the
  start enqueues run 0 immediately with `delay_seconds` to the next cron occurrence
  (Temporal's first-workflow-task backoff: the execution exists at once, so
  describe/signal/result work before the first fire, and DELAYED maps to RUNNING), and
  each close enqueues run n+1 delayed to the next occurrence after close time (a run
  that overruns an occurrence skips it, as in Temporal). No scheduler-poll latency, no
  schedule-row lifecycle to clean up on cancel/terminate, and recovery rides the same
  idempotent re-attach as continue-as-new. `get_last_completion_result()` /
  `get_last_failure()` thread the previous run's outcome forward in the meta-envelope
  (no DB walk). Cron continues after failed runs (retry policy, when present, takes
  precedence per attempt); a cancel observed by the run — or found undelivered at the
  hop, for runs that never park — ends the chain; unconsumed inbox messages otherwise
  carry over to the successor like the CAN carryover. Cron evaluation uses DBOS's
  vendored croniter: 5-field UTC with `CRON_TZ=`/`TZ=` prefixes, plus 6/7-field
  (seconds/year) accepted as an extension (DEVIATIONS D19).

### 6.5 Cancellation matrix

| Temporal action | Implementation |
|---|---|
| `handle.cancel(reason)` (cooperative) | Inbox `{kind:"cancel", reason}`. Interpreter raises `asyncio.CancelledError` into the main task at the next event boundary. User `except/finally` cleanup runs and **may still execute activities** (Temporal allows this — our interpreter keeps servicing `next_event` during unwind; only `asyncio.shield`-style semantics need care). When unwind completes, dispatcher records `_TemporalCancelledMarker` failure → status CANCELED. If user code swallows the cancel and returns normally → COMPLETED (Temporal allows that too). `workflow.cancellation_reason()` returns the reason. |
| `handle.terminate(reason)` (forceful) | Native `DBOS.cancel_workflow(dbos_id, cancel_children=True)`. No workflow code runs. Status TERMINATED. Awaiters get `WorkflowFailureError(cause=TerminatedError)`. |
| Workflow cancels an in-flight activity (`activity_handle.cancel()` or workflow cancel propagation) | Step path: set the cancel flag the heartbeat checks + cancel the asyncio task per `ActivityCancellationType` (TRY_CANCEL: request and resolve handle with `CancelledError` immediately; WAIT_CANCELLATION_COMPLETED: wait for the step task to actually finish; ABANDON: detach). Queued path: `DBOS.cancel_workflow(activity_id)` + same type semantics. |
| Child workflow cancellation | Per `ChildWorkflowCancellationType` (default WAIT_CANCELLATION_COMPLETED): inbox-cancel the child, optionally await terminal status. `ParentClosePolicy`: TERMINATE (default) → on parent terminal, native-cancel remaining children (DBOS `cancel_children=True` helps for terminate; for normal completion the dispatcher sweeps non-ABANDON children); ABANDON → leave running; REQUEST_CANCEL → inbox-cancel without waiting. |
| Activity-side observation | `activity.is_cancelled()`, `wait_for_cancelled()`, `cancellation_details()`, heartbeat-delivered `CancelledError` per §6.1.2. **DEVIATION:** like Temporal, cancellation reaches activities only via heartbeat — but our checking frequency is heartbeat-call-driven, no server-side timeout enforcement between heartbeats in v1. |

### 6.6 Child workflows & external handles

`workflow.start_child_workflow(wf, ..., id=..., task_queue=...)` → from inside the
interpreter, enqueue the child's dispatcher with `SetWorkflowID(child_dbos_id)`
(default id: `{parent_temporal_id}_{seq}` — note Temporal's default is a server-generated
UUID; deterministic-from-parent is required by DBOS's model and is strictly more useful;
**DEVIATION** only if user code asserts UUID format). Returns a `ChildWorkflowHandle` whose
await resolves when the child *starts* (mirror that: resolve after enqueue checkpoint), and
which surfaces results through `next_event`. `.signal()` → `DBOS.send` to child inbox.
`workflow.get_external_workflow_handle(id)` → `.signal()/.cancel()` via inbox send (resolve
chain to current run first); these sends are checkpointed steps from the parent's
perspective.

### 6.7 Schedules (done)

Back `client.create_schedule(id, Schedule(...))` with DBOS schedules
(`DBOS.create_schedule(schedule_name=..., workflow_fn=<schedule-fire dispatcher>,
schedule=<cron>, context=<serialized ScheduleActionStartWorkflow>, cron_timezone=...,
queue_name=...)`). Compile `ScheduleSpec`: `cron_expressions` pass through;
`intervals=[every=timedelta]` → cron where the period divides evenly (else nearest-cron +
**DEVIATION** note); calendar specs → cron fields; `start_at/end_at/skip/jitter` enforced
inside the fire dispatcher (it checks bounds, applies seeded jitter sleep, then starts the
action workflow). Overlap policies: fire dispatcher derives the action workflow id
deterministically from the nominal fire time → SKIP = idempotent duplicate-start no-op;
BUFFER_ONE/ALLOW_ALL via id suffixing + a small state event; CANCEL_OTHER/TERMINATE_OTHER
via cancel-then-start. `ScheduleHandle.pause/unpause/trigger/backfill/delete/describe/update`
→ DBOS `pause_schedule/resume_schedule/trigger_schedule/backfill_schedule/delete_schedule/
get_schedule` (+ re-create for update). **Done** as sketched, with v1 scope cuts
(DEVIATIONS D22): the `ScheduleSpec` compiles to one cron (intervals dividing a boundary
are exact, others approximate; calendar `year`/interval `offset` dropped); overlap honors
SKIP/CANCEL_OTHER/TERMINATE_OTHER/ALLOW_ALL (the fire dispatcher finds a still-running
prior via an indexed `schedule_name` lookup over recent fires — DBOS tags every fire with
the schedule name — then probes their per-occurrence action ids in one batch) but
rejects BUFFER_ONE/BUFFER_ALL; `update` is delete-then-recreate; schedule history and
`memo`/`search_attributes` aren't tracked. The action + full spec ride in the schedule's
DBOS `context` (so describe/list/update reconstruct them); the fire dispatcher
(`__temporal_schedule_fire`) is one process-global DBOS workflow. The
samples-python `schedules/` corpus runs unmodified (conformance suite).

### 6.8 Determinism helpers, info, versioning, interceptors

- `workflow.info()` → synthesized `Info` (frozen dataclass, all fields): ids from §6.4,
  `attempt`, `task_queue` = DBOS queue name, `start_time` from status `created_at`,
  `parent`/`root` from DBOS parent links, `search_attributes`/`memo` from envelope,
  `get_current_history_length()` → interpreter seq count.
- **Worker deployment versioning (done).** `common.VersioningBehavior`,
  `WorkerDeploymentVersion`, `VersioningOverride`/`PinnedVersioningOverride`/
  `AutoUpgradeVersioningOverride`, `worker.WorkerDeploymentConfig`,
  `@workflow.defn(versioning_behavior=)`, `continue_as_new(initial_versioning_behavior=)`
  (`ContinueAsNewVersioningBehavior`), and client `start_workflow(versioning_override=)` are
  mirrored. A *build ID is the DBOS `application_version`*: `Worker(build_id=)` /
  `Worker(deployment_config=)` set it, and it's surfaced via
  `Info.get_current_deployment_version()` / `get_current_build_id()`. Because DBOS scopes
  recovery and queue dequeue to `application_version`, **PINNED is enforced** — a workflow is
  recovered/continued only on workers of its build ID and never auto-migrates. **AUTO_UPGRADE**
  (move a running workflow to a newer version) and cluster ramping/routing have no DBOS
  analog and degrade to pinned; `is_target_worker_deployment_version_changed()` is always
  `False`. See DEVIATIONS D29.
- **Current details (done).** `workflow.set_current_details()`/`get_current_details()` back
  free-form UI/CLI metadata as in-memory workflow state, reconstructed on replay (no
  checkpoint); settable on the deterministic loop (run/handlers), rejected in read-only
  contexts, not surfaced to `describe()` in v1. See DEVIATIONS D30.
- **Activity context helpers (done).** `activity.is_worker_shutdown()` /
  `wait_for_worker_shutdown()` / `wait_for_worker_shutdown_sync()` read a *per-Worker* shutdown
  event (fresh per Worker, latched once the Worker trips it on `shutdown()`, so a straggler
  activity never observes a later Worker's flag); `ActivityEnvironment.worker_shutdown()` trips
  the env's own event for tests. `activity.shield_thread_cancel_exception()` is a no-op
  (cancellation is cooperative — DEVIATIONS D26); `activity.client()` returns a Temporal client
  lazily built from the Worker's `DBOSConfig` (or one passed to `ActivityEnvironment(client=)`).
  The per-Worker shutdown event + lazy client live on one `_ActivityWorkerState` object whose
  lifecycle is bound to the Worker run (built at construction, disposed off-loop after the
  graceful drain).
- `workflow.patched(id)` / `deprecate_patch(id)`: **done.** Checkpoint-marker
  implementation. The decision (`not is_replaying() or id in recorded-markers`) is computed
  synchronously at the call site, memoized per id, and — crucially — claims **no**
  function_id; only when the newer path is taken is a marker durably written (a
  `@DBOS.step(name="__tdb_patch")` whose output is the id, queued through the command queue
  so it lands at a deterministic checkpoint position). First execution records the marker and
  takes the newer path; replay of a pre-patch history finds no marker, returns `False`, and —
  because the False decision consumed no position — replays the older path with its original
  checkpoint sequence intact. The recorded-marker set is rebuilt at run start by scanning the
  step list **by id** (set membership, like temporalio's `NotifyHasPatch`), not by position,
  so it survives code that shifts checkpoints. We do **not** use DBOS's native
  `patch_async` (`enable_patching` in `dbos/_dbos_config.py`): it is async and assumes
  function_id == sequential code position, which our virtual-loop/command-queue split
  decouples, and it pins `app_version` to `"PATCHING_ENABLED"`. See `_internal/interpreter.py`
  (`_patch`, `_patch_marker`, the `execute()` scan) and [DEVIATIONS.md](DEVIATIONS.md) D28.
- `workflow.unsafe.*`: `imports_passed_through()` → no-op context manager; `is_replaying()`
  real (§4.2); `in_sandbox()` → False; the rest no-ops.
- Interceptors (client `Interceptor/OutboundInterceptor`, worker
  `ActivityInbound/Outbound`, `WorkflowInbound/Outbound`): clean fit — wrap dispatcher entry
  points and client verbs with the same `*Input` dataclasses (copy dataclass definitions
  from the SDK). **Client + activity + workflow done.** Client outbound roots in a
  `_ClientOutbound`; the activity chain is built per attempt in `_internal/activities.py`;
  the workflow chains are built per execution in `_internal/interpreter.py`
  (`_RootWorkflowInbound`/`_RootWorkflowOutbound`, advertised via the worker
  `Interceptor.workflow_interceptor_class`). Header-based context propagation works
  end-to-end: a header set at the client (or on a workflow outbound `*Input`) flows into
  `ExecuteWorkflowInput`/`Handle*Input` and on to activities, children, and signalled
  workflows, riding the run meta-envelope (`RunMeta.headers`) and inbox envelopes as
  payload dicts; `workflow.payload_converter()`/`activity.payload_converter()` encode the
  values. `handle_query`/`handle_update_validator` are driven synchronously (queries are
  sync, #11). See [DEVIATIONS.md](DEVIATIONS.md) D24.

### 6.9 Data conversion

Mirror `temporalio.converter`: `DataConverter(payload_converter_class, payload_codec,
failure_converter_class)`, `CompositePayloadConverter`, `JSONPlainPayloadConverter`
(dataclasses, enums, UUID, datetime; pydantic models are **not** supported out of the
box — `contrib.pydantic` is not provided, configure a custom `DataConverter` for them),
`PayloadCodec`
(async encode/decode of byte payloads — encryption/compression). Implementation: an adapter
that turns the configured `DataConverter` into a DBOS custom `Serializer`
(`dbos` exposes a `Serializer` protocol and `DBOSPortableJSONSerializer`), applied to
workflow inputs/outputs, activity payloads, inbox envelopes, and events. The internal
envelope structure (`_internal/payloads.py`) wraps user payloads:
`{type_name, payloads: [...], metadata}` so type hints survive and codecs see exactly the
user-payload bytes. All of this is Phase 3 (default JSON was moved out of Phase 1: nothing
in the hello conformance corpus needs it, and the pipeline should be built once, together
with codecs). **Done:** the converter, the per-boundary conversion, and the JSON DBOS
serializer (`_internal/serializer.py`) all shipped; pickle is gone (see DEVIATIONS D21).
The realized envelope is a small payload dict (`{encoding, json|b64, meta?}`,
`_internal/conversion.py`) — readable on disk — rather than the `{type_name, payloads}`
sketch above; type hints come from signatures, codecs run at the async boundaries.

### 6.10 Testing module (`temporal_dbos.testing`)

- `ActivityEnvironment`: pure in-memory (no DB) — trivial port; do it in Phase 1 (cheap
  goodwill, used by many test suites).
- `WorkflowEnvironment.start_local()` (done, Phase 2): a uniquely named throwaway
  database per environment on an env-provided Postgres server (never launches one),
  dropped on `shutdown()`. Exposes `dbos_config` (DBOS-native extension) to build the
  env's Worker from. Note: DBOS's async APIs install DBOS's pool as the calling loop's
  default executor and destroy shuts it down without restoring; since DBOS adopts the
  Worker's loop, the Worker restores a live default executor on exit so the app's
  `asyncio.to_thread` keeps working (regression-tested; upstream-worthy: DBOS could
  restore it in destroy).
- `WorkflowEnvironment.start_time_skipping()`: **out of scope** (cut from Phase 4 — see §9).
  `start_time_skipping` raises `NotImplementedError` and `supports_time_skipping` is `False`;
  tests use the real-time `start_local` env. (Sketch if ever revisited: a test-mode clock
  service — when all interpreters in the env are parked on timers (no inbox/activity
  waiters), find the earliest deadline and fast-forward by rewriting pending sleep deadlines
  in the system DB, or inject a scaled clock into `next_event`; `env.sleep(n)` advances
  manually.)

---

## 7. DBOS primitives quick reference (verified against source, June 2026)

What the shim builds on — exact names; full signatures in `/home/peter/dbos-transact-py/dbos/`:

- **Decorators**: `@DBOS.workflow(name=, max_recovery_attempts=)`,
  `@DBOS.step(retries_allowed=, interval_seconds=, max_attempts=, backoff_rate=, should_retry=, preemptible=)`.
  Async functions supported throughout (`*_async` variants of all verbs below).
- **Start/enqueue**: `DBOS.start_workflow(fn, *args)`, `Queue(name, concurrency=, worker_concurrency=, limiter=, priority_enabled=, partition_queue=).enqueue(fn, *args)`,
  `DBOS.register_queue(...)`; context managers `SetWorkflowID(id)`,
  `SetWorkflowTimeout(sec)`, `SetEnqueueOptions(deduplication_id=, priority=, app_version=, delay_seconds=)`.
  Duplicate `SetWorkflowID` start = idempotent attach to existing execution.
- **Handles**: `WorkflowHandle.get_result()/get_status()`, `DBOS.retrieve_workflow(id)`,
  `DBOS.wait_first(handles)`, `DBOS.get_workflow_status(id)`.
- **Messaging**: `DBOS.send(dest_id, msg, topic, idempotency_key=)`,
  `DBOS.recv(topic, timeout_seconds)` (checkpointed, ordered, durable),
  `DBOS.set_event(key, value)` / `DBOS.get_event(wf_id, key, timeout_seconds)`,
  streams: `DBOS.write_stream/read_stream/close_stream`.
- **Time**: `DBOS.sleep(seconds)` (durable), `DBOS.asyncio_wait(fs, timeout=, return_when=)`
  (evaluate in Phase 0, §4.2).
- **Management**: `DBOS.list_workflows(...)` (rich filters incl. `workflow_id_prefix`,
  `status`, `name`, `parent_workflow_id`, time ranges), `DBOS.list_workflow_steps(id)`,
  `DBOS.cancel_workflow(id, cancel_children=)`, `DBOS.resume_workflow(id)`,
  `DBOS.fork_workflow(id, start_step, ...)`.
- **Schedules**: `DBOS.create_schedule(schedule_name=, workflow_fn=, schedule=<cron>,
  context=, cron_timezone=, queue_name=)`, `list/get/delete/pause/resume_schedule`,
  `backfill_schedule(name, start, end)`, `trigger_schedule(name)`.
- **Client mode**: `DBOSClient(system_database_url=...)` — `enqueue(EnqueueOptions, *args)`
  (by workflow *name string* — this is why the dispatcher-per-type registration matters),
  `send`, `get_event`, `retrieve_workflow`, `list_workflows`, `cancel_workflow`, schedule
  CRUD. No code registration needed.
- **Lifecycle**: `DBOS(config=DBOSConfig(name=, system_database_url=, ...))`,
  `DBOS.launch()` (starts queue listeners + recovers PENDING workflows for this
  app_version/executor), `DBOS.destroy(workflow_completion_timeout_sec=)`,
  `DBOS.reset_system_database()` (tests only). `application_version` / `executor_id`
  config + classproperties.
- **Errors to map** (`dbos/_error.py`): `DBOSWorkflowCancelledError` (BaseException, raised
  inside a cancelled workflow), `DBOSAwaitedWorkflowCancelledError` (awaiting one),
  `DBOSMaxStepRetriesExceeded` (has `.errors` list), `DBOSQueueDeduplicatedError`,
  `DBOSConflictingWorkflowError`, `MaxRecoveryAttemptsExceededError`,
  `DBOSUnexpectedStepError` (= nondeterminism detected on replay → surface as our
  `NondeterminismError`).
- **Workflow status strings**: `PENDING, SUCCESS, ERROR, CANCELLED, ENQUEUED, DELAYED,
  MAX_RECOVERY_ATTEMPTS_EXCEEDED` (mapping table in §6.2).

DBOS semantics the design depends on (verify each in Phase 0 with a test, they are
foundational): (1) `recv` is checkpointed and replays the same message and ordering;
(2) child-workflow `get_result` from within a workflow is checkpointed; (3) steps may run
concurrently as asyncio tasks inside an async workflow and each checkpoints independently;
(4) recovery re-executes the workflow function with completed steps returning recorded
results in call order; (5) `SetWorkflowID` duplicate-start attaches idempotently.

---

## 8. Known deviations (maintain as README table from day one)

1. No Temporal server/UI/CLI; no non-Python clients. Operate via DBOS tooling.
2. Queries hit Postgres and (v1) require a RUNNING workflow; no queries on closed workflows
   until rehydrate-by-replay lands.
3. No workflow sandbox: determinism violations surface at recovery/replay as
   `NondeterminismError` rather than at development time. (Mitigation idea, Phase 4+:
   optional dev-mode double-execution lint.)
4. Visibility query language: documented subset; custom search attributes stored but not
   indexed.
5. Default child-workflow IDs are derived from the parent, not random UUIDs.
6. `FAIL` id-conflict policy has a small TOCTOU window in v1.
7. Latency/throughput profile differs (every effect = a Postgres write; no sticky-cache
   command batching). Publish benchmarks rather than hiding this.
8. Workflow/activity payloads stored in the DBOS system database (size limits = Postgres
   practical limits, not Temporal's 2MB/4MB caps — we do not enforce Temporal's caps).

---

## 9. Roadmap with exit criteria

**Phase 0 — Interpreter spike (de-risk). (COMPLETE)** `_internal/interpreter.py`,
`inbox.py`, minimal dispatcher; the §4.3 test list green under real Postgres including
SIGKILL-recovery tests; written answer to the `next_event`-vs-`asyncio_wait` question
(build on `asyncio_wait` — see `docs/phase0.md`); perf baseline recorded in `docs/perf.md`.

**Phase 1 — Core happy path. (COMPLETE)** `@workflow.defn/run`, `@activity.defn`,
`execute_activity`/`start_activity` (timeouts + RetryPolicy + ActivityError wrapping),
`workflow.sleep`, `Client.connect/start_workflow/execute_workflow/get_workflow_handle`,
`handle.result/describe`, `Worker` + `async with`, exceptions module complete,
`ActivityEnvironment`. (Default JSON data conversion moved to Phase 3 — see below.)
**Exit:** Temporal's hello-world quad and the `samples-python` `hello/` directory run
against `temporal_dbos` with the import swap plus connection-setup adaptation (§5 revised);
samples needing later-phase features are xfail-tagged with their blocking phase in the
conformance suite. Achieved: 11/19 runnable hello samples pass (table in README).

**Phase 2 — Message passing & process model. COMPLETE.** Signals (incl. buffering +
signal-with-start), queries (active), updates + validators + update-with-start,
`wait_condition`, deterministic helpers (`now/uuid4/random/wait/as_completed`), child
workflows + external handles, cooperative cancel vs terminate (full §6.5 matrix),
`workflow.info()`, workflow-task-failure retry behavior, handler lifecycle
(`all_handlers_finished`, signal buffering, `HandlerUnfinishedPolicy` warn-on-exit),
query reject conditions, real `is_replaying()` (checkpoint horizon) + replay log
suppression, `WorkflowEnvironment.start_local`. **Exit:**
`samples-python` `message_passing/`, plus SIGKILL-during-everything chaos tests.
Achieved: 4/5 `message_passing/` samples pass (the fifth needs Phase 3 continue-as-new,
xfail-tagged); chaos suite covers SIGKILL mid-cancellation-unwind, mid-child,
mid-update-handler (accepted-but-parked), and the is_replaying probe.

**Phase 3 — Operational surface.** Schedules + cron + `start_delay` (cron done via
delayed-enqueue chain hops — flipped `hello_cron`, hello 16/19; `create_schedule` +
the full ScheduleHandle surface **done** — the samples-python `schedules/` corpus runs
8/8, DEVIATIONS D22), continue-as-new
(done: chain hops, carryover, follow_runs, child chains; flipped
`hello_continue_as_new` and `safe_message_handlers` — `message_passing/` is 5/5),
dynamic *signal/query/update* handlers + handler descriptions + dynamic activities
(**done**; dynamic *workflows* are out of scope — rejected with a clear
`NotImplementedError`, DEVIATIONS D25 / §10.1),
workflow retry policies (done: attempt counts, backoff, non-retryable types,
follow-on-failure, last-failure threading; the run input grew a meta-envelope that
carries per-run chain state — the same envelope later carries memo/search attributes),
id reuse/conflict policies, heartbeats + activity
cancellation types + async activity completion (done: heartbeat-delivered
cancellation incl. sync activities, WAIT_CANCELLATION_COMPLETED,
heartbeat-timeout enforcement via an in-process watchdog (TimeoutType.HEARTBEAT,
retried per policy), in-memory heartbeat details across attempts,
`raise_complete_async` + task-token or workflow_id/activity_id-reference
completion via inbox envelopes, external fails consulting the retry policy,
start-to-close and heartbeat timeouts enforced while parked (durable-sleep
waiters; the heartbeat window counts envelopes between checkpointed timer
fires, no clock reads), and completers notified of workflow-side cancellation
via a checkpointed gone-event their heartbeat/complete polls
(AsyncActivityCancelledError) — flipped `hello_cancellation` and
`hello_async_activity_completion`, hello 15/19), `list_workflows` query parser +
`count_workflows`, client + activity interceptors (**done**: client outbound +
activity inbound/outbound, DEVIATIONS D24; workflow in/outbound is Phase 4), the
data-conversion pipeline
(**done**: default JSON conversion — moved from Phase 1 — plus custom DataConverters +
PayloadCodec, per-boundary conversion, and the JSON DBOS serializer that replaces pickle;
`contrib.pydantic` is out of scope — not provided, configure a custom DataConverter for
pydantic), memo/search-attribute storage. **Exit:** `samples-python` `schedules/`,
`activity_worker/`, expanded conformance matrix published in README.

**Phase 4 — Ecosystem & polish.** `Replayer` over
DBOS step checkpoints via `fork_workflow` (**done**: non-determinism verification +
`WorkflowHandle.fetch_history`, plus rehydrate-by-replay answering queries on closed
workflows — flipped `hello_query`, DEVIATIONS D27), `patched()`/versioning, workflow
interceptors (**done**: inbound/outbound + header propagation, DEVIATIONS D24),
perf work
(micro-checkpoint batching), signature-parity CI (introspect installed `temporalio` as a
dev-dep and diff public signatures against ours — this test is the API-drift alarm).
Final accepted-parameter audit: classify every accepted-and-ignored parameter as
honored / inert / pending (machine-checked like the parity ledgers); nothing may remain
"pending" at release, and every "inert" classification must be defensible — silent
ignores of behavior-changing parameters (e.g. `cron_schedule` before Phase 3) are the
failure mode this audit exists to catch.

**Cut from Phase 4 (out of scope):** the time-skipping `WorkflowEnvironment`
(`start_time_skipping` raises `NotImplementedError`, `supports_time_skipping` is `False`;
tests use the real-time `start_local` env) and the `temporalio` alias shim +
`python -m temporal_dbos run` runner — the import-root swap (§2.1) is the only supported
migration path.

Conformance harness (`tests/conformance/`) runs from Phase 1: clone `samples-python`,
rewrite imports mechanically (`temporalio` → `temporal_dbos`), run each sample's worker +
starter against ephemeral Postgres, assert outputs. The pass-rate table *is* the product
spec and the headline number.

---

## 10. Resolved design decisions (don't re-litigate without new information)

1. One thin registered DBOS workflow **per Temporal workflow type** (named `wf:{type}`),
   all delegating to the shared interpreter — not one global dispatcher — so DBOS-native
   listing/filtering by name works (§6.2). Same for activities (`act:{type}`).
2. Activities default to in-process step execution; queued-workflow execution only when the
   target task queue differs from the workflow's (§6.1.2).
3. `run_id` = the run's DBOS workflow id; run chains via `--r{n}` suffixes (§6.4).
4. Cooperative cancel via inbox + `_TemporalCancelledMarker`; native DBOS cancel is reserved
   for `terminate` (§6.5).
5. Non-failure workflow exceptions retry the "workflow task" in-process with capped backoff,
   keeping the workflow RUNNING — they do not fail the execution (§4.2).
6. Virtual time + seeded randomness checkpointed once per execution, not per call (§4.2).
7. Single ordered inbox topic per execution for all external messages; replies via
   `set_event` keyed by request id (§4.2).

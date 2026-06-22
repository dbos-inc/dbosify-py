# Architectural Differences from Temporal

<p align="center">
  <img src="architecture.png" alt="DBOSify architecture: a DBOSify Client and DBOSify Workers coordinate through Postgres, which handles workflow orchestration" width="720">
</p>

## Architecture

### no-server — No Temporal server, no wire protocol

There is no gRPC, Web UI, `temporal` CLI, or cross-language client — `dbosify` replaces *server + Python SDK* for Python-only apps, with each namespace mapped to its own Postgres schema (`dbosify_<namespace>`) and one namespace served per worker process.

### connection-surface — Connection surface takes DBOS machinery directly

`Client` wraps a `dbos.DBOSClient` and `Worker` takes a `dbos.DBOSConfig` (one worker per process), so migration means adapting connection setup, not just swapping imports.

### dbos-native-management — DBOS-native management bypasses the Temporal semantic layer

DBOS-native operations on the same workflows (Conductor, cancel, fork, resume) carry no Temporal semantics and render CANCELED / CONTINUED_AS_NEW runs as ERROR.

## Identity and runs

### run-ids — Run IDs are DBOS workflow IDs, with visible chain suffixes

`run_id` is the run's DBOS workflow id (`W` or `W--r{n}`), not an opaque UUID, and ids containing `--r` are rejected at start.

### child-ids — Child workflow IDs are deterministic, derived from the parent

A child's default id is the deterministic `{parent_id}_{seq}` rather than a server UUID (this is what lets recovery re-attach to an already-started child).

### cron-chains — Legacy cron workflows are run chains with per-run results

Legacy cron workflows are implemented as a run chain where `result()` returns the per-run result (Temporal's `follow_runs=True` never returns), between-run cancellation takes effect only at the next fire, 6/7-field cron is accepted, `start_delay` + `cron_schedule` raises, and an exceeded `run_timeout` ends the chain as TERMINATED instead of retrying it.

### retry-matching — Workflow-retry matching and carryover differ at the edges

Retry matching mirrors Temporal's except `non_retryable_error_types` also matches envelope failure classes (a superset), and inbox messages still unconsumed when a run fails carry over to the retry attempt.

## Process and operations model

### failover — Failover is restart-or-Conductor, not poller reassignment

A dequeued execution is pinned to its `executor_id`/`app_version` and resumes only when an equivalent executor relaunches (or via management action from DBOS Conductor), and a mid-activity crash re-runs the same attempt number with empty `heartbeat_details`.

### start-policies — Start policies are enforced client-side, with TOCTOU windows

ID-conflict / id-reuse start policies are check-then-start from the client, leaving small race windows (but signal-/update-with-start are atomic).

### blocking-stalls-worker — Blocking workflow code stalls the whole worker

Blocking sync code in a workflow body stalls every workflow on the worker's shared asyncio loop, not just one workflow task as in Temporal.

### no-multiprocess-activities — No multiprocess activity executors

`SharedStateManager` / `ProcessPoolExecutor` activities are unsupported; sync activities run on threads via `asyncio.to_thread`.

## Request/response semantics

### durable-messages — Signals, cancels, and updates are durable messages, not RPCs

Because there is no server to validate targets, sends to closed workflows are silent no-ops (not "already completed"), ops on nonexistent workflows raise a DB/`RuntimeError` (not `NOT_FOUND`), exhausted waits raise builtin `TimeoutError` (not `RPCError`), and signals/cancels are not request-deduplicated.

### terminate-no-reason — Terminate stores no reason or details

`handle.terminate(reason=...)` accepts and drops the reason; awaiters get a generic `TerminatedError`.

### async-activity-cancel — Async activities are *more* cancellable than Temporal's

Async activities are cancelled at their next `await` regardless of heartbeating (more eagerly than Temporal) and surface `asyncio.CancelledError`.

### sync-activity-cancel — Sync-activity cancellation is cooperative-only

A sync activity observes cancellation only cooperatively (at `heartbeat()` / `is_cancelled()`), always behaving as `no_thread_cancel_exception=True`. As a consequence, `@activity.defn(no_thread_cancel_exception=)` defaults to `True` and `False` is rejected.

### sync-queries — Query handlers are synchronous-only

`@workflow.query` rejects `async def` handlers at definition time, where temporalio still accepts them with a deprecation warning.

## Determinism and data

### no-sandbox — No workflow sandbox

Determinism violations surface as nondeterminism errors at replay rather than being caught at development time.

### uncapped-payloads — Payloads live in the system database, uncapped

Payloads and history length are uncapped Postgres rows — Temporal's 2MB/4MB payload and ~50k-event limits are not enforced.

### memo-search-attributes — Query language subset supported

`list_workflows`/`count_workflows` parse a subset of the Temporal visibliity query language. These field/operator pairings are supported: `WorkflowType` (`=`, `!=`, `IN`), `WorkflowId` (`=`, `STARTS_WITH`), `ExecutionStatus` (`=`, `IN`), `StartTime`/`CloseTime` (`=`, `>`, `>=`, `<`, `<=`), and custom search attributes (`=` only, an exact-match containment), plus an optional trailing `GROUP BY ExecutionStatus|WorkflowType` on `count_workflows`.
Operators may be conjoined with `AND` only.
Additionally, memos and search attributes are untyped.

### json-conversion — Data conversion: JSON transport, no protobuf payloads

Payloads convert through a temporalio-shaped `DataConverter` to readable JSON with no protobuf encoders (a raw protobuf payload fails loudly), and a `PayloadCodec` is skipped on failure `details` / `last_heartbeat_details` / cron `last_completion`.

### schedules — Schedules compile to a single cron; overlap and history are partial

A `Schedule` compiles to one DBOS cron (non-dividing intervals approximated; calendar `year` / interval `offset` dropped), honors SKIP/CANCEL_OTHER/TERMINATE_OTHER/ALLOW_ALL but rejects BUFFER_ONE/BUFFER_ALL, makes `update` a delete-then-recreate, and tracks no schedule history.

### replay — Replay and queries-on-closed run over DBOS checkpoints, in-process

`Replayer` / `fetch_history` are DB-bound (no offline JSON history), and queries on closed workflows use rehydrate-by-replay, which needs a worker for that type in the querying process.

### dynamic-handlers — Dynamic handlers and activities are supported; dynamic workflows are not

Dynamic signal/query/update handlers and dynamic activities work, but dynamic *workflows* (`@workflow.defn(dynamic=True)`) raise `NotImplementedError`.

### worker-versioning — Worker deployment versioning = DBOS versioning: PINNED is enforced, AUTO_UPGRADE is not

A build ID *is* the DBOS `application_version`, so PINNED is enforced (a workflow recovers/dequeues only on its build ID) while AUTO_UPGRADE, ramping, and cluster routing have no analog and degrade to pinned.

### current-details — Current details are in-memory, reconstructed on replay, not in describe()

`set_current_details()` / `get_current_details()` are in-memory state reconstructed on replay, settable only on the deterministic loop and not surfaced to `describe()` / `list_workflows`.

### random-seed — Random seed is fixed per run; reseed callbacks never fire

The per-run random seed never changes, so any `register_random_seed_callback` is stored but never invoked.

### activity-cancel-details — Activity cancellation details are not tracked

`activity.cancellation_details()` always returns `None` — the structured cancellation reason temporalio surfaces is not recorded.

### no-metrics — Metrics and the telemetry `runtime` module are not yet provided

Metrics are not implemented: `metric_meter()` raises `AttributeError` and the entire `temporalio.runtime` telemetry module is absent.

### start-params — Some start parameters are accepted but not yet enforced

`start_child_workflow.cron_schedule`, `start_child_workflow.id_reuse_policy`, and `start_workflow.execution_timeout` are accepted (and debug-logged) but not yet enforced.

### no-client-activities — Client-initiated (standalone) activities are not supported

`Client.start_activity` / `get_activity_handle` / `list_activities` and their supporting types are not yet supported.

### dynamic-handler-signature — Dynamic signal/query/update handlers require the new-style signature

A `dynamic=True` handler must use `(self, name: str, args: Sequence[RawValue])`; the legacy `(self, name, *args)` form is rejected at registration.

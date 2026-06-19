# Fundamental deviations from Temporal

Deviations **inherent to the architecture** — consequences of building on DBOS
Transact and Postgres with no Temporal server — or deliberate design decisions
we don't intend to reverse. Not-yet-implemented features are tracked separately
as conformance xfails (`tests/conformance/`) and parameter ledgers
(`tests/unit/test_signature_parity.py`), and summarized in the README. Each
entry's `Dn` number is the stable reference used throughout the code and tests.

## Architecture

### D1. No Temporal server, no wire protocol

There is no gRPC, Web UI, `temporal` CLI, or cross-language client — `dbosify` replaces *server + Python SDK* for Python-only apps, with each namespace mapped to its own Postgres schema (`dbosify_<namespace>`) and one namespace served per worker process.

### D2. Connection surface takes DBOS machinery directly

`Client` wraps a `dbos.DBOSClient` and `Worker` takes a `dbos.DBOSConfig` (one worker per process), so migration means adapting connection setup, not just swapping imports.

### D3. DBOS-native management bypasses the Temporal semantic layer

DBOS-native operations on the same workflows (Conductor, cancel, fork, resume) carry no Temporal semantics and render CANCELED / CONTINUED_AS_NEW runs as ERROR.

## Identity and runs

### D4. Run IDs are DBOS workflow IDs, with visible chain suffixes

`run_id` is the run's DBOS workflow id (`W` or `W--r{n}`), not an opaque UUID, and ids containing `--r` are rejected at start.

### D5. Child workflow IDs are deterministic, derived from the parent

A child's default id is the deterministic `{parent_id}_{seq}` rather than a server UUID (this is what lets recovery re-attach to an already-started child).

### D19. Cron workflows are run chains with per-run results

Cron is a run chain where `result()` returns the per-run result (Temporal's `follow_runs=True` never returns), between-run cancellation takes effect only at the next fire, 6/7-field cron is accepted, `start_delay` + `cron_schedule` raises, and an exceeded `run_timeout` ends the chain as TERMINATED instead of retrying it.

### D20. Workflow-retry matching and carryover differ at the edges

Retry matching mirrors Temporal's except `non_retryable_error_types` also matches envelope failure classes (a superset), and inbox messages still unconsumed when a run fails carry over to the retry attempt.

## Process and operations model

### D6. Failover is restart-or-management-action, not poller reassignment

A dequeued execution is pinned to its `executor_id`/`app_version` and resumes only when an equivalent executor relaunches (or via management action), and a mid-activity crash re-runs the same attempt number with empty `heartbeat_details`.

### D7. Start policies are enforced client-side, with TOCTOU windows

ID-conflict / id-reuse start policies are check-then-start from the client, leaving small race windows (signal-/update-with-start that starts a fresh run is the exception — it is atomic).

### D8. Blocking workflow code stalls the whole worker

Blocking sync code in a workflow body stalls every workflow on the worker's shared asyncio loop, not just one workflow task as in Temporal.

### D9. No multiprocess activity executors

`SharedStateManager` / `ProcessPoolExecutor` activities are unsupported; sync activities run on threads via `asyncio.to_thread`.

## Request/response semantics

### D10. Signals, cancels, and updates are durable messages, not RPCs

Because there is no server to validate targets, sends to closed workflows are silent no-ops (not "already completed"), ops on nonexistent workflows raise a DB/`RuntimeError` (not `NOT_FOUND`), exhausted waits raise builtin `TimeoutError` (not `RPCError`), and signals/cancels are not request-deduplicated.

### D11. Terminate stores no reason or details

`handle.terminate(reason=...)` accepts and drops the reason; awaiters get a generic `TerminatedError`.

### D12. Async activities are *more* cancellable than Temporal's

Async activities are cancelled at their next `await` regardless of heartbeating (more eagerly than Temporal) and surface `asyncio.CancelledError`.

### D26. Sync-activity cancellation is cooperative-only

A sync activity observes cancellation only cooperatively (at `heartbeat()` / `is_cancelled()`), always behaving as `no_thread_cancel_exception=True`.

### D17. Query handlers are synchronous-only

`@workflow.query` rejects `async def` handlers at definition time, where temporalio still accepts them with a deprecation warning.

## Determinism and data

### D13. No workflow sandbox

Determinism violations surface as nondeterminism errors at replay rather than being caught at development time.

### D18. Workflow time is assembled from participant clocks

`workflow.now()` is built from participant clocks (first executor's wall clock, timer deadlines, message `sent_at`) with no authoritative server clock, so a skewed client can step workflow time ahead.

### D14. Payloads live in the system database, uncapped

Payloads and history length are uncapped Postgres rows — Temporal's 2MB/4MB payload and ~50k-event limits are not enforced.

### D15. Memo + search attributes: stored and exposed, untyped, not fully queryable

Memo/search attributes live on DBOS's JSONB `attributes`, but search attributes are untyped, `get_current_history_size()` returns 0, and `list_workflows`/`count_workflows` support only a documented `AND`-only subset of the visibility query language.

### D16. Performance texture: every effect is a Postgres write

Every activity attempt, timer, message, and race round is one or more synchronous Postgres round-trips with no sticky cache or command batching, and several paths poll (~1s queue dequeue).

### D21. Data conversion: JSON transport, no protobuf payloads

Payloads convert through a temporalio-shaped `DataConverter` to readable JSON with no protobuf encoders (a raw protobuf payload fails loudly), and a `PayloadCodec` is skipped on failure `details` / `last_heartbeat_details` / cron `last_completion`.

### D22. Schedules compile to a single cron; overlap and history are partial

A `Schedule` compiles to one DBOS cron (non-dividing intervals approximated; calendar `year` / interval `offset` dropped), honors SKIP/CANCEL_OTHER/TERMINATE_OTHER/ALLOW_ALL but rejects BUFFER_ONE/BUFFER_ALL, makes `update` a delete-then-recreate, and tracks no schedule history.

### D23. Cross-queue activities run on a different worker, with caveats

`execute_activity(task_queue=)` runs the activity as an enqueued `__temporal_activity` workflow on another worker (retries, timeouts, and cancellation honored), with cross-process heartbeat-*detail* forwarding to the workflow side the one remaining gap.

### D24. Interceptors: client, activity, and workflow

Client, activity, and workflow inbound/outbound interceptors and header propagation are supported, but Nexus interception is absent and worker interceptors come only from `Worker(interceptors=)`.

### D27. Replay and queries-on-closed run over DBOS checkpoints, in-process

`Replayer` / `fetch_history` are DB-bound (no offline JSON history), and queries on closed workflows use rehydrate-by-replay, which needs a worker for that type in the querying process.

### D25. Dynamic handlers and activities are supported; dynamic workflows are not

Dynamic signal/query/update handlers and dynamic activities work, but dynamic *workflows* (`@workflow.defn(dynamic=True)`) raise `NotImplementedError`.

### D28. Patching is supported, backed by a stable app version

`workflow.patched()` / `deprecate_patch()` work via durable checkpoint markers (a False verdict claims no position), relying on a pinned default `application_version` rather than DBOS's native `patch_async`.

### D29. Worker deployment versioning = DBOS versioning: PINNED is enforced, AUTO_UPGRADE is not

A build ID *is* the DBOS `application_version`, so PINNED is enforced (a workflow recovers/dequeues only on its build ID) while AUTO_UPGRADE, ramping, and cluster routing have no analog and degrade to pinned.

### D30. Current details are in-memory, reconstructed on replay, not in describe()

`set_current_details()` / `get_current_details()` are in-memory state reconstructed on replay, settable only on the deterministic loop and not surfaced to `describe()` / `list_workflows`.

### D31. Random seed is fixed per run; reseed callbacks never fire

The per-run random seed never changes, so any `register_random_seed_callback` is stored but never invoked.

### D32. Activity cancellation details are not tracked

`activity.cancellation_details()` always returns `None` — the structured cancellation reason temporalio surfaces is not recorded.

### D33. Metrics and the telemetry `runtime` module are not provided in v1

Metrics are not implemented: `metric_meter()` raises `AttributeError` and the entire `temporalio.runtime` telemetry module is absent.

### D34. Worker tuning options map onto DBOS queues, with model differences

Tuning knobs map onto DBOS queues with caveats (regular and local activities share one concurrency cap; `identity` sets the recovery-scoping `executor_id`), and unfulfilable options are either inert-with-debug-log or rejected (`tuner` / `plugins` / `nexus_service_handlers`).

### D35. Start-verb options: most honored; a few accepted-but-pending

All start-verb parameters are accepted, but `start_child_workflow.cron_schedule`, `start_child_workflow.id_reuse_policy`, and `start_workflow.execution_timeout` are accepted-and-debug-logged rather than yet enforced.

### D36. Client-initiated (standalone) activities are not supported

`Client.start_activity` / `get_activity_handle` / `list_activities` and their supporting types are absent — calling one raises `AttributeError`.

### D37. `@activity.defn(no_thread_cancel_exception=)` defaults to `True`

This defaults to `True` (temporalio defaults `False`) and an explicit `False` is rejected, because sync-activity cancellation is cooperative (D26).

### D38. Dynamic signal/query/update handlers require the new-style signature

A `dynamic=True` handler must use `(self, name: str, args: Sequence[RawValue])`; the legacy `(self, name, *args)` form is rejected at registration.

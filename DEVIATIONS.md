# Fundamental deviations from Temporal

This file tracks deviations that are **inherent to the architecture** —
consequences of building on DBOS Transact and Postgres with no Temporal
server, or deliberate design decisions we don't intend to reverse. It is
*not* the place for not-yet-implemented features: those are tracked
mechanically as conformance xfails (`tests/conformance/`, each tagged with
its blocking phase) and parameter ledgers
(`tests/unit/test_signature_parity.py`), and summarized in the README.

Each entry: what differs, why it's fundamental, what it means for users,
and any mitigation.

---

## Architecture

### D1. No Temporal server, no wire protocol

There is no gRPC compatibility: SDKs in other languages cannot connect, and
the Temporal Web UI, `temporal` CLI, and tctl do not apply. This replaces
*server + Python SDK together*, for Python-only applications. Operations
happen through DBOS tooling (management APIs, Conductor). Security is
Postgres security: there is no Temporal-style mTLS endpoint or
namespace-level access control; namespaces map to Postgres schemas — cheap
isolation, not an authorization boundary.

### D2. Connection surface takes DBOS machinery directly

`Client` wraps a `dbos.DBOSClient`; `Worker` takes a `dbos.DBOSConfig` and
owns the process's DBOS lifecycle (DESIGN §5, revised). Migration is
therefore the import swap *plus* adapting connection setup —
`Client.connect("host:port")` call sites don't work unmodified. One
`Worker` per process is the current policy: DBOS's launchable runtime is
process-global, so in-process multi-worker would blur Temporal's
worker-isolation model (registration sets, lifecycles). Recorded in the
parity test's `DELIBERATE_DEVIATIONS`.

### D3. DBOS-native management bypasses the Temporal semantic layer

The same workflows are visible to DBOS tools (Conductor, `DBOS.cancel_workflow`,
fork, resume), and those operations do **not** carry Temporal semantics:
a DBOS-native cancel is a terminate with *no parent-close policy sweep*;
fork/resume on dispatcher workflows have no Temporal-defined meaning.
Status display has the same caveat: CANCELED and CONTINUED_AS_NEW are
recorded as marker "errors", so in DBOS-native views (Conductor dashboards,
raw status queries) those runs read as ERROR — a healthy long-lived entity
workflow that continues-as-new periodically looks like a stream of errored
workflows there. Operating through both layers on the same execution
requires care — the Temporal-faithful verbs and statuses are the
`temporal_dbos` client APIs.

## Identity and runs

### D4. Run IDs are DBOS workflow IDs, with visible chain suffixes

`run_id` is the run's DBOS workflow id — run *n* of workflow `W` is `W` or
`W--r{n}` — not an opaque server UUID. Consequences: run ids are
predictable and human-readable; workflow ids containing the `--r` separator
are rejected at start; code that assumes UUID-shaped run ids breaks.

### D5. Child workflow IDs are deterministic, derived from the parent

Default child id is `{parent_id}_{seq}` rather than a server-generated
UUID. This is required by DBOS's one-execution-per-id model (it is what
lets recovery *re-attach* to an already-started child instead of spawning a
twin) and is strictly more useful operationally. Same caveat: code
asserting UUID format breaks.

### D19. Cron workflows are run chains with per-run results

`start_workflow(cron_schedule=...)` creates run 0 immediately, delayed to
the next cron occurrence (the equivalent of Temporal's first-workflow-task
backoff — the execution exists at once, so describe/signal/result work
before the first fire); each close enqueues run n+1 at the next occurrence
after the close time, so a run that overruns an occurrence skips it
(Temporal semantics). Differences:

- `result()` on a *successful* cron run returns that run's result, where
  temporalio's `result(follow_runs=True)` on a cron workflow never returns
  (it follows every continuation). Failed runs *do* follow to their
  cron/retry successor. Making successes followable would require wrapping
  every workflow output in a continuation envelope; per-run results are
  also arguably more useful.
- Cancellation between runs takes effect at the next fire: the cancel
  envelope waits in the delayed next run's inbox — there is no server to
  cancel the waiting run in place. The same applies to a retry attempt
  waiting out its backoff (up to the policy's ``maximum_interval``), where
  Temporal cancels the backing-off execution immediately. And a cron run
  that never parks (no awaits) cannot observe a cooperative cancel
  mid-run: it completes, and the pending cancel ends the chain at the hop
  instead — the final run reads COMPLETED (Temporal's equivalent run also
  completes; its server suppresses the continuation).
- Cron expressions: 5-field, UTC by default, `CRON_TZ=`/`TZ=` prefixes
  honored — as in Temporal; 6-field (leading seconds) and 7-field
  (trailing year) forms are accepted as an extension where Temporal
  rejects them. The `@every` shorthand is not supported.
- `start_delay` together with `cron_schedule` raises `ValueError`. Our cron
  uses the enqueue delay internally to back off run 0 to the first
  occurrence, so a user `start_delay` cannot ride alongside. temporalio
  accepts the pair and silently ignores `start_delay` (its docstring notes
  it "does not work with cron_schedule"); we fail fast rather than swallow a
  behavior-changing parameter — a deliberate stricter-than-Temporal choice.
- `run_timeout` exceeded ends the cron (or retry) chain rather than
  continuing it: a run that blows its per-run timeout is natively cancelled
  → status TERMINATED, and the retry/cron continuation is not triggered.
  Temporal surfaces a `TIMED_OUT` failure and, with a retry policy, retries
  it. This is the run-timeout half of the still-pending `TIMED_OUT`
  status-marker work (README compatibility note), not a permanent design
  choice.

### D20. Workflow-retry matching and carryover differ at the edges

The retry decision mirrors Temporal's (`non_retryable` flag, type matching,
backoff, attempt caps, cancelled/terminated failures never retried, timeout
failures retried only for start-to-close/heartbeat types), with two edges:

- `non_retryable_error_types` matches the failure ``type`` of application
  errors — as in Temporal — but *falls back to the envelope failure class*
  (``ActivityError``, ``ChildWorkflowError``, ...) for wrapper failures,
  which Temporal treats as always retryable and never matches against the
  list. A superset: listing those class names works here and does nothing
  on a real Temporal server. Temporal's ``TemporalTimeout:StartToClose``
  string convention in the list is not supported.
- Inbox messages still unconsumed when a run fails (e.g. a signal racing
  the close) carry over to the retry attempt, mirroring the
  continue-as-new carryover; Temporal lets signals die with the failed
  run. More generous, occasionally observable.

## Process and operations model

### D6. Failover is restart-or-management-action, not poller reassignment

Once dequeued, an execution is pinned to its `executor_id`/`app_version`.
If that worker dies, the execution resumes when an equivalent executor
relaunches (automatic for same-executor restarts — our recovery tests — and
the default `executor_id` of "local" makes single-fleet setups recover on
any restart), or via management action (Conductor, admin recovery). Temporal
instead reassigns workflow tasks to *any* live poller on the queue within
seconds via task timeouts. This is the most important operational deviation:
plan worker supervision accordingly. Relatedly, a crash mid-activity
re-executes that attempt under the *same* attempt number on recovery, where
Temporal's timeout-driven retry would increment the attempt count — and
heartbeat details live in worker memory (Temporal persists them
server-side, throttled), so a worker restart presents the retry attempt
with empty ``heartbeat_details``.

### D7. Start policies are enforced client-side, with TOCTOU windows

Temporal's server arbitrates workflow starts atomically. Our id-conflict /
id-reuse policies (`FAIL`, `REJECT_DUPLICATE`, the duplicate-child-id
check) are check-then-start from the client or parent worker, leaving small
race windows under concurrent starts (DESIGN §6.4; narrowable with claim
rows, not eliminable without a central arbiter). The terminate-vs-child-start
window is closed by claim-then-start ordering; the others remain. Compound
signal-with-start and update-with-start *are* atomic when they start a fresh
run: the start enqueue and the signal/update request commit in one
system-database transaction (DBOS `enqueue_in_transaction` +
`send_in_transaction`), so a client crash can't leave the workflow started
without its signal/update. On the USE_EXISTING path that attaches to an
already-running run there is no enqueue to bundle with, so the message is
delivered by a follow-up send (idempotent by id) — which is fine, since that
run is already running. (Update-with-start still then awaits the update's
acceptance/result, as Temporal does; only the delivery is made atomic.)

### D8. Blocking workflow code stalls the whole worker

Workflow coroutines drain on the worker's shared asyncio loop. Blocking
sync code in workflow bodies or handlers (already a determinism-contract
violation in both systems) stalls *every* workflow on the worker, not just
one workflow-task thread as in Temporal. Could be engineered away
(thread-executor drains); no current plan.

### D9. No multiprocess activity executors

`SharedStateManager` / `ProcessPoolExecutor` activities are unsupported.
Sync activities run on threads via `asyncio.to_thread`; CPU-bound activity
parallelism needs multiple worker processes.

## Request/response semantics

### D10. Signals, cancels, and updates are durable messages, not RPCs

There is no server to validate targets, so the error surface differs:
signals and updates to *closed* workflows are silent no-ops / timeouts (the
message is never consumed) where Temporal raises "already completed" —
``cancel()`` and ``terminate()`` do pre-check the run's status and raise,
at the cost of one read on those (rare) paths; operations on
*nonexistent* workflows surface a database foreign-key error rather than
NOT_FOUND; there is no `cancel_requested` visibility before delivery. In
exchange, all external events share one totally-ordered durable inbox —
stronger ordering than Temporal's activation batching.

Two further consequences. Transport-layer error *types* differ: waits that
exhaust their timeout raise builtin `TimeoutError` (not
`WorkflowUpdateRPCTimeoutOrCancelledError` / `RPCError(DEADLINE_EXCEEDED)`),
and resolving a nonexistent workflow raises `RuntimeError` (not
`RPCError(NOT_FOUND)`) — `except RPCError` clauses won't match. And while
updates are deduplicated by update id (resends are safe), signals and
cancels carry no request id: an application-level resend delivers twice,
where Temporal's server dedups the RPC.

Run transitions widen the no-server gap slightly: a send resolves the
chain's current run client-side, so a message can land in a run that closes
before consuming it. Temporal routes id-addressed signals to the live run
atomically, including across continue-as-new. Continue-as-new narrows this
to a small window — the closing run forwards everything still unconsumed
(carryover) and the next run is enqueued before forwarding begins — but a
send that resolved the old run and landed after its final drain is silently
dropped. (Upstreamable: an API to consume another workflow's inbox would
let a new run sweep its predecessor.)

### D11. Terminate stores no reason or details

DBOS cancellation has no reason field, so `handle.terminate(reason=...)`
accepts and drops it; awaiters get a generic `TerminatedError`.
(Upstreamable to DBOS if it grows a cancellation-metadata field.)

### D12. Async activities are *more* cancellable than Temporal's

Temporal can only reach an activity through its heartbeat responses — a
non-heartbeating activity is uninterruptible. Our async activities run
in-process and are cancelled at their next `await` regardless of
heartbeating. Usually a strength, but code relying on
non-heartbeating-therefore-uninterruptible breaks, and the in-activity
exception is `asyncio.CancelledError` (catch via `is_cancelled_exception`
for portability; `except temporalio.exceptions.CancelledError` clauses
won't match).

### D26. Sync-activity cancellation is cooperative-only

Temporal's default for a synchronous (threaded) activity is to *raise* the
cancellation into the worker thread (`no_thread_cancel_exception=False`, via an
async thread exception). temporal-dbos never does this: a sync activity runs on
`asyncio.to_thread` and observes cancellation cooperatively — at its next
`activity.heartbeat()` (which raises `CancelledError`), or by polling
`activity.is_cancelled()` / `activity.wait_for_cancelled_sync()`. It therefore
always behaves as `no_thread_cancel_exception=True`. A sync activity that blocks
without checking (e.g. a bare `time.sleep`) is not interrupted; it runs to its
start-to-close timeout. `@activity.defn(no_thread_cancel_exception=False)` —
explicitly asking for the raise-into-the-thread behavior — raises
`NotImplementedError` at decoration time rather than silently degrading. (Even
Temporal's version is best-effort: `PyThreadState_SetAsyncExc` only fires at
Python bytecode boundaries and won't interrupt a blocking C call.) Async
activities are a separate story — see D12.

### D17. Query handlers are synchronous-only

`@workflow.query` rejects `async def` handlers at definition time, where
temporalio still accepts them with a `DeprecationWarning`. We enforce what
Temporal deprecates: queries answer inline from drained workflow state and
must not suspend. Migrating code with async query handlers must drop the
`async` (such handlers cannot usefully await in temporalio either).

## Determinism and data

### D13. No workflow sandbox

Determinism violations (wall-clock reads, uncontrolled randomness, I/O in
workflow code) surface at recovery/replay as nondeterminism errors instead
of being caught at development time. A dev-mode double-execution lint is a
possible future mitigation (DESIGN §8), not a plan of record. (Update
validators are exempt, as in Temporal: their verdict is checkpointed at
first delivery and replay reads the record instead of re-running them.)

### D18. Workflow time is assembled from participant clocks

`workflow.now()` starts at the first executor's wall clock (checkpointed)
and advances monotonically to timer deadlines and to each delivered
message's sender-clock `sent_at` (also checkpointed, so replay-stable).
There is no authoritative server clock: a skewed client can step workflow
time *ahead of* (never behind) the worker's clock. Code comparing workflow
time against external wall clocks should tolerate participant skew.

### D14. Payloads live in the system database, uncapped

Workflow/activity payloads are rows in Postgres. Temporal's 2MB/4MB payload
caps are not enforced; the practical limits are Postgres's. Large payloads
degrade the checkpoint ledger rather than being rejected. The same applies
to history *length*: Temporal warns around 10k events and terminates
workflows around 50k, pushing long-lived workflows toward continue-as-new;
our checkpoint ledger grows without limit, and any
`is_continue_as_new_suggested()` signal (Phase 3) will be a configurable
threshold, not a server-enforced cap.

### D15. Memo + search attributes: stored and exposed, untyped, not yet queryable

Memo and search attributes are stored on DBOS's native workflow `attributes`
(a JSONB column with a GIN index) under reserved `memo`/`search_attributes`
keys. They are set at start (`Client.start_workflow(memo=, search_attributes=)`),
carried to child workflows (explicitly — children do not inherit the parent's,
as in Temporal) and across continue-as-new (carried unless overridden),
upserted from inside a workflow (`workflow.upsert_memo` /
`workflow.upsert_search_attributes`, recorded as a checkpointed
`update_workflow_attributes` step so they survive recovery), and read back via
`WorkflowExecutionDescription.memo()`/`.typed_search_attributes` and
`workflow.info()`. Memo values round-trip through the `DataConverter` (so a
`PayloadCodec` encrypts them at rest); search-attribute values are stored as
plain JSON scalars (datetime as ISO-8601, keyword-list as a JSON array) so the
JSONB index can see them.

Deviations from Temporal:
- Search attributes are stored **untyped** — there is no cluster-side
  attribute registry, so any name/type is accepted and the typed key carries
  its own type (`SearchAttributeKey.for_keyword(...)` etc.); the deprecated
  untyped-dict form is also accepted, with the key type guessed as temporalio
  does.
- The **visibility query language is a documented subset**, not Temporal's full
  grammar. `Client.list_workflows(query=...)` / `count_workflows(query=...)`
  parse a flat `AND` conjunction (no `OR`, no grouping parens, no `ORDER BY`)
  over: `WorkflowType` (`= != IN`), `WorkflowId` (`= STARTS_WITH`),
  `ExecutionStatus` (`= IN`), `StartTime`/`CloseTime` (`> >= < <= =`), and custom
  search attributes (`=` only, mapped onto the GIN-indexed JSONB `@>`
  containment), plus an optional trailing `GROUP BY ExecutionStatus` /
  `GROUP BY WorkflowType` (`count_workflows` only). Anything outside the subset
  raises with a message listing what is supported. Specific gaps:
  - **Time bounds are inclusive.** `>`/`>=` (and `<`/`<=`) both translate to
    DBOS's inclusive bound, so a strict `>` may include an exact-timestamp match.
  - **`count_workflows` is aggregate-only.** It runs entirely through DBOS's
    server-side `get_workflow_aggregates` (`COUNT` + `GROUP BY`) and **rejects**
    (rather than scans) any query that operator can't express: filters on exact
    `WorkflowId`, a search attribute, `WorkflowType !=`, or an ERROR-family
    `ExecutionStatus` (Failed/Canceled/TimedOut/ContinuedAsNew — all stored as a
    single DBOS `ERROR` status), and `GROUP BY ExecutionStatus` (telling those
    error states apart needs each workflow's recorded outcome). The only
    supported grouping is `GROUP BY WorkflowType`. Use `list_workflows` to
    enumerate the rejected cases.
  - **Each run-chain link is its own row**, keyed by run id (continue-as-new /
    workflow-retry / cron hops), since `run_id` is the DBOS workflow id.
  - **Only user workflows are visible.** `list_workflows`/`count_workflows`
    return only DBOS workflows named `wf:{type}` (the user's Temporal workflow
    types); the internal dispatcher workflows DBOS records — `__temporal_activity`
    (cross-queue activity execution) and `__temporal_schedule_fire` (schedule
    fires) — are filtered out, so they don't show up as phantom executions.
  - **A scheduled workflow's `parent_id` points at its fire dispatcher.** Because
    the schedule action is enqueued from inside the `__temporal_schedule_fire`
    workflow, that dispatcher is the action's DBOS parent, so
    `WorkflowExecution.parent_id` is the (internal) fire-workflow id rather than
    `None`. Temporal scheduled workflows have no parent. (Continue-as-new and
    cron successors correctly report no parent — those links are same-chain.)
  - **Search-attribute filtering is scalar-equality only** (the containment
    subset); ranges/`IN` on a search attribute are rejected, and **keyword-list
    attributes are not filterable** — the value is stored as a JSON array and,
    with no cluster type registry, the query can't know to match it as a member
    rather than a scalar (`@>` containment of a scalar against a stored array
    never matches).
- The **deprecated untyped-dict removal idioms** are not honored. In temporalio
  an empty value list removes a search attribute: `upsert_search_attributes(
  {"k": []})` deletes the typed `k` (while leaving `k: []` in the legacy untyped
  view). Our untyped path can't express removal — the empty list is a no-op and
  `k` keeps its prior value. Use the typed form `key.value_unset()` to remove a
  search attribute.
- The **legacy untyped view after a typed unset** differs. `key.value_unset()`
  removes `k` entirely from `info().search_attributes` /
  `describe().search_attributes` (the deprecated `Mapping` view), whereas
  temporalio leaves the key present as an empty list (`info().search_attributes
  [k] == []`). The typed view (`typed_search_attributes`) is identical in both
  (the key is gone); only code reading the deprecated untyped view sees the
  difference.

Similarly, history *byte size* is not tracked:
`workflow.info().get_current_history_size()` always returns 0 — use
`is_continue_as_new_suggested()` / `get_current_history_length()` (the
checkpoint count) for continue-as-new decisions.

### D16. Performance texture: every effect is a Postgres write

No sticky cache, no command batching: each activity attempt, timer, message
consumption, and race round is one or more synchronous Postgres
round-trips, and several paths are polled (queue dequeue ~1s default,
child-result polling, client event fallback). Baselines are published in
`docs/perf.md` (~8ms/event first execution, ~1.7ms replay on localhost
Postgres) rather than hidden. Tunable; not removable.

### D21. Data conversion: JSON transport, no protobuf payloads, a few codec-exempt values

Payloads are converted through a temporalio-shaped `DataConverter` (default
JSON + type-hint reconstruction; custom
`DataConverter`/`PayloadConverter`/`PayloadCodec` via the `data_converter=`
argument to `Worker`/`Client`). The on-disk format is readable JSON, not
pickle: a user value is stored as a small payload dict (`{"encoding":
"json/plain", "json": <value>}` inline, or base64 for binary / codec-encrypted
bytes), so DBOS tooling (Conductor, `list_workflows`) shows the actual fields
rather than an opaque blob. Two deviations:

- **No protobuf payloads.** The `json/protobuf` / `binary/protobuf` encoders
  are not provided — a corollary of D1 (without other-language clients a
  cross-language protobuf schema buys nothing). A raw `protobuf.Message`
  payload falls through to the JSON encoder and fails *loudly* at encode time,
  not silently.
- **A `PayloadCodec` is skipped on a few synchronously-serialized values:**
  failure `details` / `last_heartbeat_details` and cron `last_completion` ride
  the (sync) payload converter without the async codec, so a codec that
  encrypts at rest will not cover those specific fields. Workflow/activity/
  message arguments and results — the bulk of payloads — do get the codec.
  (Upstreamable: an async failure-serialization path would close this.)

Internally, in-workflow reads of DBOS's own `WorkflowStatus` go through a step
that checkpoints only the JSON-safe fields we use (`status`, `queue_name`,
`parent_workflow_id`) — a whole `WorkflowStatus` is not JSON-serializable. This
is invisible to users and preserves the checkpoint (and thus determinism)
exactly, just serializably.

### D22. Schedules compile to a single cron; overlap and history are partial

`client.create_schedule` / `ScheduleHandle` (DESIGN §6.7) are backed by DBOS
schedules: a Temporal `Schedule` compiles to one DBOS schedule row that fires a
generic dispatcher, which starts the action workflow with a per-occurrence
deterministic id. The `Schedule`/`ScheduleSpec`/... type surface mirrors
temporalio; these edges differ:

- **`ScheduleSpec` compiles to one cron expression.** Interval periods that
  divide a cron boundary evenly (seconds into 60, minutes into 60, hours into
  24, whole days) are exact; others are approximated to the nearest cron with a
  debug-logged deviation. Calendar `year` constraints and interval `offset`s
  have no cron equivalent and are dropped (debug-logged). Multiple
  `intervals`/`calendars`/`cron_expressions` use the first.
- **Overlap policy: SKIP / CANCEL_OTHER / TERMINATE_OTHER / ALLOW_ALL honored;
  BUFFER_ONE / BUFFER_ALL rejected.** At each fire (for any policy but
  ALLOW_ALL) the dispatcher finds the most recently *started* action and checks
  whether it is still open. Every fire (regular, `trigger`, or `backfill`) is a
  dispatcher firing that DBOS tags with the schedule name, so it is found by an
  indexed schedule lookup (filter on `schedule_name`, not a prefix scan) over
  recent fires, mapping each to its per-occurrence action id and probing those
  in one batch (skipped fires leave no row). SKIP drops the new fire;
  CANCEL_OTHER cooperatively cancels the running action (but does **not** wait
  for it to finish unwinding before starting the next — they may briefly
  overlap, unlike Temporal); TERMINATE_OTHER natively cancels it. `trigger` and
  `backfill` fires now participate in overlap detection (they are tagged fires
  too). Edges: the lookup considers the most recent ~60 fires, so under SKIP a
  single action overrunning more than ~60 skipped fires may stop being detected
  (CANCEL/TERMINATE_OTHER start an action on every fire, so the prior is always
  the latest fire — no cap concern there); and a scheduled action that
  continues-as-new or retries isn't tracked across the hop.
  `BUFFER_ONE`/`BUFFER_ALL` (durable start-after-completion queueing) raise
  `NotImplementedError` at `create_schedule`.
- **Per-call overlap override is not applied.** `ScheduleHandle.trigger(overlap=)`
  and `ScheduleBackfill.overlap` can't be threaded through DBOS's
  trigger/backfill, so trigger/backfill always run under the schedule's
  *configured* overlap policy. Only `None` (use the schedule's policy) and
  `ALLOW_ALL` are accepted; any other per-call override raises
  `NotImplementedError` rather than being silently ignored.
- **`update` is delete-then-recreate.** DBOS has no in-place schedule update, so
  `ScheduleHandle.update` deletes and re-creates the row — which resets
  `last_fired_at`. `pause`/`unpause` use DBOS `pause_schedule`/`resume_schedule`
  directly and accept a `note` but do **not** persist it (DBOS only flips
  status; the stored `state.note` is unchanged).
- **Schedule history/metadata is partial.** `ScheduleInfo.next_action_times` is
  computed from the compiled cron; `recent_actions`/`running_actions`/action
  counts are empty, `created_at` reflects the row's creation time, and
  `last_updated_at` is always `None` (DBOS does not track schedule history).
  A schedule's `memo`/`search_attributes`, and the action's
  `memo`/`static_summary`/`static_details`/`priority`, are accepted and not
  stored; the action's `execution_timeout`/`task_timeout` are accepted but not
  applied (only `run_timeout` maps to a DBOS per-run timeout, as in
  `start_workflow` — see D-note #14). `list_schedules` returns all temporal-dbos
  schedules (the visibility `query` filter is ignored).

### D23. Cross-queue activities run on a different worker, with caveats

`execute_activity(..., task_queue=)` is honored (DESIGN §6.1.2): when the named
queue differs from the workflow's own queue, the activity does not run as an
in-process step — the interpreter enqueues a generic `__temporal_activity` DBOS
workflow onto that queue and awaits its result, exactly as it enqueues and
awaits a child workflow. Whatever worker listens on that queue runs the
activity, so "activities run on a different worker" holds. The activity workflow
id is the deterministic `{parent_run_id}--a{seq}` (the `--a` separator is reserved
like `--r`, so it can never collide with a user/child/run id), so a crash anywhere
after the enqueue re-attaches idempotently on recovery (no twin), and a SIGKILL of
either the workflow worker or the activity worker resumes correctly.

Operational requirement and current scope:

- **Cooperating workers share a DBOS application version automatically.** DBOS
  scopes queue dequeuing by application version, and a workflow worker and an
  activity-only worker register different function sets — so DBOS's
  auto-computed (code-hash) versions would differ and the activity worker would
  never dequeue the workflow worker's enqueue. The Worker pins a stable default
  version (`worker.DEFAULT_APP_VERSION`, D28) so all workers agree out of the
  box; no per-worker version configuration is needed. (Temporal coordinates on
  the task queue alone; this version pin is the DBOS-backed analogue.)
- **The activity workflow owns the full retry loop** (Design A): the queued
  path honors `retry_policy` (backoff, `maximum_attempts`, `non_retryable_error_types`,
  `ApplicationError(non_retryable=...)`, `next_retry_delay`), `start_to_close_timeout`
  (per attempt), and `schedule_to_close_timeout` — the last via the same
  `activities.retry_decision` the local path uses, so it bounds the *retry
  sequence* (checked between attempts), not an in-flight attempt
  (`start_to_close` bounds that), exactly as on the local path. Backoff is a
  durable `DBOS.sleep_async`, so a crash mid-backoff resumes at the right
  attempt. Minor deviation: schedule-to-close elapsed is measured from the first
  attempt's start on the activity worker, so the queue-wait before the first
  attempt is not counted toward it.
- **Cancellation reaches the activity cross-process.** When a queued activity is
  cancelled (an explicit `handle.cancel()` or propagated workflow cancellation),
  the interpreter sets a checkpointed cancel event on its run; the activity's
  attempt step polls that event on the other worker and delivers cancellation
  into the running activity (its `except`/`finally` cleanup runs), then reports
  the attempt cancelled — a cancelled activity is terminal (never retried). If the
  activity has async-parked (raise_complete_async), the interpreter also sends a
  cancellation marker to its completion topic so the parked wait wakes.
  `TRY_CANCEL` resolves the awaiter immediately; `WAIT_CANCELLATION_COMPLETED`
  resolves it only after the activity confirms its unwind via the result step;
  `ABANDON` leaves the activity running. Like the local path and D12, an async
  activity is cancelled at its next await (more eagerly than Temporal, which
  delivers only at `heartbeat()`); a sync activity observes it at its next
  `heartbeat()`.
- **A terminal close cancels in-flight cross-queue activities.** Continue-as-new,
  normal completion, and cooperative cancel all run the same close path, which
  sends the cross-process cancel signal to any still-pending queued activity — so
  a fire-and-forget or not-yet-finished activity does not outlive its workflow.
  This close-time cancellation is unconditional: it ignores the activity's
  `ActivityCancellationType`, so an `ABANDON` cross-queue activity is *also*
  cancelled at a non-`terminate` close, not left running to completion. (The
  type still governs in-run `handle.cancel()`: `ABANDON` there detaches. This
  matches the local path, which likewise cancels every in-flight attempt at
  close regardless of type.) **Exception: forceful `terminate`** is a native
  DBOS cancel that runs no close code, so it does *not* cancel in-flight
  cross-queue activities (they finish on their worker, their result discarded) —
  the same "no cleanup" behaviour `terminate` already has for child workflows.
  Use cooperative cancel if you need the activity stopped.
- **`schedule_to_start_timeout` bounds the queue dwell.** The activity workflow
  compares its enqueue time (`created_at`) to its start time on the worker and,
  if the budget was exceeded, fails with `TimeoutType.SCHEDULE_TO_START` before
  running any attempt. (On the local path it is a no-op — there is no queue
  wait — as in Temporal.)
- **`raise_complete_async` parks on the queued path.** The activity workflow
  waits on a dedicated completion topic; the task token carries the activity
  workflow id so an `AsyncActivityHandle` (`complete`/`fail`/`report_cancellation`)
  delivers there. A `fail` re-runs the activity per the retry policy; a
  `heartbeat` is skipped (it is not a completion); the recv times out against the
  start-to-close budget; and a completion / cancellation sets the gone-event so a
  later completer raises `AsyncActivityCancelledError` instead of sending into the
  void.
- **Remaining gap on the queued path:** cross-process heartbeat-*detail*
  forwarding to the workflow side. The in-activity heartbeat-timeout watchdog and
  cross-attempt `info().heartbeat_details` work on the activity worker; only
  surfacing live details to the workflow (rare) is absent — a `heartbeat` to an
  async-parked activity is accepted but its details are dropped. Documented rather
  than silently ignored.

### D24. Interceptors: client, activity, and workflow

Client, activity, and workflow interceptors are all supported (DESIGN §6.8):
pass `Worker(interceptors=[...])` (worker `Interceptor` →
`ActivityInbound/Outbound` and, via `workflow_interceptor_class`,
`WorkflowInbound/Outbound`) and `Client(interceptors=[...])` (client
`Interceptor` → `OutboundInterceptor`). The chains are built exactly as
temporalio builds them — one workflow chain per execution, rooted in the
real dispatch — and the `*Input` dataclasses are copied field-for-field from
the SDK so signatures match. Scope and edges:

- **Workflow inbound/outbound interception is supported.** Inbound wraps
  `execute_workflow`, `handle_signal`, `handle_query`, `handle_update_validator`,
  and `handle_update_handler`; outbound wraps `start_activity`,
  `start_local_activity`, `start_child_workflow`, `signal_child_workflow`,
  `signal_external_workflow`, `continue_as_new`, and `info`. `handle_query` and
  `handle_update_validator` are driven synchronously (queries are sync —
  DEVIATIONS #11 — so a `handle_query` override must not `await` anything that
  parks the loop). Nexus interception is unsupported (corollary of D1), so
  `start_nexus_operation` and the Nexus inbound interceptor are absent.
- **Header propagation is supported.** A header set on a client `*Input`
  (`start_workflow`/`signal`/`query`/`start_workflow_update`) reaches the
  workflow as `ExecuteWorkflowInput.headers` / the matching `Handle*Input.headers`;
  a header set on a workflow outbound `*Input` propagates to the activity attempt
  (`ExecuteActivityInput.headers`), the child run (`ExecuteWorkflowInput.headers`),
  or the signalled workflow (`HandleSignalInput.headers`). Header *values* are
  `Payload`s at the boundary, as in temporalio — encode/decode them with
  `workflow.payload_converter()` / `activity.payload_converter()`. Headers do not
  auto-carry across `continue_as_new` (the outbound interceptor re-injects them,
  matching Temporal); cancel-of-external is not a header-carrying verb;
  schedule-started actions begin with empty headers. A configured `PayloadCodec`
  *is* applied to header values (like args): the outbound interceptor sets raw
  `Payload`s on synchronous boundaries (`start_activity`), and the codec runs at
  the same real-loop points args are encoded (command processing, child start,
  continue-as-new, the client send), so an encrypting codec protects header
  values at rest.
- **Other copied-but-inert `*Input` fields.** `ExecuteActivityInput.executor` is
  always `None` (sync activities run via `asyncio.to_thread`, not a user
  executor); `WorkflowInterceptorClassInput.unsafe_extern_functions` is inert
  (no workflow sandbox, D3); and on the workflow outbound inputs
  `versioning_intent`, `initial_versioning_behavior`, `priority`,
  `disable_eager_execution`, and `arg_types`/`ret_type` are carried but not acted
  on. Client `*Input` carries the same inert set as before: `callbacks`, `links`,
  `request_id`, `versioning_override`, `priority`, `rpc_metadata`/`rpc_timeout`,
  and `data_converter_override`.
- **Worker interceptors come only from `Worker(interceptors=)`.** temporalio also
  pulls in client interceptors that subclass the worker `Interceptor`; a
  temporal_dbos Worker takes a `DBOSConfig`, not a client, so there is no client
  to harvest interceptors from (a corollary of D2 / one-worker-per-process).
- **`OutboundInterceptor` exposes only the verbs we route.** Workflow lifecycle
  (`start_workflow`, `signal`/`query`/`cancel`/`terminate`/`describe_workflow`,
  `start_workflow_update`), async-activity completion, and the schedule verbs are
  intercepted; Nexus, worker build-id, and workflow history-event fetching are
  absent (rather than present as silent no-ops). `update_with_start` is
  intercepted via its constituent `start_workflow` + `start_workflow_update`
  calls, not as its own outbound verb. `list_schedules` is an `async` outbound
  (our `Client.list_schedules` is async), where temporalio's is synchronous.

### D27. Replay and queries-on-closed run over DBOS checkpoints, in-process

temporalio's `Replayer` replays a *fetched event history* offline against a
server's data converter. We have no event history: a run's durable record is its
DBOS step checkpoints in Postgres. So `Replayer`/`WorkflowHandle.fetch_history`
are **DB-bound** — `fetch_history()` snapshots a live DBOS run's recorded steps,
and the `Replayer` verifies it by *forking* that run one step past its last
checkpoint (copying every recorded step) and re-executing it under the
currently-registered code. Consequences:

- **No offline JSON portability (v1).** `WorkflowHistory` references a run that
  still exists in the connected system database; there is no `from_json`/`to_json`
  round-trip yet. Replay therefore needs a launched DBOS runtime (a `Worker` for
  the workflow types), not just a history file. The `Replayer` requires those
  types to already be registered by a `Worker` and **reuses that Worker's
  process-global configuration** — its data converter, interceptors, and
  failure-exception types are authoritative; the matching `Replayer` constructor
  arguments are accepted for API parity but not re-applied (overriding them would
  clobber the live Worker, since one Worker owns the process).
- **Non-determinism detection is checkpoint-shaped.** A different step at a
  recorded position is caught by DBOS itself (`DBOSUnexpectedStepError`); two
  cases DBOS can't see on its own are caught by a guard the interpreter consults:
  current code launching a *new* durable operation past the recorded horizon
  (also prevents the replay from running a real activity), and current code
  *finishing early* (fewer steps than recorded). A workflow that faithfully
  re-fails is a *passing* replay — replay verifies determinism, not success.
- **Queries on closed workflows are answered by rehydrate-by-replay.** Where
  Temporal serves queries on any workflow within retention, a closed workflow
  here is forked to a scratch run that replays to its final state, serves the one
  query against the reconstructed instance (reusing the live query path), and is
  then discarded. Because DBOS exposes no hook to a workflow's in-memory state
  from outside the run, this is driven by an in-process rehydrate signal: it works
  when a worker for the workflow type runs **in the querying process** (the common
  embedded layout, and what the `hello_query` sample uses). A purely remote client
  with no co-located worker still gets the v1 "query requires RUNNING" behavior.
  Only COMPLETED/FAILED/CANCELED runs can be rehydrated; TERMINATED (a native
  kill leaves only partial checkpoints), TIMED_OUT, and CONTINUED_AS_NEW runs
  cannot be faithfully reconstructed and a query on them fails clearly.

### D25. Dynamic handlers and activities are supported; dynamic workflows are not

Dynamic **signal/query/update handlers** and dynamic **activities** work
(DESIGN §6.1):

- `@workflow.signal(dynamic=True)` / `@workflow.query(dynamic=True)` /
  `@workflow.update(dynamic=True)` register a single catch-all handler per
  category — invoked as `(self, name: str, args: Sequence[RawValue])` for any
  message whose name has no exact handler. An exact match always wins; the
  dynamic handler is the fallback. `description=` on these decorators is stored
  as handler metadata.
- `@activity.defn(dynamic=True)` registers a single catch-all activity —
  invoked as `(args: Sequence[RawValue])` for any activity type with no exact
  registration; the requested type is its `activity.info().activity_type`. It
  shares the full local + cross-queue execution path (timeouts, retries,
  cancellation, recovery).
- `workflow.payload_converter()` / `activity.payload_converter()` are exposed so
  a dynamic handler can convert the `RawValue` payloads it receives, e.g.
  `payload_converter().from_payload(arg.payload, MyType)`.

Edges:

- **Handler `description=` is stored but not surfaced.** temporalio exposes it
  through a `__temporal_workflow_metadata` query (backing `temporal workflow
  metadata`); temporal-dbos has no such metadata query, so the description is
  accepted and kept on the definition but never read. Inert metadata, not a
  behavior change.
- **A dynamic activity's durable step is named `act:__dynamic__`** (one shared
  step), while a registered activity's is `act:{type}`. If an *open* workflow's
  in-flight activity execution spans a redeploy that flips a type between
  dynamic-fallback and explicit registration, replay presents a different step
  name at that checkpoint and DBOS raises a step-mismatch error — the same
  hazard as any step rename across a redeploy of a running workflow. Completed
  runs and new runs are unaffected.

**Dynamic workflows (`@workflow.defn(dynamic=True)`) are not supported** and
raise `NotImplementedError`. temporal-dbos registers one DBOS workflow per
Temporal type (`wf:{type}`, resolved decision DESIGN §10.1) so that native
name-based listing/filtering works; a catch-all workflow has no such per-type
registration for an unknown incoming type to dispatch to, so it conflicts with
that model. Register each workflow type explicitly. (The `dynamic` parameter is
still accepted on `@workflow.defn` for signature parity — it is rejected, not
absent.)

### D28. Patching is supported; worker-deployment versioning is not

`workflow.patched(id)` and `workflow.deprecate_patch(id)` work and match
temporalio's semantics: `patched()` returns `True` on a first (non-replaying)
execution or when this patch's marker is already in history, and `False` when
replaying a history that predates the patch; `deprecate_patch()` follows the same
use-patch logic and records the marker so concurrent old runs keep their
checkpoint positions.

Implementation (DESIGN §6.8): the verdict is computed at the call site from the
set of patch ids already recorded in this run's step list (rebuilt at run start),
memoized per id, and claims **no** checkpoint position — so an old in-flight run
that never had the call keeps its function-id sequence and replays the older path
deterministically. Only when the newer path is taken is a marker durably written,
as a `@DBOS.step(name="__tdb_patch")` whose output is the patch id, routed through
the command queue so it lands at a deterministic position (replay reads it back).
The set is keyed by id (set membership, like temporalio's `NotifyHasPatch`), not
by position, so it is robust to code that shifts checkpoints.

We deliberately do **not** use DBOS's native `DBOS.patch_async`
(`enable_patching` in `dbos/_dbos_config.py`), even though its semantics match:
it is `async` and assumes function-id == sequential code position, which our
virtual-loop/command-queue interpreter decouples, and enabling it pins
`GlobalParams.app_version` to `"PATCHING_ENABLED"` (app version scopes queue
dequeuing — see the cross-queue notes in D23).

**Stable default app version (required for patching to mean anything).** DBOS
scopes both workflow recovery and queue dequeuing by `application_version`, which
it otherwise auto-computes from a hash of the registered code. So a genuine
redeploy — exactly the situation `patched()` exists for — would change the
version and strand every in-flight workflow under the old one: a new-code worker
would neither recover nor re-dequeue it, and the pre-patch runs `patched()` is
meant to serve would never run again. The Worker therefore pins a stable default
(`worker.DEFAULT_APP_VERSION`, currently `"0.1"`) unless the caller sets
`application_version` in the `DBOSConfig` (set it to `None` to opt into DBOS's
code-hash auto-versioning). This also lets cooperating workers that register
different function sets agree on a version with no setup (D23). The override is
config-only — there is no `DBOS__APPVERSION` special-casing. Distinct apps
sharing one database should set `application_version` explicitly to keep their
versions apart.

Edges and gaps:

- **As in temporalio, patching is the user's contract, not a safety net.**
  `deprecate_patch()` is only safe to deploy once every pre-patch run is closed
  (its whole purpose), and removing a patch branch while pre-patch runs are still
  open will diverge from their recorded history — the standard non-determinism
  hazard, surfaced at replay (D13), not at development time.
- **No worker deployment versioning.** Temporal's Build IDs / Worker Deployment
  Versions / `WorkerDeploymentConfig` and the `versioning_behavior` /
  `versioning_override` / `versioning_intent` knobs are a Temporal-cluster
  concept (the server routes tasks to compatible worker fleets). DBOS has a
  single `app_version` per deployment and no task-routing fleet model, so these
  are **not** implemented: `versioning_behavior` on `@workflow.defn` and the
  `versioning_*` fields on the client/outbound `*Input`s are accepted-and-inert
  (signature parity; carried, not acted on). Use `patched()` for in-code
  branching across deploys.

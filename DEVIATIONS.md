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
Operating through both layers on the same execution requires care — the
Temporal-faithful verbs are the `temporal_dbos` client APIs.

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
Temporal's timeout-driven retry would increment the attempt count.

### D7. Start policies are enforced client-side, with TOCTOU windows

Temporal's server arbitrates workflow starts atomically. Our id-conflict /
id-reuse policies (`FAIL`, `REJECT_DUPLICATE`, the duplicate-child-id
check) are check-then-start from the client or parent worker, leaving small
race windows under concurrent starts (DESIGN §6.4; narrowable with claim
rows, not eliminable without a central arbiter). The terminate-vs-child-start
window is closed by claim-then-start ordering; the others remain. Compound
starts are likewise non-atomic: signal-with-start and update-with-start
commit the start, then send the message — a client crash between the two
leaves the workflow started without its signal/update, where Temporal's are
a single atomic request. (Upstreamable: DBOS atomic enqueue-with-message.)

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
operations on *closed* workflows are silent no-ops (the message is never
consumed) where Temporal raises "already completed"; operations on
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
possible future mitigation (DESIGN §8), not a plan of record.

The contract also extends one step further than Temporal's: update
*validators* re-run on every replay (acceptance is re-derived, where
Temporal records it in history), so validators must be deterministic
functions of workflow state and arguments — a nondeterministic validator
that flips its verdict on replay corrupts the execution.

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

### D15. Visibility is a documented subset

`list_workflows` supports a documented subset of Temporal's visibility
query language, mapped onto DBOS filters; custom search attributes are
(post-Phase 3) stored but not indexed or queryable. The full query language
is not planned.

### D16. Performance texture: every effect is a Postgres write

No sticky cache, no command batching: each activity attempt, timer, message
consumption, and race round is one or more synchronous Postgres
round-trips, and several paths are polled (queue dequeue ~1s default,
child-result polling, client event fallback). Baselines are published in
`docs/perf.md` (~8ms/event first execution, ~1.7ms replay on localhost
Postgres) rather than hidden. Tunable; not removable.

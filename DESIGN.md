# `orc` — Design Document

> **Status:** living document, tracks current implementation
> **Audience:** contributors, reviewers, downstream library authors, and anyone trying to decide whether `orc` is the right fit
> **Scope:** the runtime, the SQLite persistence layer, the public API, and the operational properties they imply

This document is structured as an RFC. It explains *why* `orc` exists, *what*
it guarantees, and *how* it implements those guarantees in detail.
Implementation references in `path/file.go:line` form point at the current
codebase; line numbers may drift but symbol names should stay stable.

---

## 1. Motivation

Durable workflow engines (Temporal, dbos, Inngest, Restate, …) provide a
powerful programming model: write ordinary functions, get crash-safe
execution. The cost of admission is usually high — a server cluster, a
Postgres database, a control plane, sometimes a separate language SDK.

`orc` is a deliberate *miniaturisation* of that idea:

- **Embedded.** No daemon, no sidecar. The runtime is a Go library you import.
- **SQLite backed.** Persistence is a single file (or `:memory:` for tests).
- **Focused.** The features that actually matter for application code —
  workflows, steps, queues, notifications, events, sleep, schedules,
  recovery, management — are all here. Infrastructure-y things (admin
  HTTP, cloud control plane, streams, debouncer, patching) are out of
  scope.

The reference implementation `orc` borrows its programming model from is
[`dbos-inc/dbos-transact-golang`](https://github.com/dbos-inc/dbos-transact-golang).
Many of the public APIs intentionally look similar so that mental models
transfer; the storage substrate, however, is completely different.

### 1.1 Non-goals

- Horizontal scale-out across many writers. SQLite is a single-writer
  store. `orc` runs comfortably on one node and can share a database
  file with siblings via the OS-level locks SQLite provides, but the
  intended mode is *one process per database file*.
- Sub-millisecond dispatch latency. The queue runner is a polling loop;
  the polling interval (default 50 ms) is your latency floor.
- Replacing Temporal/Restate at fleet scale. If you're running thousands
  of workers and millions of workflows per minute, those projects exist
  for good reasons.

---

## 2. Glossary

| Term                 | Definition |
| -------------------- | ---------- |
| **Context**          | Root runtime object (`orc.Context`). Owns the system DB connection, the workflow registry, the queue runner, and the cron scheduler. |
| **Workflow**         | A registered Go function `func(*orc.Context, In) (Out, error)` whose execution is checkpointed to the system DB. |
| **Step**             | A leaf operation invoked via `RunAsStep`. Output (or error) is recorded and re-execution skips it. |
| **Handle**           | Typed capability returned by `RunWorkflow` / `RetrieveWorkflow`. Two variants: in-process `directHandle` and cross-process `pollingHandle`. |
| **Queue**            | A named, durably-backed dispatch queue with concurrency / rate / priority knobs. |
| **Executor**         | A single `orc.Context` instance, identified by `ExecutorID`. Multiple executors can share a database file, but recovery is partitioned by `ExecutorID`. |
| **System DB**        | The SQLite database holding workflow state. Schema lives in `migrations/0001_init.sql`. |
| **Notification**     | A message inserted into `notifications` for a target workflow + topic. Delivered by polling. |
| **Event**            | A key/value record in `workflow_events`, last-write-wins, owned by a single workflow. |

---

## 3. Programming model

### 3.1 The `Context` lifecycle

```
NewContext  -->  RegisterWorkflow*  -->  Launch  -->  ...  -->  Shutdown
```

- **`NewContext`** opens (or attaches to) a SQLite database, applies
  embedded migrations (`migrations/*.sql`), constructs the registry, and
  registers a private `_orc_internal` queue used as the default landing
  place for resumed/forked workflows.
- **`RegisterWorkflow`** must happen *before* `Launch`. Registering after
  launch panics; this is intentional. The registry is read-mostly at
  runtime.
- **`Launch`** starts the queue runner goroutine, the cron scheduler (if
  any schedules are registered), and runs a one-shot recovery pass that
  re-dispatches in-flight workflows owned by this `ExecutorID`.
- **`Shutdown`** cancels the root context, waits up to a caller-supplied
  timeout for in-flight workflows and the queue runner to drain, stops
  the cron scheduler, and closes the database (if `orc` opened it).
  Idempotent: calling `Shutdown` twice is safe.

`Context` is a lightweight struct that wraps a Go `context.Context` plus
shared mutable state behind a `*contextCore` pointer. This means the
runtime can cheaply produce per-workflow `Context` clones (for cancellation
scoping) without copying mutex/atomic primitives — a `go vet` requirement.

### 3.2 Workflows

```go
func myWorkflow(c *orc.Context, in MyInput) (MyOutput, error) { ... }
orc.RegisterWorkflow[MyInput, MyOutput](ctx, myWorkflow,
    orc.WithWorkflowName("my_workflow"),
    orc.WithMaxRecoveryAttempts(10),
)
h, _ := orc.RunWorkflow[MyInput, MyOutput](ctx, myWorkflow, in,
    orc.WithWorkflowID("..."), orc.WithQueue("..."),
    orc.WithDeduplicationID("..."), orc.WithPriority(0),
    orc.WithWorkflowTimeout(...), orc.WithWorkflowDelay(...))
```

Workflows are keyed in the registry by **two** names:

1. The function's fully-qualified runtime name (FQN), used when calling
   `RunWorkflow` with the function value.
2. The optional `WithWorkflowName(...)` override, which is what gets
   stored in `workflow_status.name` and is what recovery / queue dispatch
   looks up.

This dual indexing means a refactor that moves the function to a different
package keeps recovery working, *as long as* you registered with a stable
name override. This is the recommended practice.

Registration validates the function signature (parameter/return shape and
compatibility with the generic type parameters) and panics with an
actionable message on mismatch — failing at `RegisterWorkflow` rather than
on first execution. The common signatures are invoked through a typed fast
path; `reflect.Call` is only used for unusual-but-compatible shapes (e.g.
interface-typed parameters).

#### Child workflows

`RunWorkflow` called from inside a workflow spawns a *child* workflow, and
the spawn itself is checkpointed as a step in the parent:

1. The call consumes a step ID from the parent's counter.
2. If `operation_outputs` already has a row for that step (replay), the
   recorded `child_workflow_id` is returned as a polling handle — no new
   child is created.
3. Otherwise the child row is inserted (ID defaults to
   `<parentID>-<stepID>`, deterministic) and the spawn is recorded in
   `operation_outputs` with `child_workflow_id` set, *before* execution
   starts. A crash between insert and record converges on replay because
   the deterministic ID makes the re-insert an idempotent attach.

This means a recovered parent re-attaches to its children rather than
spawning duplicates, with or without an explicit `WithWorkflowID`.

### 3.3 Steps

```go
out, err := orc.RunAsStep(c, func(ctx context.Context) (T, error) { ... },
    orc.WithStepName("..."),
    orc.WithStepMaxRetries(5),
    orc.WithStepRetryBackoff(100*time.Millisecond, 5*time.Second, 2.0),
)
```

A step is identified by an integer **step ID** assigned in the order steps
execute within a workflow (`withinWorkflowState.takeStepID()`,
`context.go`). Step IDs are stable as long as your workflow function
performs steps in the same order on every replay — which is the standard
deterministic-replay contract.

Each step result is recorded in `operation_outputs` keyed by
`(workflow_uuid, function_id)`. On replay, `RunAsStep` consults
`operation_outputs` first; if a row exists, the recorded output (or
recorded error) is returned immediately and the step closure is *not*
invoked. This is the central durability primitive.

`WithStepMaxRetries` engages an in-process retry loop with exponential
backoff *before* persistence. Only the final outcome — success or final
failure — is stored. Transient failures are not visible from the workflow
function or to recovery.

### 3.4 Handles

```go
type WorkflowHandle[R any] interface {
    GetWorkflowID() string
    GetStatus() (WorkflowStatus, error)
    GetResult(opts ...GetResultOption) (R, error)
}
```

Two implementations:

- **`directHandle`** — returned by `RunWorkflow` when the workflow runs
  in-process. Holds a buffered channel that the workflow goroutine writes
  the result into. `GetResult` selects on that channel, the supplied
  timeout, and the underlying context's `Done()`. If the underlying
  context is cancelled (e.g. process shutdown), `directHandle` falls back
  to polling — so a result produced by another executor that picks up
  the workflow on restart can still be observed.
- **`pollingHandle`** — returned by `RunWorkflow` when the workflow goes
  on a queue (it'll be dispatched by the queue runner, possibly in
  another process), and by `RetrieveWorkflow`, `ResumeWorkflow`,
  `ForkWorkflow`. Polls the DB on `NotificationPollInterval`.

Both handle types are read-only with respect to the runtime — they cannot
cause re-execution. They only observe state.

### 3.5 Queues

```go
q := orc.NewWorkflowQueue(ctx, "name",
    orc.WithWorkerConcurrency(N),
    orc.WithGlobalConcurrency(M),
    orc.WithRateLimiter(&orc.RateLimiter{Limit: 100, Period: time.Minute}),
    orc.WithPriorityEnabled(),
)
orc.RunWorkflow[I, O](ctx, fn, in,
    orc.WithQueue(q.Name),
    orc.WithPriority(0),         // lower number = higher priority
    orc.WithDeduplicationID(...),
    orc.WithWorkflowDelay(d),    // first dispatch >= now+d
)
```

Queues are in-memory descriptors (`*WorkflowQueue`) registered against the
context. Workflow rows reference them only by `queue_name` (a TEXT column
on `workflow_status`). This means:

- Reopening the database in a new process picks up enqueued workflows
  even if the new process registers different concurrency/rate-limit
  settings — the *config* is process-local, the *backlog* is shared.
- Two processes pointing at the same DB file with the same queue name
  will both attempt to dequeue from it, racing on the SQLite writer
  lock. This works (it's just slow under contention) but isn't the
  intended primary deployment shape.

The queue runner (`queue.go:queueRunner.run`) ticks at
`Config.QueuePollInterval` and, for each registered queue, calls
`dispatchOne`. `dispatchOne` enforces the per-queue limits in order:
rate limit → worker (per-process) concurrency → global concurrency, then
calls `systemDatabase.dequeueWorkflows` for the resulting batch size.

Two latency/throughput refinements:

- **Queue wake.** Enqueuing in-process (`RunWorkflow` with `WithQueue`,
  `ResumeWorkflow`, `ForkWorkflow`, a cron tick) nudges the runner via a
  buffered wake channel, so dispatch doesn't wait out the poll interval.
  The interval tick remains the fallback for rows enqueued by *other*
  processes.
- **Idle pre-check.** `dequeueWorkflows` first runs a read-only
  `SELECT EXISTS(...)`; only when there is something to claim does it open
  the write transaction. Idle polling therefore never touches SQLite's
  writer lock.

### 3.6 Notifications

```go
orc.Send(c, destinationID, message, topic)        // any -> JSON envelope
msg, err := orc.Recv[T](c, topic, timeout)        // blocks until message or timeout
```

Implementation:

- `Send` outside a workflow inserts directly into `notifications`.
- `Send` inside a workflow wraps the insert in a `RunAsStep` named
  `orc.send`, so re-execution of the workflow does *not* re-send.
- `Recv` is *only* legal inside a workflow (returns
  `ErrNotInWorkflowErr` otherwise) and is itself a step
  (`orc.recv:<topic>`). Inside the step it polls
  `popNotification(destination_uuid=workflowID, topic)` every
  `NotificationPollInterval` until a row appears or the timeout expires.
  Successful pop sets `consumed=1` so the same row is delivered at most
  once.

`notifications.message_uuid` exists for de-duplication of `Send` retries
but is not currently exposed in the public API.

### 3.7 Events

```go
orc.SetEvent(c, "key", value)
v, err := orc.GetEvent[T](c, workflowID, "key", timeout)
```

Implementation:

- `SetEvent` is a step (`orc.setEvent:<key>`). The system DB performs an
  UPSERT into `workflow_events` keyed by `(workflow_uuid, key)`.
- `GetEvent` polls `workflow_events`. Inside a workflow it is wrapped in
  a step (`orc.getEvent:<key>`) so the observed value is durable; outside
  a workflow it's a plain poll loop.

Notifications are *messages* (one delivery per row, consumed). Events are
*facts* (last write wins, anyone can read). They're separate primitives.

#### In-process notify hub

`notify.go` implements a small subscribe/signal hub (`contextCore.hub`).
`Send`, `SetEvent` and workflow finalization signal the hub after their DB
write commits; `Recv`, `GetEvent` and `GetResult` subscribe *before* their
first DB poll (subscribe → poll → wait, so a signal can't be lost) and then
select on {hub wake, poll interval, ctx done}. The result: same-process
producer→consumer latency is effectively zero, while the DB poll remains
the source of truth and the only mechanism needed for cross-process
delivery. Missing a hub signal is never a correctness problem.

### 3.8 Sleep

```go
_, _ = orc.Sleep(c, 1*time.Hour)
```

Inside a workflow `Sleep` is a step (`orc.sleep`) whose recorded value is
the wakeup time in Unix milliseconds. On replay, the step returns the
recorded wakeup time and `Sleep` waits *only the remaining duration* —
which may be zero if the original wakeup time has already elapsed.
Outside a workflow `Sleep` is just `time.After` with `ctx.Done()`.

### 3.9 Cron schedules

```go
orc.RegisterWorkflow(ctx, fn,
    orc.WithWorkflowName("nightly"),
    orc.WithSchedule("0 0 0 * * *"),  // robfig/cron 6-field, including seconds
)
```

`WithSchedule` adds the workflow to a `*cron.Cron` instance lazily created
on `Launch`. Each fire enqueues a workflow with a deterministic ID of the
form `sched-<workflow-name>-<unix-second-of-tick>`. Because that ID is the
PRIMARY KEY of `workflow_status`, two processes pointing at the same DB
that fire the same tick collapse to a single row: the loser's insert is a
no-op (detected via SQLite `changes()`), so cron runs at-most-once per
boundary across the cluster. The scheduler is shut down gracefully on
`Shutdown`.

### 3.10 Management APIs

`management.go` exposes:

- `ListWorkflows(c, ...options)` — filter by name, status, queue,
  executor, time range, IDs; paginate; load-or-skip I/O blobs. Text
  filters match exactly; only the admin HTTP layer opts in to substring
  (fuzzy) matching via `listWorkflowsInput.Fuzzy`.
- `RetrieveWorkflow[O](c, id)` — return a polling handle.
- `CancelWorkflow(c, id)` — mark `CANCELLED` in DB **and** cancel the
  per-workflow `context.Context` with cause `ErrWorkflowCancelled` so
  blocking steps that honour `ctx.Done()` return promptly. For workflows
  running on a remote executor, the local cancel poller (default 250 ms,
  see `Config.CancelPollInterval`) picks the status change up and cancels
  the matching local goroutine.
- `ResumeWorkflow[O](c, id)` — flip from `ERROR/CANCELLED/MAX_…` to
  `ENQUEUED` on the internal queue, reset attempts, clear the recorded
  error/output. Steps already recorded in `operation_outputs` are
  *preserved*, so the resume picks up where the failed run left off.
- `ForkWorkflow[O](c, in)` — copy the original workflow row into a new
  ID with status `ENQUEUED`, copy the first `StartFromStep` step
  outputs, and re-run from there.
- `DeleteWorkflows(c, ids)` — hard delete from `workflow_status`
  (cascades to `operation_outputs`, `notifications`, `workflow_events`).
- `GetWorkflowSteps(c, id)` — enumerate `operation_outputs` for a
  workflow. Useful for dashboards.
- `ListRegisteredWorkflows(c)` — return the registry contents.

---

## 4. Data model

The full schema lives at `migrations/0001_init.sql`. Five tables, no views,
no triggers.

### 4.1 `workflow_status` — one row per workflow

```sql
workflow_uuid       TEXT PRIMARY KEY
status              TEXT NOT NULL
name                TEXT NOT NULL
output              TEXT                    -- JSON envelope, null until terminal
error               TEXT                    -- error string, null on success
executor_id         TEXT NOT NULL DEFAULT 'local'
application_version TEXT NOT NULL DEFAULT ''
application_id      TEXT NOT NULL DEFAULT ''
queue_name          TEXT                    -- null = direct execution
deduplication_id    TEXT
priority            INTEGER NOT NULL DEFAULT 0
timeout_ms          INTEGER NOT NULL DEFAULT 0
deadline_ms         INTEGER NOT NULL DEFAULT 0
delay_until_ms      INTEGER NOT NULL DEFAULT 0
created_at          INTEGER NOT NULL        -- Unix ms
updated_at          INTEGER NOT NULL        -- Unix ms
started_at_ms       INTEGER NOT NULL DEFAULT 0
attempts            INTEGER NOT NULL DEFAULT 0
input               TEXT                    -- JSON envelope
forked_from         TEXT                    -- parent workflow id when ForkWorkflow
parent_workflow_id  TEXT                    -- parent if launched from inside another wf
cron_schedule       TEXT                    -- when scheduled
UNIQUE (queue_name, deduplication_id)
```

Indices:

- `idx_workflow_status_status` — supports `ListWorkflows` filtered by status.
- `idx_workflow_status_queue_dequeue` — `(queue_name, status, priority, created_at)`. The composite key the queue runner's dequeue scan uses.
- `idx_workflow_status_executor` — `(executor_id, status)`. Used by the recovery pass.

#### Status state machine

```
                    insertWorkflow
                           │
                           ▼
                       ENQUEUED ◀────────────── ResumeWorkflow / ForkWorkflow
                       │   ▲                   (also from ERROR/CANCELLED/MAX_RECOVERY)
              dequeue  │   │ delay elapsed
                       ▼   │
                       PENDING ◀─────────────── DELAYED (when delay_until_ms <= now)
              run        │      ▲
                         │      │ recoverPending
                         ▼      │
       ┌────────────┬──────────┬──────────────────────┐
       ▼            ▼          ▼                      ▼
    SUCCESS       ERROR     CANCELLED       MAX_RECOVERY_ATTEMPTS_EXCEEDED
```

`IsTerminal()` covers the bottom row — those are end states until an
explicit `ResumeWorkflow` / `ForkWorkflow` re-opens the workflow.

### 4.2 `operation_outputs` — one row per recorded step

```sql
workflow_uuid     TEXT NOT NULL
function_id       INTEGER NOT NULL    -- ordinal step ID within the workflow
function_name     TEXT NOT NULL
output            TEXT                -- JSON envelope
error             TEXT                -- error string for the recorded final attempt
child_workflow_id TEXT                -- set when the "step" is actually a child WF
created_at        INTEGER NOT NULL
PRIMARY KEY (workflow_uuid, function_id)
FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
```

The `(workflow_uuid, function_id)` PK is what makes `RunAsStep`
idempotent: the second invocation of the same step ID reads the existing
row instead of writing a new one.

### 4.3 `notifications` — Send/Recv mailbox

```sql
id                INTEGER PRIMARY KEY AUTOINCREMENT
destination_uuid  TEXT NOT NULL
topic             TEXT NOT NULL DEFAULT ''
message           TEXT
created_at        INTEGER NOT NULL
message_uuid      TEXT NOT NULL UNIQUE
consumed          INTEGER NOT NULL DEFAULT 0
FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
```

Index: `idx_notifications_dest_topic` on `(destination_uuid, topic, consumed)`
— the Recv polling query.

### 4.4 `workflow_events` — last-write-wins facts

```sql
workflow_uuid TEXT NOT NULL
key           TEXT NOT NULL
value         TEXT
created_at    INTEGER NOT NULL
updated_at    INTEGER NOT NULL
PRIMARY KEY (workflow_uuid, key)
FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
```

### 4.5 `queue_dispatch_log` — sliding-window rate accounting

```sql
id            INTEGER PRIMARY KEY AUTOINCREMENT
queue_name    TEXT NOT NULL
workflow_uuid TEXT NOT NULL
dispatched_at INTEGER NOT NULL
```

Used by `countQueueDispatches(queueName, since)` to count dispatches in
the last `Period`. Not garbage-collected automatically yet — see §10.

---

## 5. Concurrency & transactions

`ella.to/sqlite` wraps `zombiezen.com/go/sqlite/sqlitex.Pool`. The pool
holds N connections (default 8); writers serialise via the underlying
SQLite writer lock. `orc` interacts with the pool through two patterns:

1. **`db.Exec(ctx, fn)` with `defer conn.Save(&err)`** — opens a
   `BEGIN IMMEDIATE` transaction; commits on `nil` return, rolls back on
   error. Used for writes.
2. **`db.Exec(ctx, fn)` without `Save`** — read-only; conn returned to
   the pool when the closure returns.

Every `conn.Prepare(...)` for a `SELECT` is followed by `defer
stmt.Reset()`. This is required by the underlying driver: a connection
returned to the pool with an active statement panics on the next `Put`.
Earlier development saw this exact panic; the fix was systematic.

### 5.1 Where the writer lock matters

Hot writers in steady state:

- **insertWorkflow** — when starting workflows. One small INSERT.
- **updateWorkflowStatus** — at PENDING transition, finalisation, retries.
- **recordStepOutput** — once per step.
- **dequeueWorkflows** — single transaction per queue tick that does
  one DELAYED→ENQUEUED UPDATE, one SELECT, and one bulk UPDATE.

Read paths (status polling, GetEvent, popNotification) don't take the
writer lock and parallelise across the read-only conns in the pool.

### 5.2 In-process synchronisation

`contextCore` (`context.go`) consolidates everything that mutates at
runtime:

- `cancel context.CancelCauseFunc` — root cancellation function.
- `launched, closed atomic.Bool` — lifecycle guards.
- `shutdownMu sync.Mutex` — serialises `Shutdown` with itself.
- `queues sync.Map` — `name -> *WorkflowQueue`.
- `queueRunner *queueRunner`, `queueDone chan struct{}`,
  `queueStarted atomic.Bool`.
- `scheduler *cron.Cron`, `schedulerMu sync.Mutex`.
- `workflowsWg sync.WaitGroup` — counts in-flight workflow goroutines.
- `active sync.Map` — `workflowID -> *activeWorkflow{resultCh, cancel}`
  for in-process result delivery and proactive cancellation. The
  `cancel` is a `context.CancelCauseFunc` derived per workflow so
  `CancelWorkflow` (locally) and the cancel poller (cross-process) can
  push `ErrWorkflowCancelledErr` into running steps.
- `cancelPoller *cancelPoller`, `cancelDone chan struct{}`,
  `cancelStarted atomic.Bool` — the goroutine that observes
  externally-set CANCELLED rows and cancels the local match.

`Context` holds a `*contextCore`, which is why `Context` values are
cheap to copy (each per-workflow child clone gets its own `ctx` but
shares the same core). This was deliberately re-architected to satisfy
`go vet -copylocks` while still allowing per-workflow cancellation
scoping.

---

## 6. Workflow execution lifecycle (deep dive)

Sequence for a direct (no-queue) `RunWorkflow`:

```
caller goroutine                              workflow goroutine                  system DB
─────────────────                              ──────────────────                  ─────────
RunWorkflow
  registry.get(fqn)
  serializer.Encode(input)
  insertWorkflow ─────────────────────────────────────────────────────────────────►  INSERT workflow_status
                                                                                      status=ENQUEUED
                                                                                    (or read existing row)
  active.Store(wfID, resultCh)
  workflowsWg.Add(1)
  go runWorkflowExecution ─────►  runWorkflowExecution starts
  return directHandle                updateWorkflowStatus ────────────────────────►  status=PENDING, attempts++
                                     entry.Wrapper(wc, inputStr)
                                       RunAsStep #0
                                         checkStepOutput ─────────────────────────►  SELECT operation_outputs
                                         (none) → run step closure
                                         recordStepOutput ────────────────────────►  INSERT operation_outputs
                                       RunAsStep #1
                                         ...
                                     (wrapper returns out, err)
                                     finalizeWorkflow ───────────────────────────►   UPDATE workflow_status
                                                                                       status=SUCCESS|ERROR|CANCELLED
                                     resultCh <- outcome
                                     close(resultCh); active.Delete; wg.Done

handle.GetResult
  select on resultCh / timeout / ctx.Done
  return outcome
```

If the process dies anywhere between *INSERT workflow_status* and
*UPDATE status=SUCCESS*, on the next launch the recovery pass picks the
row up — *unless* it's already in a terminal state.

### 6.1 Idempotent `RunWorkflow`

`insertWorkflow` does an `INSERT … ON CONFLICT(workflow_uuid) DO NOTHING`
and then reads the row back. The result struct carries an
`AlreadyExisted` flag. `RunWorkflow` checks it:

- Same name + same encoded input → return a polling handle to the
  existing row (idempotent).
- Different name or different input → return `ErrConflictingInputErr`.

Inputs are compared by their **encoded** form, not by deep Go equality,
so a `Serializer` swap mid-flight could cause spurious conflicts.

### 6.2 Queued workflows

`RunWorkflow(..., WithQueue(q.Name))` does the same `insertWorkflow` but
returns a `pollingHandle` immediately (no in-process goroutine). The
queue runner picks the row up on its next tick, transitions it to
PENDING, and runs `runRegisteredWorkflowFromDB` in a goroutine on the
runner's process. From that point the lifecycle matches §6.

### 6.3 `runRegisteredWorkflowFromDB`

Used by both the queue runner and the recovery pass. It looks up the
workflow by `status.Name`, encodes the recorded `Input` back into the
on-the-wire form, and starts a fresh worker goroutine that calls
`runWorkflowExecution`. Because all step outputs already in
`operation_outputs` are honoured, the workflow resumes from its last
checkpoint.

---

## 7. Queue dispatch (deep dive)

`queue.go`. Each `tick`:

```
for each registered queue q:
  dispatchOne(q):
    if rate limit configured:
       used = countQueueDispatches(q.Name, now - period)
       limit = min(defaultLimit, q.RateLimit - used)
       if limit <= 0: skip
    if worker concurrency configured:
       active = countActiveQueueWorkflows(q.Name, executorID)
       limit = min(limit, q.WorkerConcurrency - active)
       if limit <= 0: skip
    if global concurrency configured:
       (similar global-active count)
       if limit <= 0: skip
    ids = systemDB.dequeueWorkflows({queue, limit, executor, priority})
    if not ids: skip
    recordQueueDispatch(q.Name, ids)
    for each id:
      st = getWorkflowStatus(id, loadIO=true)
      runRegisteredWorkflowFromDB(c, *st)
```

`dequeueWorkflows` runs in a single transaction:

1. UPDATE `DELAYED → ENQUEUED` for any rows whose `delay_until_ms <= now`.
2. SELECT candidate workflow IDs ordered by `priority ASC, created_at
   ASC` (or `created_at ASC` if priority not enabled).
3. UPDATE the picked rows to `PENDING`, set `executor_id`,
   `started_at_ms`.

Because step 3 atomically claims the rows, two queue runners pointing at
the same DB cannot dispatch the same workflow twice — the slower one
will see the rows in PENDING already on its next attempt.

`recordQueueDispatch` writes to `queue_dispatch_log` for rate-limit
accounting. A janitor runs inside `queueRunner.run` (every
`QueueDispatchLogPurgeInterval`, default 1 minute) and deletes rows
older than `max(QueueDispatchLogRetention, 2 × longest configured
RateLimiter.Period)` so the table stays bounded regardless of dispatch
rate.

### 7.1 Backpressure and concurrency knobs

| Knob                          | Where enforced                  | What it bounds                                           |
| ----------------------------- | ------------------------------- | -------------------------------------------------------- |
| `WithWorkerConcurrency(N)`    | per-process count of PENDING    | parallelism this process drives for one queue            |
| `WithGlobalConcurrency(M)`    | global count of PENDING         | total parallelism across processes (best-effort)         |
| `WithRateLimiter(L,Period)`   | sliding window of dispatches    | dispatches per `Period` for one queue                    |
| `WithPriorityEnabled()`       | order-by clause in dequeue      | ordering only, not throughput                            |

These compose. The dispatcher always chooses the **smallest** of the
permissible budgets and dequeues at most that many.

---

## 8. Recovery

`recovery.go:recoverPending` runs once during `Launch`:

```
pending      = listWorkflows(status=PENDING, executor=this)
enqueuedSolo = listWorkflows(status=ENQUEUED, executor=this) where queue_name = ''
todo         = pending ++ enqueuedSolo

for each st in todo:
    entry = registry.get(st.Name)
    if not entry:
        log warning, skip
        continue

    if st.Attempts >= max(MaxRecoveryAttempts, entry.MaxRetries):
        update status = MAX_RECOVERY_ATTEMPTS_EXCEEDED
        continue

    runRegisteredWorkflowFromDB(c, st)
```

Two points worth highlighting:

- **PENDING** workflows were caught mid-run by the previous crash.
- **ENQUEUED with no queue** workflows are direct-execution workflows
  that crashed *before* the worker goroutine got a chance to flip them
  to PENDING. Without this branch they would silently never be picked
  up. Queued workflows are not part of recovery — they're the queue
  runner's responsibility.
- The `executor_id` filter scopes recovery to this process. Two
  processes sharing a DB file with different `ExecutorID`s can each
  recover their own backlog.

Recovery never re-runs already-completed steps, by construction: replay
goes through `RunAsStep`, which short-circuits on `operation_outputs`
hits.

### 8.1 Cap on recovery attempts

Each recovery bumps `attempts` (via `BumpAttempts: true` in
`updateWorkflowStatus`). When `attempts >= max`, the row is moved to the
terminal `MAX_RECOVERY_ATTEMPTS_EXCEEDED` state. This is the safety
valve against a poisoned workflow that crashes the process every time
it runs.

---

## 9. Failure & cancellation semantics

| Failure                                             | What happens                                                                                                |
| --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Step closure returns error, `WithStepMaxRetries` exhausted | Final error recorded in `operation_outputs.error`. `RunAsStep` returns it. Workflow can catch it.   |
| Workflow function returns non-nil error             | `finalizeWorkflow` sets status `ERROR` (or `CANCELLED` if the underlying error is one of the cancellation sentinels). |
| Workflow function panics                            | `recover()` in `runWorkflowExecution` finalises the row to ERROR and posts an `orc: workflow panic` outcome. |
| Step succeeded but its checkpoint write failed      | `RunAsStep` returns an error (the workflow run fails and can be recovered/resumed) rather than continuing past an unpersisted checkpoint, which would re-execute the step on replay. |
| Process crash mid-step                              | Step row is *not* present in `operation_outputs`. Recovery re-runs the workflow; the step closure is invoked again. |
| Process crash after step recorded but before workflow finalised | Step row is present. Recovery re-runs the workflow; that step is skipped via the recorded output. |
| `CancelWorkflow` while running                      | DB row flipped to CANCELLED. The per-workflow `context.Context` is cancelled with cause `ErrWorkflowCancelledErr`; steps that honour `ctx.Done()` (`Sleep`, `Recv`, etc.) return promptly. Cross-process cancellation is delivered by the cancel poller within `Config.CancelPollInterval` (default 250 ms). |
| `WithWorkflowTimeout` expires                       | Per-run sub-context fires `DeadlineExceeded`. Workflow errors out as `ErrWorkflowTimedOutErr`; row is marked CANCELLED. |
| `Shutdown(timeout)` while workflows in flight       | Root context cancelled. Workflows that observe ctx will exit. After timeout, the DB connection is closed and any straggler step writes will silently fail. The next `Launch` will recover them. |
| Awaited workflow errored                            | `pollHandleResult` returns `wrapError(ErrAwaitedWorkflowFailed, st.Error, ...)`. Use `errors.Is(err, orc.ErrAwaitedWorkflowFailedErr)` to detect. |

---

## 10. Known limitations & future work

These are real, intentional gaps. Each is fixable but not yet fixed.

1. **Single-writer storage.** SQLite serialises writes. Multiple
   processes on the same DB file are supported, but throughput is
   bounded by the writer.
2. **Polling across processes.** Within one process, waiters are woken by
   the in-process notify hub (near-zero latency). Between processes
   sharing a DB file, floor latency for `Recv`/`GetEvent`/queue dispatch
   is `*PollInterval` (default 50 ms). Tunable, but never sub-millisecond.
3. **No streams / no patching system.** dbos has both; `orc` does not.
   Streams are a deliberate omission for the SQLite-only scope.
4. **Inputs compared by encoded form.** Changing the configured
   `Serializer` mid-flight will make existing rows look like conflicting
   inputs to subsequent `RunWorkflow` calls.
5. **No multi-tenant isolation.** All workflows share one schema; there
   is no equivalent of dbos's per-app schema. Use separate DB files for
   isolation.
6. **No admin HTTP server, no CLI.** Build them on top of `ListWorkflows`
   et al. if needed.

### Resolved (was previously listed here)

- **Child workflow replay duplication.** `RunWorkflow` inside a workflow
  is now checkpointed as a step (`operation_outputs.child_workflow_id`)
  with a deterministic default child ID, so recovered parents re-attach
  to existing children. See §3.2 "Child workflows".
- **`WithWorkflowDelay` without a queue.** Previously silently ignored
  (the row was inserted DELAYED but executed immediately). Direct delayed
  runs are now routed through the internal queue so the delay is durable.
- **Fuzzy filters leaking into internal queries.** The admin API's
  substring search on name/queue/executor briefly applied to *all*
  `listWorkflows` callers, which let executor `node-1` recover workflows
  owned by `node-10`. Substring matching is now opt-in
  (`listWorkflowsInput.Fuzzy`), set only by the admin HTTP layer.
- **Unrecorded step checkpoints.** A failed `recordStepOutput` after a
  successful step now fails the workflow run (so it retries from a
  consistent state) instead of letting execution continue past an
  unpersisted checkpoint.

- **Cancellation is now proactive.** `CancelWorkflow` cancels the
  per-workflow `context.Context` with cause `ErrWorkflowCancelled` for
  the local executor; remote executors pick the change up via the
  cancel poller (default 250 ms). Steps that honour `ctx.Done()` and
  `Sleep(c, ...)` return promptly. If a workflow swallows the cancel
  error and tries to return normally, `runWorkflowExecution` overrides
  the result so the run is still recorded `CANCELLED`.
- **`queue_dispatch_log` is now bounded.** A janitor inside
  `queueRunner.run` periodically deletes rows older than
  `max(QueueDispatchLogRetention, 2 × longest rate-limit Period)`.
- **Cron tick deduplication.** Each tick uses a deterministic
  workflow ID `sched-<name>-<unix_seconds>`. Concurrent inserts from
  peer executors collapse to a single workflow row via the
  `workflow_status.workflow_uuid` PK + `INSERT … ON CONFLICT DO NOTHING`
  + `changes()` detection in `insertWorkflow`.

---

## 11. Comparison to `dbos-transact-golang`

| Area                          | dbos-transact-golang                          | orc                                                         |
| ----------------------------- | --------------------------------------------- | ----------------------------------------------------------- |
| Storage                       | Postgres                                      | SQLite (`ella.to/sqlite`)                                   |
| Notification delivery         | LISTEN / NOTIFY                               | Polling                                                     |
| Concurrency primitive         | Postgres advisory locks, SKIP LOCKED          | Single-writer transaction; UPDATE-then-claim                |
| Multi-tenancy                 | Per-app Postgres schema                       | Out of scope (use separate DB files)                        |
| Admin HTTP                    | Built-in                                      | Out of scope                                                |
| Conductor (cloud control)     | Built-in client                               | Out of scope                                                |
| Streams                       | Yes                                           | Out of scope                                                |
| Patching                      | Yes                                           | Out of scope                                                |
| Workflow / Step API           | Same shape                                    | Same shape                                                  |
| Queues, notifications, events | Same model                                    | Same model                                                  |
| Cron                          | robfig/cron                                   | robfig/cron                                                 |
| Recovery                      | PENDING by executor                           | PENDING + ENQUEUED-without-queue, by executor               |

If you're already familiar with dbos, you'll feel at home in `orc` —
just smaller.

---

## 12. Operational guidance

### 12.1 Deployment shapes

- **Single binary, single SQLite file.** Recommended default. One
  `orc.Context` per process. No coordination needed.
- **Multiple binaries, one shared SQLite file.** Works (SQLite handles
  the file-level locks). Throughput bounded by the writer. Good for
  hot-standby setups where only one process is *actually* doing work.
- **Multiple binaries, multiple SQLite files.** Each binary is fully
  independent; this is "horizontal scale" for `orc`. You partition by
  some application key.

### 12.2 Sizing & tuning

- `Config.PoolSize` defaults to 8. Increase if you have lots of read
  fan-out (e.g. dashboards calling `ListWorkflows` constantly) and CPUs
  to spare. The writer is still serialised.
- `Config.QueuePollInterval` (default 50 ms): trades latency for SQL
  load. At 50 ms with N queues you do at most 20·N cheap read-only
  SELECTs / sec — idle ticks never take the writer lock, and in-process
  enqueues bypass the interval entirely via the queue wake channel.
- `Config.NotificationPollInterval` (default 50 ms): floor for
  `Recv` / `GetEvent` / inside-`Sleep` wakeups **from other processes**;
  same-process producers wake waiters immediately through the notify hub.
- `Config.CancelPollInterval` (default 250 ms): how often the cancel
  poller scans for workflows that another process marked CANCELLED so
  it can interrupt the local goroutine. In single-executor setups this
  is rarely-fired and you can leave it at the default; in multi-executor
  setups, lower it for sharper cross-process cancel latency.
- `Config.QueueDispatchLogPurgeInterval` (default 1 minute) and
  `Config.QueueDispatchLogRetention` (default 1 hour): control the
  janitor that bounds `queue_dispatch_log`. The effective retention is
  the larger of `Retention` and `2 × longest configured RateLimiter.Period`.
- `MaxRecoveryAttempts` (default 50): consider lowering if your
  workflows are expensive; the cap is per workflow, not per executor.

### 12.3 Backups

It's a SQLite file. Use `sqlite3 .backup`, `litestream`, filesystem
snapshots, whatever you already do. ORC does not hold transactions open
across calls.

### 12.4 Migrations

The `migrations/` directory is `embed`'d at build time and applied with
`sqlite.Migration` on `NewContext`. Set `Config.SkipMigrations = true`
if you manage migrations externally (e.g. a separate `migrate up` step
in deployment).

### 12.5 Observability

- Use `Config.Logger` to capture the runtime's structured logs (slog).
- `ListWorkflows` plus `GetWorkflowSteps` is the canonical way to power
  a dashboard.
- The DB is plain SQL; `sqlite3 orc.db .schema` and ad-hoc queries are
  fully supported. The schema is intended to be queryable directly.

---

## 13. Code map

| File                       | What lives there                                                        |
| -------------------------- | ----------------------------------------------------------------------- |
| `doc.go`                   | Package docs (godoc).                                                   |
| `config.go`                | `Config` struct + defaults.                                             |
| `errors.go`                | `Error`, `ErrorCode`, sentinel `Err…Err` values, `IsCode`.              |
| `serializer.go`            | `Serializer` interface, default JSON-envelope serializer.               |
| `status.go`                | `WorkflowStatusType`, `WorkflowStatus`, `StepInfo`.                     |
| `context.go`               | `Context`, `contextCore`, `NewContext`, `Launch`, `Shutdown`.           |
| `registry.go`              | Workflow registration, FQN/custom-name dual indexing, wrapper builder.  |
| `workflow.go`              | `RunWorkflow`, `RunAsStep`, `runRegisteredWorkflowFromDB`, finalisation.|
| `handle.go`                | `WorkflowHandle`, `directHandle`, `pollingHandle`, polling logic.       |
| `queue.go`                 | `WorkflowQueue`, `NewWorkflowQueue`, `queueRunner`, dispatch loop, `queue_dispatch_log` janitor. |
| `notify.go`                | In-process notify hub: subscribe/signal used by Recv/GetEvent/GetResult waits and the queue wake. |
| `cancel_poller.go`         | Cross-process cancel poller that cancels local workers when their DB row is flipped to CANCELLED elsewhere. |
| `notifications.go`         | `Send`, `Recv` and their step-wrapping logic.                           |
| `events.go`                | `SetEvent`, `GetEvent` and their step-wrapping logic.                   |
| `sleep.go`                 | `Sleep` (durable timer).                                                |
| `schedule.go`              | Cron scheduler integration.                                             |
| `recovery.go`              | `recoverPending`.                                                       |
| `management.go`            | `ListWorkflows`, `Cancel`, `Resume`, `Fork`, `Delete`, `Retrieve`, `GetWorkflowSteps`. |
| `systemdb.go`              | `systemDatabase` interface + I/O structs.                               |
| `systemdb_sqlite.go`       | SQLite implementation of `systemDatabase`.                              |
| `migrations/0001_init.sql` | Initial schema.                                                         |
| `uuid.go`                  | UUID generation helper.                                                 |

Test files mirror their subjects (`workflow_test.go`,
`queue_test.go`, `notifications_test.go`, `recovery_test.go`,
`management_test.go`, …). `examples_test.go` contains 12 in-package
end-to-end story tests; `examples/` contains 13 standalone runnable
programs that demonstrate the same patterns.

---

## 14. Versioning & stability

`orc` follows semantic versioning. Pre-1.0:

- Public API in `*.go` non-test files is the surface that needs to stay
  compatible across patch releases. Breaking changes are allowed in
  minor releases until 1.0.
- The on-disk schema is part of the contract. Schema changes will be
  delivered as additional `migrations/000N_*.sql` files; downgrades are
  not supported.

---

## 15. Open questions

These are the things I'd want a reviewer to push back on:

1. **Should `ENQUEUED` workflows with a queue also be recovered on
   launch?** Currently the queue runner picks them up on its next tick;
   recovery doesn't double-handle them. This is correct under the
   single-process model. Under the multi-process model, a long-dead
   executor's enqueued items will sit in the queue forever unless
   another executor pulls them. Worth thinking about.
2. **Do we want a step-level retry policy that *re-runs across process
   restarts* rather than just within one process?** Today retries are
   in-process only; a permanent step failure is permanent across
   recovery (recorded as an error in `operation_outputs`).
3. **Should we expose `MessageUUID` on `Send`?** It's already in the
   schema for de-duplication; surfacing it lets callers do their own
   client-side at-most-once.
4. **Is JSON the right default serializer?** It's portable but gives up
   structural typing for `any`. A typed gob/proto path would be faster
   and safer for known input/output types.

---

*— end of design —*

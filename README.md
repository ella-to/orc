```
░█████╗░██████╗░░█████╗░
██╔══██╗██╔══██╗██╔══██╗
██║░░██║██████╔╝██║░░╚═╝
██║░░██║██╔══██╗██║░░██╗
╚█████╔╝██║░░██║╚█████╔╝
░╚════╝░╚═╝░░╚═╝░╚════╝░
```

> **Why does this exist?**
> Sometimes all you need is a simple embedded durable workflow — without the
> bells and whistles, the cluster, or the control plane. I built `orc` for
> myself. I've been using SQLite for everything and I wanted a workflow
> engine that just works on top of it.

`orc` is a lightweight, embeddable, **durable workflow orchestrator for Go,
backed by SQLite**. It's a focused re-implementation of the core ideas in
[`dbos-inc/dbos-transact-golang`](https://github.com/dbos-inc/dbos-transact-golang)
on top of [`ella.to/sqlite`](https://github.com/ella-to/sqlite), so you can
get durable workflows, queues, scheduled jobs, and inter-workflow messaging
without spinning up a Postgres cluster.

You write ordinary Go functions; `orc` checkpoints their inputs, step outputs,
errors, and completion to SQLite. If your process crashes, `orc` resumes the
in-flight workflows from their last checkpoint when it next launches.

---

## Highlights

- **Durable workflows** — registered Go functions whose execution state survives crashes.
- **Durable steps** — `RunAsStep` records each step's output so re-execution skips work that's already done.
- **Durable child workflows** — `RunWorkflow` called *inside* a workflow is checkpointed too: a replayed parent re-attaches to the same children instead of spawning duplicates.
- **Durable queues** — concurrency limits, rate limits, priorities, deduplication.
- **Durable delays** — `WithWorkflowDelay` schedules a one-shot run in the future; the delay survives restarts.
- **Inter-workflow messaging** — `Send` / `Recv` with topics.
- **Workflow events** — `SetEvent` / `GetEvent` to expose progress to the outside world.
- **Durable sleep** — `Sleep` survives restarts.
- **Cron scheduling** — `WithSchedule("* * * * * *")` registers a cron-driven workflow.
- **Recovery** — interrupted workflows automatically resume on `Launch`.
- **Low latency in-process** — same-process `Send`→`Recv`, `SetEvent`→`GetEvent`, enqueue→dispatch and completion→`GetResult` are signal-driven (near-instant); the configured poll intervals only matter as the cross-process fallback.
- **Caller-scoped cancellation** — opt in to `WithCallerContext(r.Context())` to tie a workflow's lifetime to an HTTP request (or any `context.Context`); the workflow stops when the caller disconnects and `GetResult` returns `context.Canceled`.
- **Management APIs** — `ListWorkflows`, `CancelWorkflow`, `ResumeWorkflow`, `ForkWorkflow`, `DeleteWorkflows`, `GCWorkflows`.
- **Admin HTTP handler** — `orc.AdminHandler(ctx)` returns a `http.Handler` you can mount under any prefix in your own `net/http` mux for live monitoring, cancel/resume/fork, parent/child tree inspection, and per-workflow durations.
- **Bundled web dashboard** — `web.Handler("/admin")` from the `ella.to/orc/web` sub-package serves a single-file HTML/JS UI that talks to the admin handler — paginated workflow list with filters, live status counters, per-workflow detail with steps/children/tree, and stop / restart / fork / delete buttons. No build step, no external assets.
- **Single binary** — only dependency at runtime is the SQLite file.

---

## Installation

```bash
go get ella.to/orc@v0.0.2
```

`orc` is developed and tested on Go 1.25. Earlier versions may work as long
as they support generics and `log/slog`, but 1.25 is what the module file
declares.

---

## 60-second tour

```go
package main

import (
    "context"
    "fmt"
    "time"

    "ella.to/orc"
)

// A workflow is just a Go function with the signature
//   func(*orc.Context, In) (Out, error)
func greet(c *orc.Context, name string) (string, error) {
    return "hello, " + name, nil
}

func main() {
    ctx, err := orc.NewContext(context.Background(), orc.Config{
        AppName:      "demo",
        DatabasePath: "demo.db",
    })
    if err != nil { panic(err) }

    orc.RegisterWorkflow[string, string](ctx, greet)
    if err := orc.Launch(ctx); err != nil { panic(err) }
    defer orc.Shutdown(ctx, 5*time.Second)

    h, _ := orc.RunWorkflow[string, string](ctx, greet, "world")
    out, _ := h.GetResult(orc.WithHandleTimeout(2 * time.Second))
    fmt.Println(out) // hello, world
}
```

---

## Core concepts

| Concept            | API                                                     | Notes                                                                                |
| ------------------ | ------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| **Context**        | `orc.NewContext` / `orc.Launch` / `orc.Shutdown`        | The root runtime object. Pass it to every workflow.                                  |
| **Workflow**       | `orc.RegisterWorkflow` / `orc.RunWorkflow`              | A registered Go function whose lifetime is checkpointed.                             |
| **Step**           | `orc.RunAsStep`                                         | Idempotent leaf operation. Output is checkpointed; replays skip it.                  |
| **Handle**         | `WorkflowHandle[O]` returned by `RunWorkflow`           | Use `GetResult` / `GetStatus`.                                                       |
| **Queue**          | `orc.NewWorkflowQueue` + `orc.WithQueue`                | Background dispatcher with concurrency / rate limits / priority / deduplication.     |
| **Notifications**  | `orc.Send` / `orc.Recv[T]`                              | Pass typed messages between workflows on a topic.                                    |
| **Events**         | `orc.SetEvent` / `orc.GetEvent[T]`                      | Workflow publishes progress; readers (inside or outside) poll.                       |
| **Sleep**          | `orc.Sleep`                                             | Durable timer. Wakeup time is checkpointed.                                          |
| **Schedule**       | `orc.WithSchedule("* * * * * *")` at registration time  | Robfig cron, with seconds granularity.                                               |
| **Recovery**       | Implicit at `Launch`                                    | Picks up `PENDING` workflows + `ENQUEUED` workflows assigned to this `ExecutorID`.   |
| **Management**     | `orc.ListWorkflows`, `CancelWorkflow`, `ResumeWorkflow`, `ForkWorkflow`, `DeleteWorkflows`, `GCWorkflows`, `RetrieveWorkflow`, `GetWorkflowSteps` | Programmatic admin. |
| **Admin HTTP**     | `orc.AdminHandler(ctx)`                                 | A `net/http` handler that exposes all of the above as a JSON API.                    |
| **Web dashboard**  | `ella.to/orc/web` → `web.Handler(apiBase)`              | Embedded single-file HTML/JS UI for the admin API. Mount alongside `AdminHandler`.   |

---

## Configuration

```go
orc.Config{
    AppName:                  "myapp",          // required
    DatabasePath:             "data/orc.db",    // empty = in-memory (tests only)
    Database:                 nil,              // optional: bring your own *sqlite.Database
    PoolSize:                 8,                // SQLite connection pool size
    Logger:                   slog.Default(),   // *slog.Logger
    Serializer:               nil,              // defaults to JSON envelope
    ApplicationVersion:       "v1.2.0",         // for blue/green rollouts
    ExecutorID:               "node-a",         // identifies this process
    MaxRecoveryAttempts:           50,                     // safety cap on recovery loops
    QueuePollInterval:             50 * time.Millisecond,  // queue dispatch tick
    NotificationPollInterval:      50 * time.Millisecond,  // Recv / GetEvent / Sleep wakeup floor
    CancelPollInterval:            250 * time.Millisecond, // cross-process cancel pickup latency
    QueueDispatchLogRetention:     time.Hour,              // floor for how long rate-limit rows live
    QueueDispatchLogPurgeInterval: time.Minute,            // how often the janitor runs
    SkipMigrations:                false,                  // disable auto-migration
}
```

Environment variables `DBOS__APPVERSION` and `DBOS__VMID` are read for
`ApplicationVersion` / `ExecutorID` when those fields are empty.

> **Note on poll intervals.** Within a single process, orc is signal-driven:
> a `Send`, `SetEvent`, enqueue or workflow completion wakes its local waiters
> immediately. `QueuePollInterval` and `NotificationPollInterval` are the
> *fallback* cadence, which is what bounds latency when the producer lives in
> a **different process** sharing the same database file. If you run one
> process per DB (the recommended shape), the defaults are nearly free —
> idle queue polls are read-only and skip SQLite's writer lock entirely.

---

## Workflows

A workflow function is just:

```go
func myWorkflow(c *orc.Context, in MyInput) (MyOutput, error) { … }
```

You register it before calling `Launch`:

```go
orc.RegisterWorkflow[MyInput, MyOutput](ctx, myWorkflow,
    orc.WithWorkflowName("my-workflow"),  // stable, refactor-proof name
    orc.WithMaxRecoveryAttempts(10),
)
```

Registration validates the function's signature up front: a function that
isn't `func(*orc.Context, I) (O, error)` (or the no-input variant
`func(*orc.Context) (O, error)`), or whose types don't match the generic
parameters, panics immediately with a message explaining what was expected —
rather than failing on its first execution.

You start it with:

```go
h, err := orc.RunWorkflow[MyInput, MyOutput](ctx, myWorkflow, in,
    orc.WithWorkflowID("explicit-id"),    // makes the call idempotent
    orc.WithQueue("background"),          // run in the background
    orc.WithDeduplicationID("logical-key"),
    orc.WithPriority(1),
    orc.WithWorkflowTimeout(30*time.Second),
    orc.WithWorkflowDelay(5*time.Second), // delay first dispatch
    orc.WithCallerContext(r.Context()),   // cancel with caller (e.g. HTTP request)
)
out, err := h.GetResult(orc.WithHandleTimeout(time.Minute))
```

If you call `RunWorkflow` twice with the **same `WorkflowID` and same input**,
you get a handle to the **existing** run instead of starting a new one — that's
the durable-idempotent contract.

`WithWorkflowDelay(d)` records the run as `DELAYED` and dispatches it once
`d` has elapsed — durably, so the pending delay survives a restart. It works
with or without `WithQueue` (without one, the run is routed through orc's
internal queue and you get a polling handle).

### Child workflows

Calling `RunWorkflow` *from inside* another workflow spawns a durable child —
and the spawn itself is checkpointed as a step in the parent:

```go
func parent(c *orc.Context, n int) (int, error) {
    h, err := orc.RunWorkflow[int, int](c, childWf, n) // no explicit ID needed
    if err != nil { return 0, err }
    return h.GetResult(orc.WithHandleTimeout(time.Minute))
}
```

- Without `WithWorkflowID`, the child's ID is derived deterministically from
  `(parent ID, step number)`.
- The child's ID is recorded in the parent's step log
  (`operation_outputs.child_workflow_id`), so if the parent crashes and is
  replayed it **re-attaches to the same child** instead of spawning a
  duplicate.
- Children appear in the admin API/dashboard under the parent's
  `/children` and `/tree` views.

As with steps, the usual determinism rule applies: spawn children in the
same order on every replay.

### Steps

Steps are how you make side-effecting operations restart-safe:

```go
func processOrder(c *orc.Context, orderID string) (string, error) {
    chargeID, err := orc.RunAsStep(c, func(ctx context.Context) (string, error) {
        return chargeStripe(ctx, orderID)
    }, orc.WithStepName("charge_card"),
       orc.WithStepMaxRetries(5),
       orc.WithStepRetryBackoff(100*time.Millisecond, 5*time.Second, 2.0))
    if err != nil { return "", err }

    return chargeID, nil
}
```

- The step's output is recorded in SQLite **before** the workflow advances.
- On replay, `RunAsStep` looks up the recorded output and returns it without re-running the function.
- `WithStepMaxRetries` enables in-process retries with exponential backoff before persisting failure.

---

## Queues

A queue lets you enqueue many workflows for background processing under a
shared concurrency / rate-limit budget:

```go
q := orc.NewWorkflowQueue(ctx, "background",
    orc.WithWorkerConcurrency(4),                                      // 4 in-flight at once
    orc.WithRateLimiter(&orc.RateLimiter{Limit: 100, Period: time.Minute}),
    orc.WithPriorityEnabled(),                                         // dispatch lowest-priority first
)

orc.RunWorkflow[string, string](ctx, processOrder, "order-1",
    orc.WithQueue(q.Name),
    orc.WithPriority(0),
    orc.WithDeduplicationID("order-1"), // collapses duplicate enqueues
)
```

### Notifications

```go
// Inside workflow A:
orc.Send(c, "workflow-b-id", "ping", "topic-x")

// Inside workflow B (must be running):
msg, err := orc.Recv[string](c, "topic-x", 30*time.Second)
```

`Send` and `Recv` are durable when invoked inside a workflow — they record their
operation as a step so a re-execution doesn't double-send / double-receive.

On timeout, `Recv` returns T's zero value with a **nil error** — use a pointer
or wrapper type for T if you need to distinguish "timed out" from a legitimate
zero-value message.

### Events

```go
// Inside workflow:
orc.SetEvent(c, "stage", "uploading")
// … later …
orc.SetEvent(c, "stage", "encoding")
orc.SetEvent(c, "stage", "done")

// Outside (or in another workflow):
stage, _ := orc.GetEvent[string](ctx, workflowID, "stage", 5*time.Second)
```

### Sleep

```go
orc.Sleep(c, 10*time.Minute) // durable: re-execution skips elapsed sleeps
```

### Cron

```go
orc.RegisterWorkflow[string, string](ctx, hourlyJob,
    orc.WithWorkflowName("hourly_job"),
    orc.WithSchedule("0 0 * * * *"), // robfig/cron with seconds field
)
```

### Management

```go
ws, _ := orc.ListWorkflows(ctx,
    orc.WithListWorkflowStatus(orc.WorkflowStatusError),
    orc.WithListWorkflowLimit(50),
)
// Programmatic name/queue/executor filters match EXACTLY. (The admin HTTP
// API's equivalents use substring matching, for interactive search.)

_ = orc.CancelWorkflow(ctx, "wf-123")
h, _ := orc.ResumeWorkflow[string](ctx, "wf-123")
h, _ := orc.ForkWorkflow[string](ctx, orc.ForkWorkflowInput{
    OriginalWorkflowID: "wf-123",
    StartFromStep:      2, // re-use step outputs 0..1, re-run from step 2
})
_ = orc.DeleteWorkflows(ctx, []string{"wf-123"})

// Bulk GC. Permanently delete every SUCCESS workflow last updated more
// than a week ago (and cascade to its steps / events / notifications).
res, _ := orc.GCWorkflows(ctx, orc.GCWorkflowsInput{
    Statuses:      []orc.WorkflowStatusType{orc.WorkflowStatusSuccess},
    UpdatedBefore: time.Now().Add(-7 * 24 * time.Hour),
})
fmt.Printf("gc: deleted=%d failed=%d\n", res.Deleted, res.Failed)

steps, _ := orc.GetWorkflowSteps(ctx, "wf-123")
```

---

## Examples

Each of these is a complete, copy-pasteable program. They exercise the same
patterns covered by the project's test suite (see `examples_test.go`).

### Example 1 — Multi-step durable payment pipeline

```go
package main

import (
    "context"
    "fmt"
    "time"

    "ella.to/orc"
)

type paymentRequest struct {
    OrderID string
    Cents   int64
    Card    string
}

type paymentResult struct {
    OrderID  string
    ChargeID string
    Email    bool
}

func processPayment(c *orc.Context, req paymentRequest) (paymentResult, error) {
    chargeID, err := orc.RunAsStep(c, func(ctx context.Context) (string, error) {
        // call Stripe / Braintree / etc.
        return "charge-" + req.OrderID, nil
    }, orc.WithStepName("charge_card"))
    if err != nil { return paymentResult{}, err }

    _, err = orc.RunAsStep(c, func(ctx context.Context) (string, error) {
        // write to ledger DB
        return "ledger-" + chargeID, nil
    }, orc.WithStepName("insert_ledger"))
    if err != nil { return paymentResult{}, err }

    sent, err := orc.RunAsStep(c, func(ctx context.Context) (bool, error) {
        // send confirmation email
        return true, nil
    }, orc.WithStepName("send_email"))
    if err != nil { return paymentResult{}, err }

    return paymentResult{OrderID: req.OrderID, ChargeID: chargeID, Email: sent}, nil
}

func main() {
    ctx, _ := orc.NewContext(context.Background(), orc.Config{
        AppName:      "payments",
        DatabasePath: "payments.db",
    })
    orc.RegisterWorkflow[paymentRequest, paymentResult](ctx, processPayment,
        orc.WithWorkflowName("process_payment"))
    _ = orc.Launch(ctx)
    defer orc.Shutdown(ctx, 5*time.Second)

    req := paymentRequest{OrderID: "ord-123", Cents: 9999, Card: "4111-1111-1111-1111"}
    h, _ := orc.RunWorkflow[paymentRequest, paymentResult](ctx, processPayment, req,
        orc.WithWorkflowID("ord-123")) // idempotent on retry
    res, _ := h.GetResult(orc.WithHandleTimeout(time.Minute))
    fmt.Printf("result: %+v\n", res)
}
```

If your process crashes between the charge and the ledger insert, `Launch`
will re-run the workflow on restart. Step 0 (`charge_card`) is replayed from
its recorded output, step 1 (`insert_ledger`) actually executes, and the
external charge is **not** double-billed.

### Example 2 — Background queue with concurrency limit

```go
q := orc.NewWorkflowQueue(ctx, "fulfillment", orc.WithWorkerConcurrency(2))

for _, id := range orderIDs {
    orc.RunWorkflow[string, string](ctx, fulfillOrder, id, orc.WithQueue(q.Name))
}
// At most two orders run in parallel. Restarting the process picks up where
// you left off.
```

### Example 3 — Rate-limited webhook delivery

```go
q := orc.NewWorkflowQueue(ctx, "webhooks",
    orc.WithRateLimiter(&orc.RateLimiter{
        Limit:  100,
        Period: time.Minute,
    }),
)

orc.RunWorkflow[Webhook, struct{}](ctx, deliverWebhook, w, orc.WithQueue(q.Name))
```

### Example 4 — Two workflows talking via Send/Recv

```go
// Listener
func listener(c *orc.Context, _ string) (string, error) {
    msg, err := orc.Recv[string](c, "greetings", 30*time.Second)
    if err != nil { return "", err }
    return "got:" + msg, nil
}

// Speaker
func speaker(c *orc.Context, target string) (string, error) {
    return "ok", orc.Send(c, target, "hello, world", "greetings")
}

orc.RegisterWorkflow[string, string](ctx, listener, orc.WithWorkflowName("listener"))
orc.RegisterWorkflow[string, string](ctx, speaker,  orc.WithWorkflowName("speaker"))
_ = orc.Launch(ctx)

hL, _ := orc.RunWorkflow[string, string](ctx, listener, "", orc.WithWorkflowID("listener-1"))
hS, _ := orc.RunWorkflow[string, string](ctx, speaker, "listener-1")
_, _ = hS.GetResult()
got, _ := hL.GetResult()
fmt.Println(got) // got:hello, world
```

### Example 5 — Long-running approval, observed via events

```go
func approval(c *orc.Context, _ string) (string, error) {
    _ = orc.SetEvent(c, "stage", "waiting")
    _, _ = orc.Sleep(c, 24*time.Hour) // durable
    _ = orc.SetEvent(c, "stage", "approved")
    return "approved", nil
}

// Outside the workflow (e.g. from an HTTP handler):
stage, _ := orc.GetEvent[string](ctx, "approval-1", "stage", 0)
fmt.Println("current stage:", stage)
```

### Example 6 — Saga / compensation

```go
func bookTrip(c *orc.Context, _ string) (string, error) {
    _, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
        return bookHotel(), nil
    }, orc.WithStepName("book_hotel"))
    if err != nil { return "", err }

    if _, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
        return "", errors.New("flight provider unreachable")
    }, orc.WithStepName("book_flight")); err != nil {
        // Compensate.
        _, _ = orc.RunAsStep(c, func(_ context.Context) (string, error) {
            refundHotel()
            return "refunded", nil
        }, orc.WithStepName("refund_hotel"))
        return "", err
    }
    return "trip-booked", nil
}
```

### Example 7 — Step retries with exponential backoff

```go
func wf(c *orc.Context, _ string) (string, error) {
    return orc.RunAsStep(c, callFlakyAPI,
        orc.WithStepName("call_api"),
        orc.WithStepMaxRetries(5),
        orc.WithStepRetryBackoff(100*time.Millisecond, 10*time.Second, 2.0),
    )
}
```

### Example 8 — Fan-out / fan-in

```go
func parent(c *orc.Context, n int) (int, error) {
    q := orc.NewWorkflowQueue(c, "fanout", orc.WithWorkerConcurrency(8))
    handles := make([]orc.WorkflowHandle[int], n)
    for i := 0; i < n; i++ {
        h, _ := orc.RunWorkflow[int, int](c, doubler, i, orc.WithQueue(q.Name))
        handles[i] = h
    }
    sum := 0
    for _, h := range handles {
        v, err := h.GetResult(orc.WithHandleTimeout(time.Minute))
        if err != nil { return 0, err }
        sum += v
    }
    return sum, nil
}
```

### Example 9 — Cron schedule

```go
orc.RegisterWorkflow[string, string](ctx, hourlyDigest,
    orc.WithWorkflowName("hourly_digest"),
    orc.WithSchedule("0 0 * * * *"), // top of every hour (with seconds field)
)
_ = orc.Launch(ctx)
```

The cron format is robfig/cron's 6-field variant: `seconds minutes hours dom month dow`.

Each tick enqueues a workflow with a deterministic ID derived from the
tick's whole-second boundary, so multiple processes pointing at the same
SQLite database collapse concurrent firings to a single workflow row —
cron is at-most-once across the cluster.

### Example 10 — Cancel an in-flight workflow

```go
func longJob(c *orc.Context, _ string) (string, error) {
    // To get prompt cancellation, propagate the error from durable
    // primitives (Sleep, Recv) and check the underlying ctx in your
    // own steps.
    if _, err := orc.Sleep(c, 30*time.Second); err != nil {
        return "", err
    }
    return "done", nil
}

h, _ := orc.RunWorkflow[string, string](ctx, longJob, "", orc.WithWorkflowID("job-1"))

// later, from anywhere (including a different process pointing at the
// same DB — a background poller will pick it up):
_ = orc.CancelWorkflow(ctx, "job-1")

_, err := h.GetResult(orc.WithHandleTimeout(5*time.Second))
fmt.Println(err) // workflow cancelled
```

`CancelWorkflow` cancels the workflow's `context.Context` with cause
`orc.ErrWorkflowCancelledErr`. Steps that honour `ctx.Done()` (the
built-in `Sleep` / `Recv`, plus any HTTP/DB calls in your own step
closures that take `ctx`) return promptly. If a workflow swallows the
cancel error and tries to return normally, the runtime still records
the run as `CANCELLED`.

### Example 10b — Cancel a workflow when the HTTP request goes away

Use `orc.WithCallerContext` when a workflow is logically scoped to a
caller's lifetime (typically an HTTP handler) and should stop work the
moment the caller disconnects:

```go
func runHandler(ctx *orc.Context) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        h, err := orc.RunWorkflow[string, string](ctx, slowJob, "label",
            orc.WithCallerContext(r.Context()))
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        result, err := h.GetResult(orc.WithHandleTimeout(time.Minute))
        switch {
        case errors.Is(err, context.Canceled):
            // Client aborted. The workflow row is already marked CANCELLED.
            http.Error(w, "request cancelled", 499)
            return
        case errors.Is(err, context.DeadlineExceeded):
            http.Error(w, "request timed out", http.StatusGatewayTimeout)
            return
        case err != nil:
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        fmt.Fprintln(w, result)
    }
}
```

What happens when the client disconnects:

- `r.Context()` is cancelled by `net/http`.
- ORC propagates the cancellation into the workflow's per-run context
  with the same cause (`context.Canceled`).
- Steps that honour `ctx.Done()` — the built-in `orc.Sleep` / `orc.Recv`,
  plus any HTTP/DB calls in your own step closures that take the step's
  `ctx` — return promptly with `context.Canceled`.
- The workflow's `GetResult` returns `context.Canceled`, so the handler
  can detect it via `errors.Is(err, context.Canceled)`.
- The workflow row is recorded as `CANCELLED` (not `ERROR`), with the
  original error message preserved on the row's `error` field.

This is **opt-in**. By default ORC workflows are decoupled from any
caller's context — they keep running even after `RunWorkflow` returns,
which is the right default for durable background work. Reach for
`WithCallerContext` only when you want request-scoped lifetime.

`WithCallerContext` only takes effect for workflows that execute in
this process (no `WithQueue`). For queued workflows, observe the
caller context yourself and call `orc.CancelWorkflow` when it fires.

See [`examples/15-http-context-cancel`](./examples/15-http-context-cancel)
for a runnable demo.

### Example 11 — Resume a failed workflow

```go
// Some workflow ended in ERROR state.
h, _ := orc.ResumeWorkflow[string](ctx, "wf-123")
result, _ := h.GetResult(orc.WithHandleTimeout(time.Minute))
```

`ResumeWorkflow` puts the row back in the internal queue, resets attempts,
and the queue runner re-dispatches it. Steps that already succeeded are
skipped on replay.

### Example 12 — Fork from a checkpoint

```go
// Re-run wf-123 starting from step 2, keeping the outputs of steps 0 and 1.
h, _ := orc.ForkWorkflow[string](ctx, orc.ForkWorkflowInput{
    OriginalWorkflowID: "wf-123",
    StartFromStep:      2,
})
```

### Example 13 — Deduplicated enqueues

```go
q := orc.NewWorkflowQueue(ctx, "dedup-q")

for i := 0; i < 10; i++ {
    go orc.RunWorkflow[string, string](ctx, wf, "payload",
        orc.WithQueue(q.Name),
        orc.WithDeduplicationID("logical-key"), // only one row will be created
    )
}
```

### Example 14 — Mounting the admin HTTP server (and the web dashboard)

```go
import (
    "ella.to/orc"
    "ella.to/orc/web"
)

ctx, _ := orc.NewContext(context.Background(), orc.Config{
    AppName:      "admin-demo",
    DatabasePath: "admin-demo.db",
})
defer orc.Shutdown(ctx, 5*time.Second)

orc.RegisterWorkflow[string, string](ctx, longJob, orc.WithWorkflowName("long_job"))
_ = orc.Launch(ctx)

mux := http.NewServeMux()

// JSON admin API.
mux.Handle("/admin/", http.StripPrefix("/admin",
    orc.AdminHandler(ctx, orc.WithAdminPrettyJSON()),
))

// Bundled HTML/JS dashboard. The argument is the URL prefix at which the
// admin handler above is mounted; the UI calls "{apiBase}/workflows", etc.
mux.Handle("/ui/", http.StripPrefix("/ui", web.Handler("/admin")))

log.Fatal(http.ListenAndServe(":8080", mux))
```

Then:

```bash
open  http://localhost:8080/ui/                         # web dashboard
curl  http://localhost:8080/admin/                      # route index
curl  http://localhost:8080/admin/info                  # registered workflows + queue summary
curl  http://localhost:8080/admin/workflows             # list everything
curl  http://localhost:8080/admin/workflows/job-1       # detail (status, duration, steps, children)
curl  http://localhost:8080/admin/workflows/job-1/tree  # parent + descendants as a tree
curl  -X POST http://localhost:8080/admin/workflows/job-1/cancel
curl  -X POST http://localhost:8080/admin/workflows/job-1/resume
```

See [Admin HTTP server](#admin-http-server) below for the full route table,
and [Web dashboard](#web-dashboard) for the UI.

### Example 15 — Listing & filtering workflows

```go
recent, _ := orc.ListWorkflows(ctx,
    orc.WithListWorkflowStatus(orc.WorkflowStatusSuccess, orc.WorkflowStatusError),
    orc.WithListWorkflowTimeRange(time.Now().Add(-24*time.Hour), time.Now()),
    orc.WithListWorkflowSortDescending(),
    orc.WithListWorkflowLimit(100),
)

for _, w := range recent {
    fmt.Printf("%s  %-20s  %s\n", w.ID, w.Name, w.Status)
}
```

---

## Admin HTTP server

`orc.AdminHandler(ctx, opts...)` returns an `http.Handler` that exposes a
read-and-write JSON admin API for the running ORC `Context`. It uses only
the Go standard library (`net/http` `ServeMux` with method-based pattern
matching introduced in Go 1.22) so you can mount it inside any existing
HTTP server you already have, under any path prefix you like, with no
additional dependencies.

> **Authentication and authorization are out of scope.** The handler does
> not enforce any auth — protect it at a higher layer (reverse proxy,
> middleware, internal-only listener, mTLS, etc.) before exposing it
> externally.

### Mounting

Use `http.StripPrefix` so the handler's internal routes (e.g. `/workflows`)
resolve correctly under your chosen prefix:

```go
mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", orc.AdminHandler(ctx)))
log.Fatal(http.ListenAndServe(":8080", mux))
```

You can mount it under any prefix — `/orc/admin`, `/internal/jobs`, even
just `/`:

```go
mux.Handle("/orc/admin/", http.StripPrefix("/orc/admin", orc.AdminHandler(ctx)))
```

### Options

| Option                                         | Purpose                                                                                           |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `orc.WithAdminPrettyJSON()`                    | Indent JSON responses (handy in browsers / `curl`).                                               |
| `orc.WithAdminDefaultListLimit(n)`             | Default page size for `GET /workflows` when `?limit` is missing (default `100`).                  |
| `orc.WithAdminMaxListLimit(n)`                 | Upper bound on `?limit` for `GET /workflows` (default `1000`).                                    |
| `orc.WithAdminMaxScanLimit(n)`                 | Cap on rows scanned in-memory when building `/tree`, `/children`, queue counts (default `5000`).  |

### Routes

All paths below are relative to wherever you mounted the handler. With the
`/admin` mount above, `GET /workflows` becomes `GET /admin/workflows`.

| Method   | Path                              | Description                                                                                              |
| -------- | --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `GET`    | `/`                               | Discovery: returns the route index (so the API is self-documenting).                                     |
| `GET`    | `/health`                         | Liveness probe; returns `{"status":"ok","now":"..."}`.                                                   |
| `GET`    | `/info`                           | Executor info, registered workflow names, queue summaries with pending/running counts, status counts.    |
| `GET`    | `/registered`                     | Sorted list of registered workflow names.                                                                |
| `GET`    | `/queues`                         | Registered queues + per-queue pending and running counts.                                                |
| `GET`    | `/workflows`                      | List workflows with filters (see params below).                                                          |
| `POST`   | `/workflows/delete`               | Bulk delete. Body: `{"ids":["a","b"]}`.                                                                  |
| `POST`   | `/workflows/gc`                   | Garbage-collect by status + cutoff. Body: `{"statuses":[...], "updated_before":"RFC3339", "allow_non_terminal":false}`. Returns `{"deleted":N, "failed":N, "errors":[...]}`. |
| `GET`    | `/workflows/{id}`                 | Workflow detail: status, computed duration, all checkpointed steps, direct child workflows.              |
| `DELETE` | `/workflows/{id}`                 | Hard-delete a single workflow and its dependent rows.                                                    |
| `GET`    | `/workflows/{id}/steps`           | List the workflow's checkpointed steps.                                                                  |
| `GET`    | `/workflows/{id}/children`        | Direct child workflows (workflows whose `parent_workflow_id` equals `{id}`).                             |
| `GET`    | `/workflows/{id}/tree`            | Workflow + descendants as a tree. Optional `?depth=N` truncates depth (`0` = root only).                 |
| `POST`   | `/workflows/{id}/cancel`          | Cancel a workflow (same semantics as `orc.CancelWorkflow`).                                              |
| `POST`   | `/workflows/{id}/resume`          | Resume a failed/cancelled workflow (same semantics as `orc.ResumeWorkflow`).                             |
| `POST`   | `/workflows/{id}/fork`            | Fork. Body: `{"start_from_step": int, "new_workflow_id": "optional"}`. Returns `{"new_workflow_id":...}`. |

### `GET /workflows` query parameters

All parameters are optional and may be combined.

| Param      | Type / format                                | Description                                                                          |
| ---------- | -------------------------------------------- | ------------------------------------------------------------------------------------ |
| `name`     | string                                       | Substring (fuzzy) match on workflow name.                                            |
| `status`   | repeatable enum                              | One of `PENDING`, `ENQUEUED`, `DELAYED`, `SUCCESS`, `ERROR`, `CANCELLED`, `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. Repeat to OR-filter. |
| `queue`    | string                                       | Substring (fuzzy) match on queue name.                                               |
| `executor` | string                                       | Substring (fuzzy) match on executor id.                                              |
| `id`       | repeatable string                            | Return only workflows whose id is in this set. Repeat for multiple ids.              |
| `start`    | RFC3339 timestamp                            | Filter by `created_at >= start`.                                                     |
| `end`      | RFC3339 timestamp                            | Filter by `created_at <= end`.                                                       |
| `limit`    | int                                          | Page size (default `100`, capped at `1000`). Configurable via `WithAdminDefaultListLimit` / `WithAdminMaxListLimit`. |
| `offset`   | int                                          | Pagination offset (rows to skip). Mutually-exclusive with `page` — when both are set, `page` wins. |
| `page`     | int (1-based)                                | Convenience alternative to `offset`. Equivalent to `offset = (page - 1) * limit`. Returns `400` for `page < 1`. |
| `sort_by`  | enum (`created`,`name`,`status`,`queue`,`attempts`,`duration`) | Column to sort by. Default `created`.                                                 |
| `sort_dir` | enum (`asc`,`desc`)                          | Sort direction. Default `desc` in the bundled UI; API default is legacy `desc=false` unless explicitly set. |
| `desc`     | bool (`true`/`false`)                        | Legacy fallback when `sort_dir` is absent. Kept for compatibility.                   |
| `load_io`  | bool                                         | Include `input` / `output` in each row. Default `true`.                              |

### Response shape — `AdminWorkflowView`

Most workflow endpoints return rows shaped like this. The `duration_*` and
`age_*` fields are computed at query time so you don't have to do any
arithmetic to answer "how long has this been running?":

```json
{
  "workflow_id":         "job-1",
  "name":                "long_job",
  "status":              "PENDING",
  "queue_name":          "demo-queue",
  "executor_id":         "local",
  "application_version": "dev",
  "attempts":            1,
  "parent_workflow_id":  "parent-1",
  "created_at":          "2026-04-26T12:00:00Z",
  "started_at":          "2026-04-26T12:00:00.5Z",
  "updated_at":          "2026-04-26T12:00:03Z",
  "duration_ms":         2500,
  "duration_human":      "2.5s",
  "age_ms":              3000,
  "age_human":           "3.0s",
  "is_terminal":         false,
  "is_running":          true,
  "input":               { "...": "..." },
  "output":              null
}
```

`GET /workflows/{id}` additionally returns the `steps` (`AdminStepView[]`)
and direct `children` (`AdminWorkflowView[]`).

`GET /workflows/{id}/tree` returns an `AdminWorkflowTreeNode` — the same
view embedded in a recursive `children` array.

### Paginated list response — `AdminListResponse`

`GET /workflows` wraps the rows in an envelope that carries everything you
need to render a "Page X of Y · N total" UI without a second round trip:

```json
{
  "items":     [ /* AdminWorkflowView, ... */ ],
  "limit":     50,
  "offset":    100,
  "count":     50,
  "total":     327,
  "page":      3,
  "page_size": 50
}
```

| Field       | Meaning                                                                                       |
| ----------- | --------------------------------------------------------------------------------------------- |
| `items`     | The workflow rows for this page.                                                              |
| `limit`     | Echo of the effective `?limit` (after defaulting and clamping).                               |
| `offset`    | Echo of the effective offset (whether supplied as `?offset` or derived from `?page`).         |
| `count`     | `len(items)` — the size of this batch (≤ `limit`).                                            |
| `total`     | Total rows matching the request's filters across **all** pages.                               |
| `page`      | 1-based current page. `(offset / limit) + 1`.                                                 |
| `page_size` | Same as `limit`, named for symmetry with `page`.                                              |

Compute the last page as `ceil(total / page_size)`. Counting uses the same
`WHERE` clause as the list query, so totals match exactly what an
unpaginated request would return.

### Garbage collection — `POST /workflows/gc`

`GCWorkflows` (and the `POST /workflows/gc` endpoint that wraps it) is
designed for bulk database hygiene: as the workflow table grows, range
scans, status counts and the workflow list all get slower. A periodic GC
keeps the table compact and the cascading deletes (`operation_outputs`,
`notifications`, `workflow_events`) happen automatically via the schema's
`ON DELETE CASCADE`.

**Request body**

```json
{
  "statuses":           ["SUCCESS", "ERROR"],
  "updated_before":     "2026-04-01T00:00:00Z",
  "allow_non_terminal": false
}
```

| Field                | Type / format         | Notes                                                                 |
| -------------------- | --------------------- | --------------------------------------------------------------------- |
| `statuses`           | non-empty enum array  | Lowercase is normalised to uppercase. By default only terminal statuses (`SUCCESS`, `ERROR`, `CANCELLED`, `MAX_RECOVERY_ATTEMPTS_EXCEEDED`) are accepted; passing a non-terminal status without `allow_non_terminal` returns `400`. |
| `updated_before`     | RFC3339 timestamp     | Required. Only rows whose `updated_at` is **strictly less than** this cutoff are deleted, so the cutoff is exclusive. |
| `allow_non_terminal` | bool                  | Optional. Set to `true` to also GC `PENDING` / `ENQUEUED` / `DELAYED` rows — useful for pruning zombies but dangerous for live workflows. Default `false`. |

**Response body**

```json
{
  "deleted": 42,
  "failed":  0,
  "errors":  []
}
```

| Field     | Meaning                                                                                       |
| --------- | --------------------------------------------------------------------------------------------- |
| `deleted` | Total number of `workflow_status` rows actually removed across all requested statuses.        |
| `failed`  | Best-effort estimate of rows that matched the filter but couldn't be deleted (zero in the happy path; populated when a per-status DELETE returns an error). |
| `errors`  | Per-status error messages, if any.                                                            |

**Implementation notes**

- Each status runs in its own DELETE statement, so a partial failure
  (one status fails) does not block the others.
- Counting and deletion share the same `WHERE` clause, so totals reported
  in `failed` match exactly what the GC would have considered.
- This endpoint is a thin wrapper over `orc.GCWorkflows` — see the
  Management section above for the equivalent in-process API.

### Errors

Any non-2xx response has a JSON body of the form:

```json
{ "error": "workflow not found", "code": 404 }
```

| Status | Meaning                                                           |
| ------ | ----------------------------------------------------------------- |
| `400`  | Bad request: invalid query param, invalid JSON body, empty `ids`. |
| `404`  | Workflow not found.                                               |
| `500`  | Internal error from the system DB / runtime.                      |

### Quick examples

```bash
# discover the available routes
curl http://localhost:8080/admin/

# system overview: registered workflows, queues, status counts
curl http://localhost:8080/admin/info

# list everything that's currently in flight
curl 'http://localhost:8080/admin/workflows?status=PENDING&status=ENQUEUED'

# paginated: page 3 of completed workflows, 50 per page, newest first
curl 'http://localhost:8080/admin/workflows?status=SUCCESS&sort_by=created&sort_dir=desc&limit=50&page=3'

# equivalent using offset
curl 'http://localhost:8080/admin/workflows?status=SUCCESS&sort_by=created&sort_dir=desc&limit=50&offset=100'

# sort alphabetically by name
curl 'http://localhost:8080/admin/workflows?sort_by=name&sort_dir=asc&limit=25'

# detail for one workflow (status + steps + children + duration)
curl http://localhost:8080/admin/workflows/parent-1

# parent + descendants as a tree
curl http://localhost:8080/admin/workflows/parent-1/tree

# only the immediate children
curl http://localhost:8080/admin/workflows/parent-1/children

# cancel a running job
curl -X POST http://localhost:8080/admin/workflows/job-1/cancel

# resume a failed job
curl -X POST http://localhost:8080/admin/workflows/flaky-1/resume

# fork from step 2 (re-use steps 0..1, re-run from step 2)
curl -X POST -d '{"start_from_step":2}' \
     http://localhost:8080/admin/workflows/job-1/fork

# bulk delete
curl -X POST -d '{"ids":["job-1","job-2"]}' \
     http://localhost:8080/admin/workflows/delete

# garbage collect: prune SUCCESS rows older than a week
curl -X POST -H 'Content-Type: application/json' \
     -d "{\"statuses\":[\"SUCCESS\"],\"updated_before\":\"$(date -u -v-7d '+%Y-%m-%dT%H:%M:%SZ')\"}" \
     http://localhost:8080/admin/workflows/gc
# -> {"deleted":42,"failed":0}
```

A complete runnable example with seeded workflows lives in
[`examples/14-admin-http`](./examples/14-admin-http).

---

## Web dashboard

`ella.to/orc/web` is a tiny sub-package whose only public symbol is:

```go
func Handler(apiBase string) http.Handler
```

It returns an `http.Handler` that serves a single, embedded `index.html`
containing the full ORC admin UI (HTML + CSS + JS, no external assets,
no build step). The page uses hash routing, so any unknown sub-path
(`/ui/foo/bar`) still serves the same document and the client takes over
from there.

`apiBase` is the URL prefix at which `orc.AdminHandler` is mounted —
the UI calls `{apiBase}/workflows`, `{apiBase}/info`, etc. Pass an empty
string to default to `"/admin"`. The value is baked into the served HTML
once at handler-construction time; it can also be overridden at runtime
with the `?api=/some/path` query string (handy when the UI is opened from
a different origin).

### Mounting alongside the admin handler

```go
import (
    "ella.to/orc"
    "ella.to/orc/web"
)

mux := http.NewServeMux()
mux.Handle("/admin/", http.StripPrefix("/admin", orc.AdminHandler(ctx)))
mux.Handle("/ui/",    http.StripPrefix("/ui",    web.Handler("/admin")))
http.ListenAndServe(":8080", mux)
```

Open <http://localhost:8080/ui/> in a browser.

### What it shows

- **Top bar** with app name, executor id, configured API base, a
  health dot, and an auto-refresh toggle (default on, 2 s polling).
- **Workflows list** with:
  - Status counters at the top (click a counter to filter).
  - Filters: status (multi-select), name, queue, page size — all
    persisted in the URL search params.
  - Pagination: First / Prev / Next / Last + a "Go to: N" jump input,
    showing `Page X of Y · N total · K per page`. Auto-redirects to
    the last valid page if data shrinks underneath you.
  - Live-ticking "Duration" for `PENDING` rows; frozen "Duration" for
    terminal rows; relative "Created" timestamp.
  - **Garbage collection panel** (collapsed by default) that wraps
    `POST /workflows/gc`: pick statuses, pick a cutoff (with quick
    presets for 1 hour / 1 day / 1 week / 1 month), confirm, and the
    result panel shows how many rows were deleted vs failed.
- **Workflow detail** — full metadata, action buttons (**Stop /
  Cancel**, **Restart / Resume**, **Fork…**, **Delete**), checkpointed
  steps with output/error, direct children (clickable), input/output
  JSON panes, and a descendant tree.
- **Queues** — registered queues with worker/global concurrency, rate
  limit, and live pending/running counts. Click a queue to jump to a
  queue-filtered workflow list.
- **Info** — executor info and registered workflow names.

> **Authentication and authorization are out of scope** for the web
> handler too — protect both `AdminHandler` and `web.Handler` behind a
> reverse proxy or middleware before exposing them externally.

---

## Patterns & best practices

1. **Wrap every external side effect in `RunAsStep`.** The whole durability
   contract relies on side effects being checkpointed.
2. **Name your steps explicitly.** Closures get unstable Go-runtime names;
   use `WithStepName("…")` so that recovery from a different binary still
   finds the recorded output.
3. **Pick stable workflow IDs** for anything you might want to retry, query,
   or cancel from the outside. `WithWorkflowID(orderID)` is a great pattern.
   (Child workflows get deterministic replay-safe IDs automatically.)
4. **Don't share mutable state between workflow invocations.** Treat your
   workflow function as a deterministic re-runnable function over its inputs
   and the recorded step outputs.
5. **Use `WithDeduplicationID`** when multiple producers might enqueue the
   same logical job.
6. **Use `ExecutorID`** to scope recovery to a single process: each process
   only resumes workflows it had previously been driving.

---

## How recovery works

On `Launch`, ORC scans for two kinds of in-flight rows owned by this
`ExecutorID`:

- `PENDING` workflows — they were running when the process died.
- `ENQUEUED` workflows with no queue — they were started directly but never
  reached the worker goroutine before shutdown.

Each is re-dispatched by name (the registry must contain a function with the
same name). The wrapper re-runs the workflow function; `RunAsStep` returns
recorded outputs for any step that already succeeded, so only the unfinished
tail of the workflow actually re-executes.

Workflows enqueued onto a real queue are picked up by the queue runner the
next time it ticks — no special recovery is needed for them.

---

## Differences from `dbos-transact-golang`

`orc` keeps the developer-facing model (workflows, steps, queues,
notifications, events, schedules, recovery, management) but trades scope for
simplicity:

- **SQLite, not Postgres.** Single-writer storage; throughput is bounded by
  SQLite's writer.
- **Polling, not LISTEN/NOTIFY.** Configurable via `QueuePollInterval` and
  `NotificationPollInterval`.
- **No Conductor cloud client, no streams, no patching system, no
  debouncer, no CLI.** These can be layered on top. (orc *does* ship a
  built-in admin HTTP handler and a bundled web dashboard — see
  [Admin HTTP server](#admin-http-server) and
  [Web dashboard](#web-dashboard) below.)

If you need any of those, build them above the public API; the durable
substrate is the same.

---

## Database schema

`orc` ships a single embedded migration that creates these tables:

- `workflow_status` — one row per workflow execution
- `operation_outputs` — one row per recorded step
- `notifications` — pending `Send` deliveries
- `workflow_events` — `SetEvent` key/value records
- `queue_dispatch_log` — used by rate-limit accounting

You can inspect everything with the standard `sqlite3` CLI; nothing about
`orc`'s data is opaque or binary.

---

## Testing

The project ships with a comprehensive test suite covering each subsystem
(workflows, steps, queues, notifications, events, sleep, recovery,
management) and 12 end-to-end "story" tests in `examples_test.go` that mirror
the examples in this README.

```bash
go test ./...
go test -race ./...
```

---

## License

See [LICENSE](LICENSE).

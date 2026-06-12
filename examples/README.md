# orc — Examples

Each subfolder is a standalone, runnable Go program that demonstrates one
feature or pattern of [`ella.to/orc`](..). They are deliberately small and
self-contained: just `cd` into one and `go run .`.

| #  | Folder                                                              | What it shows                                                                              |
| -- | ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| 01 | [`01-hello-world`](./01-hello-world)                                      | Smallest possible workflow: register, launch, run, get result.                             |
| 02 | [`02-step-retries`](./02-step-retries)                              | A flaky external call wrapped in `RunAsStep` with `WithStepMaxRetries` + backoff.          |
| 03 | [`03-fan-in-results`](./03-fan-in-results)                          | N worker workflows ship results to one aggregator workflow via `Send` / `Recv`.            |
| 04 | [`04-spawn-child-workflows`](./04-spawn-child-workflows)            | A parent workflow spawns N child workflows on a queue and awaits them.                     |
| 05 | [`05-crash-recovery`](./05-crash-recovery)                          | Multi-step workflow; simulate a crash; on restart steps with recorded outputs are skipped. |
| 06 | [`06-saga-compensation`](./06-saga-compensation)                    | A booking saga whose later step fails and triggers compensating steps.                     |
| 07 | [`07-cron-scheduler`](./07-cron-scheduler)                          | A workflow registered with `WithSchedule` fires once per second.                           |
| 08 | [`08-rate-limited-queue`](./08-rate-limited-queue)                  | A queue with `WithRateLimiter` paces a burst of webhook deliveries.                        |
| 09 | [`09-cancel-workflow`](./09-cancel-workflow)                        | Start a long workflow, then `CancelWorkflow` it from outside.                              |
| 10 | [`10-events-monitor`](./10-events-monitor)                          | Workflow publishes progress via `SetEvent`; another goroutine polls `GetEvent`.            |
| 11 | [`11-idempotent-run`](./11-idempotent-run)                          | Calling `RunWorkflow` 5× with the same `WorkflowID` invokes the workflow once.             |
| 12 | [`12-priority-queue`](./12-priority-queue)                          | `WithPriorityEnabled` queue dispatches lowest-priority-number first.                       |
| 13 | [`13-status-polling`](./13-status-polling)                          | A long-running workflow whose status is polled by a watcher goroutine.                     |
| 14 | [`14-admin-http`](./14-admin-http)                                  | Mount `orc.AdminHandler` in `net/http` for live monitoring, cancel, resume, fork & tree.   |
| 15 | [`15-http-context-cancel`](./15-http-context-cancel)                | `WithCallerContext` ties a workflow's lifetime to an HTTP request; cancel = CANCELLED.     |
| 16 | [`16-delayed-run`](./16-delayed-run)                                | `WithWorkflowDelay` schedules a one-shot run N seconds out; the delay survives restarts.   |

## Running

Each example is its own Go package. From the repo root:

```bash
cd examples/02-step-retries
go run .
```

Each example writes its own `<name>.db` SQLite file in the working
directory; delete it (or let the example overwrite it) to start fresh.

## Recommended reading order

If you're new to orc, work through them roughly in order:

1. **01-hello-world** — minimal lifecycle.
2. **02-step-retries** — what `RunAsStep` is for.
3. **05-crash-recovery** — the central durability promise. **This is the
   one to read if you want to understand orc's value proposition.**
4. **04-spawn-child-workflows** — how parents and children compose.
5. **03-fan-in-results** — durable cross-workflow messaging.
6. **06-saga-compensation** — error-handling patterns.
7. **08-rate-limited-queue**, **12-priority-queue** — queue tuning.
8. **07-cron-scheduler**, **09-cancel-workflow**, **10-events-monitor**,
   **11-idempotent-run**, **16-delayed-run** — assorted features.
9. **14-admin-http**, **15-http-context-cancel** — wiring orc into an HTTP
   server (admin API + dashboard, request-scoped workflows).

## What you'll always see

Every example follows the same skeleton:

```go
ctx, _ := orc.NewContext(context.Background(), orc.Config{
    AppName:      "<example>",
    DatabasePath: "<example>.db",
})
defer orc.Shutdown(ctx, 5*time.Second)

orc.RegisterWorkflow[In, Out](ctx, myWorkflow, orc.WithWorkflowName("my_workflow"))
_ = orc.Launch(ctx)

h, _ := orc.RunWorkflow[In, Out](ctx, myWorkflow, in)
out, _ := h.GetResult(orc.WithHandleTimeout(...))
```

Once you internalise that skeleton, every other example is just "what did
the workflow function look like?"

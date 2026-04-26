// Package orc provides lightweight durable workflow orchestration backed by SQLite.
//
// ORC is a Go re-implementation of the core ideas in dbos-transact-golang
// (https://github.com/dbos-inc/dbos-transact-golang) but built on top of
// ella.to/sqlite instead of PostgreSQL.
//
// # Overview
//
// You write ordinary Go functions and register them as durable workflows.
// ORC checkpoints workflow inputs, step outputs, errors and completion to
// SQLite, so that if your process crashes mid-execution it can pick up where
// it left off when it restarts. The same persistence layer powers durable
// queues, durable inter-workflow messaging, durable events and durable sleep.
//
// # Quick start
//
//	ctx, _ := orc.NewContext(context.Background(), orc.Config{
//	    AppName:      "myapp",
//	    DatabasePath: "orc.db",
//	})
//	orc.RegisterWorkflow(ctx, myWorkflow)
//	_ = orc.Launch(ctx)
//	defer orc.Shutdown(ctx, 5*time.Second)
//
//	handle, _ := orc.RunWorkflow(ctx, myWorkflow, "hello")
//	result, _ := handle.GetResult()
//
// # Concepts
//
//   - Workflow: a registered Go function executed durably. Its input/output
//     and final state are persisted in SQLite.
//   - Step: a leaf operation executed inside a workflow with `RunAsStep`.
//     Step outputs are checkpointed and skipped on re-execution.
//   - Queue: a durable, optionally rate-limited & concurrency-limited place
//     to enqueue workflows for background execution.
//   - Notifications: Send / Recv messages between workflows by ID + topic.
//   - Events: SetEvent / GetEvent expose key/value progress data.
//   - Sleep: durable timer that survives restarts.
//   - Schedule: cron-driven workflow invocation.
//
// # Differences from dbos-transact-golang
//
//   - Uses SQLite (via ella.to/sqlite) instead of PostgreSQL. Notifications
//     and queues are polling-driven instead of LISTEN/NOTIFY-driven.
//   - Drops Postgres-specific features (advisory locks, schemas, SKIP LOCKED).
//   - No admin HTTP server, no Conductor cloud client, no streams, no patching.
//   - Single-writer SQLite means concurrent enqueue/dequeue is serialised at
//     the storage layer; throughput is bounded accordingly.
package orc

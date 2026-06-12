// Example 16 — Delayed (scheduled-once) workflows.
//
// Demonstrates:
//   - orc.WithWorkflowDelay: run a workflow N seconds from now
//   - The delay is durable: the row sits in DELAYED state in SQLite, so it
//     survives a restart — if the process dies before the delay elapses,
//     the workflow still runs once a new process Launches and the queue
//     runner promotes it.
//   - Works with or without WithQueue. Without a queue the workflow is
//     routed onto orc's internal queue so the dispatcher can promote it.
//
// Use cases: "send the follow-up email in 24h", "retry this payment in
// 10 minutes", "auto-expire the reservation in 15 minutes".
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/orc"
)

type reminder struct {
	Email   string
	Message string
}

func sendReminder(c *orc.Context, in reminder) (string, error) {
	// The side effect is a step, as always.
	return orc.RunAsStep(c, func(_ context.Context) (string, error) {
		// In real life: send the email.
		return fmt.Sprintf("sent %q to %s", in.Message, in.Email), nil
	}, orc.WithStepName("send_email"))
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "delayed-run",
		DatabasePath: "delayed-run.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[reminder, string](ctx, sendReminder,
		orc.WithWorkflowName("send_reminder"))

	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const delay = 3 * time.Second
	fmt.Printf("scheduling reminder to run in %v...\n", delay)

	h, err := orc.RunWorkflow[reminder, string](ctx, sendReminder,
		reminder{Email: "user@example.com", Message: "your trial expires soon"},
		orc.WithWorkflowID("reminder-1"), // idempotent: re-running this program won't double-schedule
		orc.WithWorkflowDelay(delay),
	)
	if err != nil {
		panic(err)
	}

	// While the delay is pending the row is in DELAYED state. You could
	// kill the process here and restart it — the reminder still fires.
	st, _ := h.GetStatus()
	fmt.Printf("status right after scheduling: %s\n", st.Status) // DELAYED

	start := time.Now()
	out, err := h.GetResult(orc.WithHandleTimeout(30 * time.Second))
	if err != nil {
		panic(err)
	}
	fmt.Printf("after %v: %s\n", time.Since(start).Round(100*time.Millisecond), out)
}

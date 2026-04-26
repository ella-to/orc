// Example 13 — Polling a long-running workflow's status from another goroutine.
//
// Demonstrates:
//   - A long-running workflow that progresses through several steps
//   - A separate goroutine using GetStatus on the workflow handle to
//     observe lifecycle transitions: ENQUEUED -> PENDING -> SUCCESS
//   - GetStatus is read-only and never blocks the workflow
//
// Think of the watcher as an HTTP /workflows/:id handler, a CLI `orc
// status`, or a Prometheus exporter — anything that needs to know "is this
// thing still running?" without touching its execution.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"ella.to/orc"
)

// longJob runs a few durable steps with sleeps in between to give the
// watcher something interesting to observe.
func longJob(c *orc.Context, _ string) (string, error) {
	for i := 1; i <= 5; i++ {
		_, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
			fmt.Printf("    workflow: doing step %d/5\n", i)
			return fmt.Sprintf("step-%d", i), nil
		}, orc.WithStepName(fmt.Sprintf("step_%d", i)))
		if err != nil {
			return "", err
		}
		_, _ = orc.Sleep(c, 400*time.Millisecond)
	}
	return "all-done", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "status-polling",
		DatabasePath: "status-polling.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, longJob,
		orc.WithWorkflowName("long_job"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const wfID = "job-1"
	h, err := orc.RunWorkflow[string, string](ctx, longJob, "",
		orc.WithWorkflowID(wfID))
	if err != nil {
		panic(err)
	}

	// Watcher goroutine: polls the workflow's status (and the count of
	// recorded steps) every 250ms. It uses the workflow handle's GetStatus
	// — but you could equally call orc.RetrieveWorkflow(ctx, wfID) from a
	// completely different process.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		seen := orc.WorkflowStatusType("")
		for range ticker.C {
			st, err := h.GetStatus()
			if err != nil {
				fmt.Printf("watcher: error: %v\n", err)
				return
			}

			// Count how many steps have been checkpointed so far.
			steps, _ := orc.GetWorkflowSteps(ctx, wfID)

			if st.Status != seen {
				fmt.Printf("watcher: status=%-10s  steps_done=%d  attempts=%d\n",
					st.Status, len(steps), st.Attempts)
				seen = st.Status
			} else {
				fmt.Printf("watcher: status=%-10s  steps_done=%d (no change)\n",
					st.Status, len(steps))
			}

			// Stop once the workflow has reached a terminal state.
			if st.Status == orc.WorkflowStatusSuccess ||
				st.Status == orc.WorkflowStatusError ||
				st.Status == orc.WorkflowStatusCancelled {
				return
			}
		}
	}()

	out, err := h.GetResult(orc.WithHandleTimeout(30 * time.Second))
	if err != nil {
		panic(err)
	}
	wg.Wait()

	fmt.Println()
	fmt.Printf("workflow result : %q\n", out)

	final, _ := h.GetStatus()
	fmt.Printf("final status    : %s\n", final.Status)
	fmt.Printf("started at      : %s\n", final.StartedAt.Format(time.RFC3339))
	fmt.Printf("updated at      : %s\n", final.UpdatedAt.Format(time.RFC3339))

	steps, _ := orc.GetWorkflowSteps(ctx, wfID)
	fmt.Printf("\n%d checkpointed steps:\n", len(steps))
	for _, s := range steps {
		fmt.Printf("  step %d : %s\n", s.StepID, s.StepName)
	}
}

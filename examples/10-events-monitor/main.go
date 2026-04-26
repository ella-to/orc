// Example 10 — Workflow events for external observability.
//
// Demonstrates:
//   - SetEvent inside the workflow to publish progress
//   - GetEvent from outside the workflow to observe progress
//   - A separate goroutine (think: HTTP /status endpoint) polling the
//     event without ever touching the workflow itself
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

func longJob(c *orc.Context, _ string) (string, error) {
	stages := []string{"queued", "downloading", "transcoding", "uploading", "done"}
	for _, s := range stages {
		if err := orc.SetEvent(c, "stage", s); err != nil {
			return "", err
		}
		fmt.Printf("workflow: -> %s\n", s)
		_, _ = orc.Sleep(c, 250*time.Millisecond)
	}
	return "ok", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "events",
		DatabasePath: "events.db",
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

	// The "watcher" goroutine — imagine this is an HTTP handler showing the
	// current stage on a dashboard. It just polls GetEvent, never blocks
	// the workflow.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seen := ""
		for {
			stage, err := orc.GetEvent[string](ctx, wfID, "stage", 200*time.Millisecond)
			if err != nil {
				return
			}
			if stage != seen {
				fmt.Printf("watcher : observed stage = %s\n", stage)
				seen = stage
			}
			if stage == "done" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	out, err := h.GetResult(orc.WithHandleTimeout(15 * time.Second))
	if err != nil {
		panic(err)
	}
	wg.Wait()
	fmt.Printf("\nworkflow result = %q\n", out)
}

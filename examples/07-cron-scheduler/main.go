// Example 07 — Cron-driven scheduled workflow.
//
// Demonstrates:
//   - Registering a workflow with WithSchedule (robfig/cron, 6 fields:
//     "seconds minutes hours dom month dow")
//   - The runtime starts firing it once Launch is called
//   - Each fire creates a new durable workflow row (visible via
//     ListWorkflows), with all the usual recovery semantics
//
// The example fires once a second for ~5 seconds and then exits.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"ella.to/orc"
)

var ticks atomic.Int32

// hourlyDigest is the workflow that gets triggered by the schedule. In a
// real app it would do something useful: send digests, roll a partition,
// rotate keys, etc.
func hourlyDigest(c *orc.Context, _ string) (string, error) {
	n := ticks.Add(1)
	now := time.Now().Format("15:04:05.000")
	fmt.Printf("[%s] tick #%d\n", now, n)
	return fmt.Sprintf("tick-%d", n), nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "cron",
		DatabasePath: "cron.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, hourlyDigest,
		orc.WithWorkflowName("hourly_digest"),
		orc.WithSchedule("* * * * * *"), // every second (sec min hr dom mon dow)
	)
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	fmt.Println("scheduler armed; sleeping 5 seconds...")
	time.Sleep(5 * time.Second)

	all, _ := orc.ListWorkflows(ctx, orc.WithListWorkflowName("hourly_digest"))
	fmt.Printf("\n%d scheduled workflow rows recorded:\n", len(all))
	for _, w := range all {
		fmt.Printf("  %s  status=%s  created=%s\n",
			w.ID, w.Status, w.CreatedAt.Format("15:04:05.000"))
	}
}

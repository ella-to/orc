// Example 12 — Priority queue: high-priority items jump the line.
//
// Demonstrates:
//   - WithPriorityEnabled on a queue
//   - WithPriority(n) on each enqueue (lower n = higher priority)
//   - Items dispatch in priority order, not enqueue order
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

type job struct {
	ID    string
	Label string
}

var (
	mu       sync.Mutex
	executed []string
)

func processJob(c *orc.Context, j job) (string, error) {
	mu.Lock()
	executed = append(executed, j.Label)
	mu.Unlock()
	fmt.Printf("ran %-12s\n", j.Label)
	time.Sleep(50 * time.Millisecond)
	return "ok", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "priority",
		DatabasePath: "priority.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[job, string](ctx, processJob,
		orc.WithWorkflowName("process_job"))

	q := orc.NewWorkflowQueue(ctx, "prio",
		orc.WithWorkerConcurrency(1), // serialise to make ordering visible
		orc.WithPriorityEnabled(),
	)

	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	type spec struct {
		label    string
		priority int
	}
	items := []spec{
		{"low-1", 10},
		{"low-2", 10},
		{"high-1", 0},
		{"med-1", 5},
		{"high-2", 0},
		{"low-3", 10},
	}

	handles := make([]orc.WorkflowHandle[string], len(items))
	for i, it := range items {
		h, err := orc.RunWorkflow[job, string](ctx, processJob,
			job{ID: fmt.Sprintf("%d", i), Label: it.label},
			orc.WithQueue(q.Name),
			orc.WithPriority(it.priority),
		)
		if err != nil {
			panic(err)
		}
		handles[i] = h
	}

	for _, h := range handles {
		if _, err := h.GetResult(orc.WithHandleTimeout(15 * time.Second)); err != nil {
			panic(err)
		}
	}

	fmt.Println("\nexecution order (lowest priority number first):")
	for _, l := range executed {
		fmt.Println("  ", l)
	}
}

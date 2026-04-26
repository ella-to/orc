// Example 04 — A workflow that spawns child workflows and awaits them.
//
// Demonstrates:
//   - Calling RunWorkflow from inside another workflow (parent -> child)
//   - Children run on a queue, parent collects results
//   - Each child has its own durable lifecycle and can be inspected,
//     cancelled, or recovered independently
//
// Pattern: think of the parent as a "coordinator" and each child as a
// long-running unit of work.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"time"

	"ella.to/orc"
)

// child does a small piece of work. In real life it could be transcoding a
// video, calling a slow API, training a model, etc.
func child(c *orc.Context, in childInput) (childOutput, error) {
	// One step is enough to demonstrate that children also benefit from
	// step-level idempotency.
	processed, err := orc.RunAsStep(c, func(_ context.Context) (string, error) {
		return fmt.Sprintf("child #%d processed %q", in.Index, in.Payload), nil
	}, orc.WithStepName("process"))
	if err != nil {
		return childOutput{}, err
	}
	return childOutput{Index: in.Index, Result: processed}, nil
}

type childInput struct {
	Index   int
	Payload string
}

type childOutput struct {
	Index  int
	Result string
}

// parent spawns N child workflows on a queue and waits for them to complete.
// Each child has its own row in workflow_status that can be inspected with
// `sqlite3` or via ListWorkflows.
func parent(c *orc.Context, n int) ([]string, error) {
	q := orc.NewWorkflowQueue(c, "children", orc.WithWorkerConcurrency(3))

	handles := make([]orc.WorkflowHandle[childOutput], n)
	for i := 0; i < n; i++ {
		// Each child gets a deterministic ID — re-running the parent with
		// the same input would resume the same children rather than
		// spawning fresh ones.
		childID := fmt.Sprintf("child-%d", i)
		h, err := orc.RunWorkflow[childInput, childOutput](c, child,
			childInput{Index: i, Payload: fmt.Sprintf("payload-%d", i)},
			orc.WithQueue(q.Name),
			orc.WithWorkflowID(childID),
		)
		if err != nil {
			return nil, err
		}
		handles[i] = h
		fmt.Printf("parent spawned %s\n", childID)
	}

	results := make([]string, n)
	for i, h := range handles {
		out, err := h.GetResult(orc.WithHandleTimeout(60 * time.Second))
		if err != nil {
			return nil, fmt.Errorf("child %d failed: %w", i, err)
		}
		results[i] = out.Result
	}
	return results, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "spawn-children",
		DatabasePath: "spawn-children.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[childInput, childOutput](ctx, child,
		orc.WithWorkflowName("child"))
	orc.RegisterWorkflow[int, []string](ctx, parent,
		orc.WithWorkflowName("parent"))

	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	h, err := orc.RunWorkflow[int, []string](ctx, parent, 5,
		orc.WithWorkflowID("parent-1"))
	if err != nil {
		panic(err)
	}
	results, err := h.GetResult(orc.WithHandleTimeout(60 * time.Second))
	if err != nil {
		panic(err)
	}
	fmt.Println()
	for _, r := range results {
		fmt.Println(" >", r)
	}

	// You can also inspect the children programmatically:
	all, _ := orc.ListWorkflows(ctx, orc.WithListWorkflowName("child"))
	fmt.Printf("\n%d child workflow rows in the DB:\n", len(all))
	for _, w := range all {
		fmt.Printf("  %s  status=%s\n", w.ID, w.Status)
	}
}

// Example 11 — Idempotent runs by WorkflowID.
//
// Demonstrates:
//   - Calling RunWorkflow N times with the same WorkflowID + same input
//   - The workflow function is invoked exactly ONCE; subsequent calls
//     return a handle to the existing run
//   - Different input under the same ID is rejected with ErrConflictingInput
//
// Useful for: HTTP retries, at-least-once message brokers, anywhere
// upstream might call you twice.
//
// Run:  go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"ella.to/orc"
)

var calls atomic.Int32

func process(c *orc.Context, n int) (int, error) {
	calls.Add(1)
	return n * n, nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "idempotent",
		DatabasePath: "idempotent.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[int, int](ctx, process, orc.WithWorkflowName("square"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const id = "square-of-7"
	for i := 1; i <= 5; i++ {
		h, err := orc.RunWorkflow[int, int](ctx, process, 7, orc.WithWorkflowID(id))
		if err != nil {
			panic(err)
		}
		got, err := h.GetResult(orc.WithHandleTimeout(2 * time.Second))
		if err != nil {
			panic(err)
		}
		fmt.Printf("call #%d => %d  (workflow invocations so far: %d)\n",
			i, got, calls.Load())
	}

	// Same ID, different input should error.
	_, err = orc.RunWorkflow[int, int](ctx, process, 8, orc.WithWorkflowID(id))
	if err != nil {
		fmt.Println()
		fmt.Printf("conflicting input rejected (good): %v\n", err)
		if errors.Is(err, orc.ErrConflictingInputErr) {
			fmt.Println("  (matched orc.ErrConflictingInputErr)")
		}
	}

	fmt.Printf("\ntotal real workflow invocations: %d  (expected 1)\n", calls.Load())
}

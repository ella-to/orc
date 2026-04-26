// Example 09 — Cancel an in-flight workflow.
//
// Demonstrates:
//   - A long-running workflow that uses durable Sleep
//   - CancelWorkflow from outside the workflow
//   - The handle's GetResult returns a cancellation error
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

var finishedNormally atomic.Bool

func sleeper(c *orc.Context, _ string) (string, error) {
	fmt.Println("workflow: sleeping for up to 30s ...")
	if _, err := orc.Sleep(c, 30*time.Second); err != nil {
		fmt.Printf("workflow: woke up early via context cancel: %v\n", err)
		return "", err
	}
	finishedNormally.Store(true)
	return "done", nil
}

func main() {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "cancel",
		DatabasePath: "cancel.db",
	})
	if err != nil {
		panic(err)
	}
	defer orc.Shutdown(ctx, 5*time.Second)

	orc.RegisterWorkflow[string, string](ctx, sleeper,
		orc.WithWorkflowName("sleeper"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}

	const wfID = "sleeper-1"
	h, err := orc.RunWorkflow[string, string](ctx, sleeper, "",
		orc.WithWorkflowID(wfID))
	if err != nil {
		panic(err)
	}

	// Let it start, then cancel.
	time.Sleep(200 * time.Millisecond)
	fmt.Println("main:     issuing CancelWorkflow")
	if err := orc.CancelWorkflow(ctx, wfID); err != nil {
		panic(err)
	}

	_, err = h.GetResult(orc.WithHandleTimeout(5 * time.Second))
	fmt.Println()
	if err != nil {
		fmt.Printf("main:     handle returned error (expected): %v\n", err)
	}
	fmt.Printf("main:     workflow finished normally? %v (should be false)\n",
		finishedNormally.Load())

	st, _ := h.GetStatus()
	fmt.Printf("main:     final DB status = %s\n", st.Status)
}

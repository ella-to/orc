// Example 05 — Crash recovery: a workflow runs many steps. After a
// simulated crash, the recovered run does NOT re-execute steps whose
// outputs are already in the database — it remembers their values.
//
// This is the central durability promise of orc: side-effecting steps
// happen at most once even across process restarts.
//
// What this program does:
//
//  1. Create a file-backed orc Context.
//  2. Register a workflow that runs 5 steps. Each step bumps an in-memory
//     counter and returns a value.
//  3. Start the workflow but Shutdown the context after a short time, so
//     only some of the steps complete.
//  4. Reopen the SAME database in a new Context. Recovery runs.
//  5. The workflow finishes. We print, for each step, how many times it
//     actually executed.
//
// Expected output: each step that ran in the first session executes
// exactly once total; steps that didn't get a chance the first time run
// once in the recovered session. Either way, the total per-step is 1.
//
// Run:  go run .
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"ella.to/orc"
)

const dbPath = "crash-recovery.db"

// Counters are intentionally process-global so we can observe the call
// counts across the simulated restart.
var stepCalls [5]atomic.Int32

func slowStep(name string, idx int, work time.Duration, value string) func(_ context.Context) (string, error) {
	return func(_ context.Context) (string, error) {
		stepCalls[idx].Add(1)
		fmt.Printf("    >> executing step %s (call #%d)\n", name, stepCalls[idx].Load())
		time.Sleep(work)
		return value, nil
	}
}

func longRunningPipeline(c *orc.Context, _ string) (string, error) {
	fmt.Println("  workflow start")

	a, err := orc.RunAsStep(c, slowStep("A", 0, 80*time.Millisecond, "alpha"),
		orc.WithStepName("step_a"))
	if err != nil {
		return "", err
	}
	b, err := orc.RunAsStep(c, slowStep("B", 1, 80*time.Millisecond, "bravo"),
		orc.WithStepName("step_b"))
	if err != nil {
		return "", err
	}
	cc, err := orc.RunAsStep(c, slowStep("C", 2, 80*time.Millisecond, "charlie"),
		orc.WithStepName("step_c"))
	if err != nil {
		return "", err
	}
	d, err := orc.RunAsStep(c, slowStep("D", 3, 80*time.Millisecond, "delta"),
		orc.WithStepName("step_d"))
	if err != nil {
		return "", err
	}
	e, err := orc.RunAsStep(c, slowStep("E", 4, 80*time.Millisecond, "echo"),
		orc.WithStepName("step_e"))
	if err != nil {
		return "", err
	}

	final := fmt.Sprintf("%s+%s+%s+%s+%s", a, b, cc, d, e)
	fmt.Printf("  workflow end (final = %s)\n", final)
	return final, nil
}

func newCtx() *orc.Context {
	ctx, err := orc.NewContext(context.Background(), orc.Config{
		AppName:      "crash-recovery",
		DatabasePath: dbPath,
		ExecutorID:   "demo-executor", // SAME id across runs => recovery applies
	})
	if err != nil {
		panic(err)
	}
	orc.RegisterWorkflow[string, string](ctx, longRunningPipeline,
		orc.WithWorkflowName("pipeline"))
	if err := orc.Launch(ctx); err != nil {
		panic(err)
	}
	return ctx
}

func main() {
	// Fresh start: remove any DB from a previous run.
	_ = os.Remove(dbPath)

	const wfID = "pipeline-run-1"

	// ---- session 1: start the workflow, then "crash" mid-execution ----
	fmt.Println("=== session 1: start + crash ===")
	ctx1 := newCtx()
	if _, err := orc.RunWorkflow[string, string](ctx1, longRunningPipeline, "go!",
		orc.WithWorkflowID(wfID)); err != nil {
		panic(err)
	}

	// Let some (but not all) steps complete: at 80ms each, ~250ms gives
	// us roughly steps A, B, C while D and E remain to be run.
	time.Sleep(250 * time.Millisecond)
	fmt.Println("  >> SIMULATED CRASH (Shutdown with 0s grace)")
	orc.Shutdown(ctx1, 0)

	// Snapshot the per-step counters before recovery.
	var beforeCalls [5]int32
	for i := range stepCalls {
		beforeCalls[i] = stepCalls[i].Load()
	}
	fmt.Printf("  per-step calls so far: %v\n\n", beforeCalls)

	// ---- session 2: reopen the same DB; recovery resumes the workflow ----
	fmt.Println("=== session 2: restart (recovery should resume) ===")
	ctx2 := newCtx()
	defer orc.Shutdown(ctx2, 5*time.Second)

	h, err := orc.RetrieveWorkflow[string](ctx2, wfID)
	if err != nil {
		panic(err)
	}
	final, err := h.GetResult(orc.WithHandleTimeout(15 * time.Second))
	if err != nil {
		panic(err)
	}

	fmt.Println()
	fmt.Printf("recovered final = %q\n\n", final)
	fmt.Println("per-step total call counts (each should be exactly 1):")
	for i, name := range []string{"A", "B", "C", "D", "E"} {
		fmt.Printf("  step %s : %d call(s)\n", name, stepCalls[i].Load())
	}

	// Demonstrate that we can also enumerate the recorded steps from the DB.
	steps, _ := orc.GetWorkflowSteps(ctx2, wfID)
	fmt.Printf("\n%d checkpointed steps in workflow_status / operation_outputs:\n", len(steps))
	for _, s := range steps {
		fmt.Printf("  step %d : %s\n", s.StepID, s.StepName)
	}
}

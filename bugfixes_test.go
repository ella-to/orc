package orc

import (
	"strings"
	"testing"
	"time"
)

// TestListWorkflows_ExactExecutorMatch guards against the recovery-scoping
// bug where executor "local" LIKE-matched workflows owned by "local2".
// Internal queries must use exact matching; only the admin API opts in to
// fuzzy (substring) search.
func TestListWorkflows_ExactExecutorMatch(t *testing.T) {
	c := newTestContext(t)
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	mk := func(id, executor string) {
		_, err := c.systemDB.insertWorkflow(c.ctx, insertWorkflowInput{Status: WorkflowStatus{
			ID:         id,
			Name:       "wf",
			Status:     WorkflowStatusPending,
			ExecutorID: executor,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	mk("a", "local")
	mk("b", "local2")
	mk("c", "my-local-3")

	exact, err := c.systemDB.listWorkflows(c.ctx, listWorkflowsInput{ExecutorID: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if len(exact) != 1 || exact[0].ID != "a" {
		t.Fatalf("exact executor filter matched %d rows, want only %q", len(exact), "a")
	}

	fuzzy, err := c.systemDB.listWorkflows(c.ctx, listWorkflowsInput{ExecutorID: "local", Fuzzy: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(fuzzy) != 3 {
		t.Fatalf("fuzzy executor filter matched %d rows, want 3", len(fuzzy))
	}
}

// TestChildWorkflow_ReplayDoesNotDuplicate verifies that spawning a child
// workflow from inside a parent is checkpointed: re-executing the parent
// (as crash recovery would) must re-attach to the same child rather than
// spawning a second one.
func TestChildWorkflow_ReplayDoesNotDuplicate(t *testing.T) {
	c := newTestContext(t)

	child := func(c *Context, in string) (string, error) {
		return "child:" + in, nil
	}
	parent := func(c *Context, in string) (string, error) {
		h, err := RunWorkflow[string, string](c, child, in)
		if err != nil {
			return "", err
		}
		return h.GetResult(WithHandleTimeout(5 * time.Second))
	}
	RegisterWorkflow[string, string](c, child, WithWorkflowName("dedup-child"))
	RegisterWorkflow[string, string](c, parent, WithWorkflowName("dedup-parent"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	h, err := RunWorkflow[string, string](c, parent, "x", WithWorkflowID("dedup-parent-1"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if out != "child:x" {
		t.Fatalf("unexpected parent output %q", out)
	}

	// The spawn must be recorded as a step pointing at the child.
	steps, err := GetWorkflowSteps(c, "dedup-parent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].ChildWorkflowID == "" {
		t.Fatalf("expected one child-spawn step, got %+v", steps)
	}
	childID := steps[0].ChildWorkflowID
	if !strings.HasPrefix(childID, "dedup-parent-1-") {
		t.Fatalf("child ID %q not derived from parent", childID)
	}

	// Simulate a crash replay: re-run the parent from its DB row. The replay
	// must short-circuit on the recorded step and NOT create a new child.
	st, err := c.systemDB.getWorkflowStatus(c.ctx, "dedup-parent-1", true)
	if err != nil {
		t.Fatal(err)
	}
	rh, err := runRegisteredWorkflowFromDB(c, *st)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rh.GetResult(WithHandleTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	children, err := ListWorkflows(c, WithListWorkflowName("dedup-child"))
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 {
		t.Fatalf("replay spawned %d children, want exactly 1", len(children))
	}
	if children[0].ID != childID {
		t.Fatalf("replayed child %q != original %q", children[0].ID, childID)
	}
}

// TestWorkflowDelay_HonoredWithoutQueue: WithWorkflowDelay used to be
// silently ignored when the workflow wasn't enqueued on a named queue.
// It must now be durably delayed via the internal queue.
func TestWorkflowDelay_HonoredWithoutQueue(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, in string) (string, error) {
		return "done:" + in, nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("delayed-wf"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	const delay = 200 * time.Millisecond
	start := time.Now()
	h, err := RunWorkflow[string, string](c, wf, "in", WithWorkflowDelay(delay))
	if err != nil {
		t.Fatal(err)
	}

	st, err := h.GetStatus()
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WorkflowStatusDelayed {
		t.Fatalf("expected DELAYED before the delay elapses, got %s", st.Status)
	}

	out, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if out != "done:in" {
		t.Fatalf("unexpected output %q", out)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Fatalf("workflow completed after %v, before the %v delay elapsed", elapsed, delay)
	}
}

// TestRunWorkflow_IdempotentAttachComparesRawInput: re-running with the same
// ID and input attaches; a different input is rejected.
func TestRunWorkflow_IdempotentAttachInputCheck(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, in string) (string, error) {
		_, _ = Sleep(c, 50*time.Millisecond)
		return in, nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("idem"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	h1, err := RunWorkflow[string, string](c, wf, "same", WithWorkflowID("idem-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunWorkflow[string, string](c, wf, "same", WithWorkflowID("idem-1")); err != nil {
		t.Fatalf("same input should attach, got %v", err)
	}
	if _, err := RunWorkflow[string, string](c, wf, "different", WithWorkflowID("idem-1")); !IsCode(err, ErrConflictingInput) {
		t.Fatalf("different input should conflict, got %v", err)
	}
	if _, err := h1.GetResult(WithHandleTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

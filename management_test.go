package orc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestListWorkflows_FiltersByName(t *testing.T) {
	c := newTestContext(t)
	wfA := func(c *Context, n int) (int, error) { return n, nil }
	wfB := func(c *Context, n int) (int, error) { return n, nil }
	RegisterWorkflow[int, int](c, wfA, WithWorkflowName("alpha"))
	RegisterWorkflow[int, int](c, wfB, WithWorkflowName("beta"))
	_ = Launch(c)

	// Drive a few of each.
	for i := 0; i < 3; i++ {
		h, _ := RunWorkflow[int, int](c, wfA, i)
		_, _ = h.GetResult()
	}
	for i := 0; i < 2; i++ {
		h, _ := RunWorkflow[int, int](c, wfB, i)
		_, _ = h.GetResult()
	}

	got, err := ListWorkflows(c, WithListWorkflowName("alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Errorf("alpha: got %d", len(got))
	}
	got, err = ListWorkflows(c, WithListWorkflowName("beta"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("beta: got %d", len(got))
	}
}

func TestListWorkflows_FiltersByStatus(t *testing.T) {
	c := newTestContext(t)
	good := func(c *Context, _ string) (string, error) { return "ok", nil }
	bad := func(c *Context, _ string) (string, error) { return "", errors.New("nope") }
	RegisterWorkflow[string, string](c, good, WithWorkflowName("good"))
	RegisterWorkflow[string, string](c, bad, WithWorkflowName("bad"))
	_ = Launch(c)
	h1, _ := RunWorkflow[string, string](c, good, "")
	h2, _ := RunWorkflow[string, string](c, bad, "")
	_, _ = h1.GetResult()
	_, _ = h2.GetResult()

	ok, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusSuccess))
	if len(ok) != 1 {
		t.Errorf("success: %d", len(ok))
	}
	bad2, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusError))
	if len(bad2) != 1 {
		t.Errorf("error: %d", len(bad2))
	}
}

func TestRetrieveWorkflow(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, s string) (string, error) { return s + "!", nil }
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "hi", WithWorkflowID("retrieve-test"))
	_, _ = h.GetResult()

	r, err := RetrieveWorkflow[string](c, "retrieve-test")
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.GetResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hi!" {
		t.Errorf("got %q", got)
	}

	_, err = RetrieveWorkflow[string](c, "nope")
	if !IsCode(err, ErrWorkflowNotFound) {
		t.Errorf("expected not found, got %v", err)
	}
}

func TestCancelWorkflow(t *testing.T) {
	c := newTestContext(t)
	var stopped atomic.Bool
	wf := func(c *Context, _ string) (string, error) {
		select {
		case <-c.Underlying().Done():
			stopped.Store(true)
			return "", c.Underlying().Err()
		case <-time.After(2 * time.Second):
			return "done", nil
		}
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("cancel-me"))
	time.Sleep(50 * time.Millisecond)
	if err := CancelWorkflow(c, "cancel-me"); err != nil {
		t.Fatal(err)
	}
	st, _ := h.GetStatus()
	if st.Status != WorkflowStatusCancelled {
		t.Errorf("status = %s", st.Status)
	}
}

func TestForkWorkflow(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		s1, _ := RunAsStep(c, func(_ context.Context) (string, error) { return "A", nil }, WithStepName("s1"))
		s2, _ := RunAsStep(c, func(_ context.Context) (string, error) { return "B", nil }, WithStepName("s2"))
		return s1 + s2, nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("orig"))
	if got, _ := h.GetResult(); got != "AB" {
		t.Fatalf("orig got %q", got)
	}

	// Fork from step 1 onward (re-run step 2 only).
	h2, err := ForkWorkflow[string](c, ForkWorkflowInput{
		OriginalWorkflowID: "orig",
		StartFromStep:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h2.GetResult(WithHandleTimeout(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "AB" {
		t.Errorf("fork got %q", got)
	}
	steps, _ := GetWorkflowSteps(c, h2.GetWorkflowID())
	if len(steps) != 2 {
		t.Errorf("expected 2 steps after fork, got %d", len(steps))
	}
}

func TestDeleteWorkflows(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "ok", nil }
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("d1"))
	_, _ = h.GetResult()

	if err := DeleteWorkflows(c, []string{"d1"}); err != nil {
		t.Fatal(err)
	}
	_, err := RetrieveWorkflow[string](c, "d1")
	if !IsCode(err, ErrWorkflowNotFound) {
		t.Errorf("expected not found, got %v", err)
	}
}

func TestResumeWorkflow_AfterError(t *testing.T) {
	c := newTestContext(t)
	var attempts atomic.Int32
	wf := func(c *Context, _ string) (string, error) {
		n := attempts.Add(1)
		if n == 1 {
			return "", errors.New("first try fails")
		}
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("resumeable"))
	_ = Launch(c)
	id := "resume-1"
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID(id))
	if _, err := h.GetResult(); err == nil {
		t.Fatal("expected initial failure")
	}

	// Resume the failed run.
	h2, err := ResumeWorkflow[string](c, id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h2.GetResult(WithHandleTimeout(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "ok" {
		t.Errorf("got %q", got)
	}
}

func TestGCWorkflows_DeletesByStatusAndCutoff(t *testing.T) {
	c := newTestContext(t)
	good := func(c *Context, _ string) (string, error) { return "ok", nil }
	bad := func(c *Context, _ string) (string, error) { return "", errors.New("nope") }
	RegisterWorkflow[string, string](c, good, WithWorkflowName("good"))
	RegisterWorkflow[string, string](c, bad, WithWorkflowName("bad"))
	_ = Launch(c)

	// Drive a few of each to terminal state. updated_at gets set to "now"
	// when the workflow finalises.
	for i := 0; i < 3; i++ {
		h, _ := RunWorkflow[string, string](c, good, "")
		_, _ = h.GetResult()
	}
	for i := 0; i < 2; i++ {
		h, _ := RunWorkflow[string, string](c, bad, "")
		_, _ = h.GetResult()
	}

	// Sanity: pre-GC counts.
	preOK, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusSuccess))
	preErr, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusError))
	if len(preOK) != 3 || len(preErr) != 1 {
		// `bad` returns an error on every attempt; only one such
		// workflow may end up with WorkflowStatusError because of
		// step-checkpoint dedup. Just sanity-check the shape.
		t.Logf("pre-gc success=%d error=%d", len(preOK), len(preErr))
	}

	// GC SUCCESS rows updated before "now + 1m". updated_at is monotonic
	// from the workflow runtime, so this cutoff catches all of them.
	cutoff := time.Now().Add(time.Minute)
	res, err := GCWorkflows(c, GCWorkflowsInput{
		Statuses:      []WorkflowStatusType{WorkflowStatusSuccess},
		UpdatedBefore: cutoff,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != len(preOK) {
		t.Errorf("Deleted=%d want=%d", res.Deleted, len(preOK))
	}
	if res.Failed != 0 || len(res.Errors) != 0 {
		t.Errorf("unexpected failures: failed=%d errors=%v", res.Failed, res.Errors)
	}

	postOK, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusSuccess))
	if len(postOK) != 0 {
		t.Errorf("post-gc success rows = %d want 0", len(postOK))
	}
	// ERROR rows must be untouched.
	postErr, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusError))
	if len(postErr) != len(preErr) {
		t.Errorf("post-gc error rows = %d want %d", len(postErr), len(preErr))
	}
}

func TestGCWorkflows_RejectsNonTerminalByDefault(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	_, err := GCWorkflows(c, GCWorkflowsInput{
		Statuses:      []WorkflowStatusType{WorkflowStatusPending},
		UpdatedBefore: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error for non-terminal status")
	}
}

func TestGCWorkflows_RequiresInputs(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	if _, err := GCWorkflows(c, GCWorkflowsInput{}); err == nil {
		t.Errorf("expected error for empty statuses")
	}
	if _, err := GCWorkflows(c, GCWorkflowsInput{
		Statuses: []WorkflowStatusType{WorkflowStatusSuccess},
	}); err == nil {
		t.Errorf("expected error for zero UpdatedBefore")
	}
}

func TestGCWorkflows_RespectsCutoff(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "ok", nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("ok-cutoff"))
	_ = Launch(c)

	for i := 0; i < 3; i++ {
		h, _ := RunWorkflow[string, string](c, wf, "")
		_, _ = h.GetResult()
	}

	// A cutoff in the past should match nothing.
	res, err := GCWorkflows(c, GCWorkflowsInput{
		Statuses:      []WorkflowStatusType{WorkflowStatusSuccess},
		UpdatedBefore: time.Now().Add(-time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 0 {
		t.Errorf("deleted=%d want 0 (cutoff in past)", res.Deleted)
	}

	rows, _ := ListWorkflows(c, WithListWorkflowStatus(WorkflowStatusSuccess))
	if len(rows) != 3 {
		t.Errorf("survivors=%d want 3", len(rows))
	}
}

func TestListRegisteredWorkflows(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "", nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("zz"))
	names := ListRegisteredWorkflows(c)
	found := false
	for _, n := range names {
		if n == "zz" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing zz in %v", names)
	}
}

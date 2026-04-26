package orc

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ----- workflow declarations used by the tests below -----

func wfHelloString(c *Context, in string) (string, error) {
	return "hello, " + in, nil
}

func wfFail(c *Context, msg string) (string, error) {
	return "", errors.New(msg)
}

type sumIn struct {
	A, B int
}

func wfSum(c *Context, in sumIn) (int, error) {
	return in.A + in.B, nil
}

// ----- tests -----

func TestRunWorkflow_Synchronous(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfHelloString)
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	h, err := RunWorkflow[string, string](c, wfHelloString, "world")
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello, world" {
		t.Errorf("got %q", got)
	}

	// Status should be SUCCESS.
	st, err := h.GetStatus()
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != WorkflowStatusSuccess {
		t.Errorf("expected SUCCESS got %s", st.Status)
	}
}

func TestRunWorkflow_StructInputOutput(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[sumIn, int](c, wfSum)
	_ = Launch(c)
	h, err := RunWorkflow[sumIn, int](c, wfSum, sumIn{A: 7, B: 35})
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("got %d", got)
	}
}

func TestRunWorkflow_Failure(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfFail)
	_ = Launch(c)
	h, err := RunWorkflow[string, string](c, wfFail, "boom")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.GetResult()
	if err == nil || err.Error() != "boom" {
		t.Errorf("expected boom error, got %v", err)
	}
	st, _ := h.GetStatus()
	if st.Status != WorkflowStatusError {
		t.Errorf("expected ERROR got %s", st.Status)
	}
}

func TestRunWorkflow_NotRegistered(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	_, err := RunWorkflow[string, string](c, wfHelloString, "x")
	if !IsCode(err, ErrWorkflowNotRegistered) {
		t.Fatalf("expected ErrWorkflowNotRegistered, got %v", err)
	}
}

func TestRunWorkflow_RequiresLaunch(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfHelloString)
	_, err := RunWorkflow[string, string](c, wfHelloString, "x")
	if !IsCode(err, ErrInitialization) {
		t.Fatalf("expected ErrInitialization, got %v", err)
	}
}

// Step idempotency: when WithWorkflowID is reused, the workflow runs once
// and subsequent runs return the same result.
func TestRunWorkflow_Idempotent_WithWorkflowID(t *testing.T) {
	c := newTestContext(t)

	var calls atomic.Int32
	wf := func(c *Context, n int) (int, error) {
		calls.Add(1)
		return n * 2, nil
	}
	RegisterWorkflow[int, int](c, wf)
	_ = Launch(c)

	id := "stable-id-1"
	h1, err := RunWorkflow[int, int](c, wf, 5, WithWorkflowID(id))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := h1.GetResult(); got != 10 {
		t.Errorf("h1 got %d", got)
	}

	// Second call with same ID + same input must NOT re-run the function.
	h2, err := RunWorkflow[int, int](c, wf, 5, WithWorkflowID(id))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := h2.GetResult(); got != 10 {
		t.Errorf("h2 got %d", got)
	}
	if c := calls.Load(); c != 1 {
		t.Errorf("workflow called %d times, expected 1", c)
	}
}

// Conflicting input on existing ID must error.
func TestRunWorkflow_ConflictingInput(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, n int) (int, error) { return n, nil }
	RegisterWorkflow[int, int](c, wf)
	_ = Launch(c)

	id := "stable-id-2"
	_, err := RunWorkflow[int, int](c, wf, 1, WithWorkflowID(id))
	if err != nil {
		t.Fatal(err)
	}
	_, err = RunWorkflow[int, int](c, wf, 999, WithWorkflowID(id))
	if !IsCode(err, ErrConflictingInput) {
		t.Errorf("expected ErrConflictingInput, got %v", err)
	}
}

// Steps are checkpointed so repeated invocation in the same workflow body
// only runs the underlying function once.
func TestRunAsStep_CheckpointsOutput(t *testing.T) {
	c := newTestContext(t)
	var stepCalls atomic.Int32
	step := func(_ context.Context) (string, error) {
		stepCalls.Add(1)
		return "ok", nil
	}
	wf := func(c *Context, _ string) (string, error) {
		out, _ := RunAsStep(c, step)
		return out, nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)

	h, _ := RunWorkflow[string, string](c, wf, "")
	got, _ := h.GetResult()
	if got != "ok" {
		t.Errorf("got %q", got)
	}
	if stepCalls.Load() != 1 {
		t.Errorf("step called %d times", stepCalls.Load())
	}

	// Verify a single step row was written.
	steps, err := GetWorkflowSteps(c, h.GetWorkflowID())
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Errorf("expected 1 step, got %d", len(steps))
	}
}

func TestRunAsStep_RetriesUntilSuccess(t *testing.T) {
	c := newTestContext(t)
	var calls atomic.Int32
	step := func(_ context.Context) (string, error) {
		n := calls.Add(1)
		if n < 3 {
			return "", fmt.Errorf("attempt %d", n)
		}
		return "ok", nil
	}
	wf := func(c *Context, _ string) (string, error) {
		return RunAsStep(c, step,
			WithStepName("flaky"),
			WithStepMaxRetries(5),
			WithStepRetryBackoff(time.Millisecond, 5*time.Millisecond, 2),
		)
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "")
	got, err := h.GetResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != "ok" {
		t.Errorf("got %q", got)
	}
	if calls.Load() != 3 {
		t.Errorf("calls=%d", calls.Load())
	}
}

func TestRunAsStep_OutsideWorkflow_RunsDirectly(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	got, err := RunAsStep(c, func(_ context.Context) (int, error) { return 7, nil })
	if err != nil || got != 7 {
		t.Errorf("got %d err %v", got, err)
	}
}

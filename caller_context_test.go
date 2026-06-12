package orc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// wfSleeper sleeps with the durable Sleep primitive so it can be woken up
// early by cancelling the workflow's context.
func wfSleeper(c *Context, d string) (string, error) {
	if _, err := Sleep(c, 5*time.Second); err != nil {
		return "", err
	}
	return "done:" + d, nil
}

// TestWithCallerContext_Cancel verifies that cancelling the caller-supplied
// context propagates into the workflow, returns context.Canceled from
// GetResult, and marks the row as CANCELLED.
func TestWithCallerContext_Cancel(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfSleeper, WithWorkflowName("sleeper_caller_cancel"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	h, err := RunWorkflow[string, string](c, wfSleeper, "x",
		WithCallerContext(callerCtx))
	if err != nil {
		t.Fatal(err)
	}

	// Let the workflow start and reach Sleep before we cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	_, err = h.GetResult(WithHandleTimeout(2 * time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from GetResult, got %v", err)
	}

	// The DB row should reflect a CANCELLED status, not ERROR.
	assertEventually(t, time.Second, func() bool {
		st, err := h.GetStatus()
		if err != nil {
			return false
		}
		return st.Status == WorkflowStatusCancelled
	}, "expected WorkflowStatusCancelled")
}

// TestWithCallerContext_Deadline verifies that a caller context with a
// deadline propagates context.DeadlineExceeded into the workflow.
func TestWithCallerContext_Deadline(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfSleeper, WithWorkflowName("sleeper_caller_deadline"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	h, err := RunWorkflow[string, string](c, wfSleeper, "x",
		WithCallerContext(callerCtx))
	if err != nil {
		t.Fatal(err)
	}

	_, err = h.GetResult(WithHandleTimeout(2 * time.Second))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded from GetResult, got %v", err)
	}

	assertEventually(t, time.Second, func() bool {
		st, err := h.GetStatus()
		if err != nil {
			return false
		}
		return st.Status == WorkflowStatusCancelled
	}, "expected WorkflowStatusCancelled")
}

// TestWithCallerContext_NormalCompletion verifies that the watcher
// goroutine is reaped and no leak occurs when the workflow completes
// before the caller context cancels.
func TestWithCallerContext_NormalCompletion(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfHelloString, WithWorkflowName("hello_caller_normal"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	// We deliberately never cancel; the watcher should still exit via
	// wfCtx.Done() when the workflow finishes naturally.
	defer cancel()

	before := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		h, err := RunWorkflow[string, string](c, wfHelloString, "world",
			WithCallerContext(callerCtx))
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.GetResult(WithHandleTimeout(time.Second))
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if got != "hello, world" {
			t.Fatalf("iter %d: got %q", i, got)
		}
	}

	// Give watcher goroutines a chance to exit.
	assertEventually(t, time.Second, func() bool {
		return runtime.NumGoroutine() <= before+5
	}, "watcher goroutines were not reaped")
}

// TestWithCallerContext_NotSet verifies the default behaviour is unchanged:
// cancelling some unrelated context does NOT terminate the workflow.
func TestWithCallerContext_NotSet(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfHelloString, WithWorkflowName("hello_no_caller"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	unrelated, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled — must not affect the workflow

	_ = unrelated

	h, err := RunWorkflow[string, string](c, wfHelloString, "world")
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult(WithHandleTimeout(time.Second))
	if err != nil {
		t.Fatalf("workflow should not be affected by unrelated cancelled context: %v", err)
	}
	if got != "hello, world" {
		t.Fatalf("got %q", got)
	}
}

// TestWithCallerContext_AlreadyCancelled verifies that passing an
// already-cancelled context immediately stops the workflow.
func TestWithCallerContext_AlreadyCancelled(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfSleeper, WithWorkflowName("sleeper_already_cancelled"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	cancel()

	h, err := RunWorkflow[string, string](c, wfSleeper, "x",
		WithCallerContext(callerCtx))
	if err != nil {
		t.Fatal(err)
	}

	_, err = h.GetResult(WithHandleTimeout(2 * time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestWithCallerContext_StepReceivesCancel verifies that the workflow's
// underlying context (the one passed to a step's StepFunc) is cancelled.
func TestWithCallerContext_StepReceivesCancel(t *testing.T) {
	c := newTestContext(t)
	var stepSawCancel atomic.Bool

	wf := func(c *Context, _ string) (string, error) {
		return RunAsStep(c, func(stepCtx context.Context) (string, error) {
			select {
			case <-stepCtx.Done():
				stepSawCancel.Store(true)
				return "", stepCtx.Err()
			case <-time.After(5 * time.Second):
				return "never", nil
			}
		}, WithStepName("waiter"))
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("step_caller_ctx"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	callerCtx, cancel := context.WithCancel(context.Background())
	h, err := RunWorkflow[string, string](c, wf, "x",
		WithCallerContext(callerCtx))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(20 * time.Millisecond)
	cancel()

	_, err = h.GetResult(WithHandleTimeout(2 * time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !stepSawCancel.Load() {
		t.Fatalf("step never saw its context cancel")
	}
}

// TestWithCallerContext_HTTPHandler is an end-to-end test that mirrors the
// motivating use case: a workflow runs inside an http.Handler, the client
// aborts, and the workflow row ends up CANCELLED.
func TestWithCallerContext_HTTPHandler(t *testing.T) {
	c := newTestContext(t)
	RegisterWorkflow[string, string](c, wfSleeper, WithWorkflowName("sleeper_http"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	var handlerErr atomic.Value  // error
	var handlerWFID atomic.Value // string
	handlerDone := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		h, err := RunWorkflow[string, string](c, wfSleeper, "http",
			WithCallerContext(r.Context()))
		if err != nil {
			http.Error(w, err.Error(), 500)
			handlerErr.Store(err)
			return
		}
		handlerWFID.Store(h.GetWorkflowID())
		_, err = h.GetResult(WithHandleTimeout(2 * time.Second))
		if err != nil {
			handlerErr.Store(err)
			http.Error(w, err.Error(), 499)
			return
		}
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	reqCtx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(reqCtx, http.MethodGet, srv.URL+"/", nil)

	doneCh := make(chan error, 1)
	go func() {
		_, err := http.DefaultClient.Do(req)
		doneCh <- err
	}()

	// Give the handler a chance to start the workflow, then abort.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("client request did not return")
	}

	// Handler goroutine should observe context.Canceled too.
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not finish")
	}

	got, _ := handlerErr.Load().(error)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("handler expected context.Canceled, got %v", got)
	}

	wfID, _ := handlerWFID.Load().(string)
	if wfID == "" {
		t.Fatal("handler never recorded workflow id")
	}

	assertEventually(t, time.Second, func() bool {
		st, err := c.systemDB.getWorkflowStatus(c.ctx, wfID, false)
		if err != nil || st == nil {
			return false
		}
		return st.Status == WorkflowStatusCancelled
	}, "workflow row should be CANCELLED after request abort")
}

package orc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestCancel_ProactivelyInterruptsRunningStep verifies that CancelWorkflow
// pushes the cancel into the worker goroutine: a step that honors
// ctx.Done() should return well before its scheduled wakeup.
func TestCancel_ProactivelyInterruptsRunningStep(t *testing.T) {
	c := newTestContext(t)

	type empty struct{}
	wf := func(c *Context, _ empty) (string, error) {
		_, err := RunAsStep(c, func(stepCtx context.Context) (string, error) {
			select {
			case <-stepCtx.Done():
				return "", stepCtx.Err()
			case <-time.After(30 * time.Second):
				return "should-not-happen", nil
			}
		})
		if err != nil {
			return "", err
		}
		return "ok", nil
	}
	RegisterWorkflow[empty, string](c, wf, WithWorkflowName("cancellable-step"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	const wfID = "cancellable-step-1"
	h, err := RunWorkflow[empty, string](c, wf, empty{}, WithWorkflowID(wfID))
	if err != nil {
		t.Fatal(err)
	}

	// Let the step start blocking before cancelling.
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := CancelWorkflow(c, wfID); err != nil {
		t.Fatal(err)
	}

	_, gerr := h.GetResult(WithHandleTimeout(3 * time.Second))
	elapsed := time.Since(start)
	if gerr == nil {
		t.Fatal("expected cancellation error from GetResult")
	}
	if elapsed > 1*time.Second {
		t.Errorf("cancel was not proactive: took %v", elapsed)
	}
	st, _ := h.GetStatus()
	if st.Status != WorkflowStatusCancelled {
		t.Errorf("expected status CANCELLED, got %+v", st)
	}
}

// TestCancel_RemoteExecutorPickedUpByPoller simulates a multi-process setup:
// one Context starts the workflow, a second Context (sharing the same DB)
// calls CancelWorkflow. The first Context's cancel poller must observe the
// status change and cancel the local goroutine.
func TestCancel_RemoteExecutorPickedUpByPoller(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "remote-cancel.db")

	mkCtx := func(executorID string) *Context {
		cfg := Config{
			AppName:                  "orctest",
			DatabasePath:             dbPath,
			Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
			ExecutorID:               executorID,
			QueuePollInterval:        5 * time.Millisecond,
			NotificationPollInterval: 5 * time.Millisecond,
			CancelPollInterval:       50 * time.Millisecond,
		}
		ctx, err := NewContext(context.Background(), cfg)
		if err != nil {
			t.Fatalf("NewContext: %v", err)
		}
		return ctx
	}

	type empty struct{}
	wf := func(c *Context, _ empty) (string, error) {
		_, err := RunAsStep(c, func(stepCtx context.Context) (string, error) {
			select {
			case <-stepCtx.Done():
				return "", stepCtx.Err()
			case <-time.After(30 * time.Second):
				return "no", nil
			}
		})
		if err != nil {
			return "", err
		}
		return "ok", nil
	}

	worker := mkCtx("worker-1")
	defer Shutdown(worker, 2*time.Second)
	RegisterWorkflow[empty, string](worker, wf, WithWorkflowName("remote-cancel"))
	if err := Launch(worker); err != nil {
		t.Fatal(err)
	}

	const wfID = "remote-cancel-1"
	h, err := RunWorkflow[empty, string](worker, wf, empty{}, WithWorkflowID(wfID))
	if err != nil {
		t.Fatal(err)
	}

	// Let the worker start the step.
	time.Sleep(100 * time.Millisecond)

	// A second process opens the same DB and cancels.
	canceller := mkCtx("canceller-1")
	defer Shutdown(canceller, 2*time.Second)
	RegisterWorkflow[empty, string](canceller, wf, WithWorkflowName("remote-cancel"))
	if err := Launch(canceller); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := CancelWorkflow(canceller, wfID); err != nil {
		t.Fatal(err)
	}

	_, gerr := h.GetResult(WithHandleTimeout(3 * time.Second))
	elapsed := time.Since(start)
	if gerr == nil {
		t.Fatal("expected cancellation error")
	}
	// Worker's cancel poller should pick this up within a few poll intervals.
	if elapsed > 1500*time.Millisecond {
		t.Errorf("remote cancel poller was slow: took %v", elapsed)
	}
}

// TestQueueDispatchLog_JanitorPurgesOldRows asserts that queue_dispatch_log
// rows older than the largest configured rate-limit window get cleaned up
// by the background janitor.
func TestQueueDispatchLog_JanitorPurgesOldRows(t *testing.T) {
	c := newTestContext(t, func(cfg *Config) {
		cfg.QueueDispatchLogRetention = 100 * time.Millisecond
		cfg.QueueDispatchLogPurgeInterval = 50 * time.Millisecond
	})
	NewWorkflowQueue(c, "purge-q")
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	ids := []string{"a", "b", "c"}
	if err := c.systemDB.recordQueueDispatch(c.ctx, "purge-q", ids); err != nil {
		t.Fatal(err)
	}
	count, err := c.systemDB.countQueueDispatches(c.ctx, "purge-q", time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("pre-purge count = %d, want 3", count)
	}

	// Wait for retention + at least one janitor tick.
	assertEventually(t, 2*time.Second, func() bool {
		n, err := c.systemDB.countQueueDispatches(c.ctx, "purge-q", time.Now().Add(-time.Hour))
		return err == nil && n == 0
	}, "janitor never purged dispatch log rows")
}

// TestCron_DeduplicationByTickBoundary asserts that two cron registrations
// firing at the same boundary insert at most one workflow row (the second
// AlreadyExisted=true). We exercise this directly via the schedule path
// so we don't have to wait on real cron.
func TestCron_DeduplicationByTickBoundary(t *testing.T) {
	c := newTestContext(t)
	var hits atomic.Int32
	wf := func(c *Context, _ time.Time) (string, error) {
		hits.Add(1)
		return "tick", nil
	}
	RegisterWorkflow[time.Time, string](c, wf, WithWorkflowName("cron-dedup"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	boundary := time.Now().UTC().Truncate(time.Second)
	id := fmt.Sprintf("sched-%s-%d", "cron-dedup", boundary.Unix())

	mkRow := func() WorkflowStatus {
		return WorkflowStatus{
			ID:                 id,
			Name:               "cron-dedup",
			Status:             WorkflowStatusEnqueued,
			Input:              boundary,
			ExecutorID:         c.cfg.ExecutorID,
			ApplicationVersion: c.cfg.ApplicationVersion,
			QueueName:          internalQueueName,
			CreatedAt:          time.Now(),
			UpdatedAt:          time.Now(),
			CronSchedule:       "* * * * * *",
		}
	}

	r1, err := c.systemDB.insertWorkflow(c.ctx, insertWorkflowInput{Status: mkRow()})
	if err != nil {
		t.Fatal(err)
	}
	if r1.AlreadyExisted {
		t.Fatal("first insert should not be a duplicate")
	}
	r2, err := c.systemDB.insertWorkflow(c.ctx, insertWorkflowInput{Status: mkRow()})
	if err != nil {
		t.Fatal(err)
	}
	if !r2.AlreadyExisted {
		t.Fatal("second insert with the same boundary id should be deduplicated")
	}

	assertEventually(t, 2*time.Second, func() bool { return hits.Load() == 1 }, "expected exactly one execution")
	time.Sleep(150 * time.Millisecond)
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly one execution, got %d", got)
	}
}

// Sanity: cancellation cause should round-trip as ErrWorkflowCancelled even
// when the workflow swallows the underlying error.
func TestCancel_OverridesSwallowedError(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		_, _ = Sleep(c, 10*time.Second) // intentionally swallows.
		return "should-not-be-recorded", nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("swallower"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	const wfID = "swallow-1"
	h, err := RunWorkflow[string, string](c, wf, "", WithWorkflowID(wfID))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := CancelWorkflow(c, wfID); err != nil {
		t.Fatal(err)
	}
	_, gerr := h.GetResult(WithHandleTimeout(3 * time.Second))
	if gerr == nil {
		t.Fatal("expected cancellation error even when wrapper swallowed it")
	}
	if !errors.Is(gerr, ErrWorkflowCancelledErr) {
		t.Errorf("expected ErrWorkflowCancelled, got %v", gerr)
	}
	st, _ := h.GetStatus()
	if st.Status != WorkflowStatusCancelled {
		t.Errorf("expected status CANCELLED, got %+v", st)
	}
}

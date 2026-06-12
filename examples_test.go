package orc

// End-to-end "story" tests that exercise multiple ORC features in concert,
// demonstrating realistic usage patterns. Each example reads top-to-bottom
// like a small program and is independently runnable as `go test -run
// Example_<name>`.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Example 1: Payment Processing Pipeline
//
// A typical multi-step durable workflow:
//   1. Validate the payment request
//   2. Charge the card via a (mocked) external API
//   3. Persist the payment in the local ledger
//   4. Send a confirmation email
//
// Steps 2, 3 and 4 each have side effects so they are wrapped in `RunAsStep`.
// If the workflow restarts, steps that have already succeeded are skipped.
// ---------------------------------------------------------------------------

type paymentRequest struct {
	OrderID string
	Cents   int64
	Card    string
}

type paymentResult struct {
	OrderID       string
	ChargeID      string
	LedgerEntryID string
	EmailQueued   bool
}

func TestExample_PaymentPipeline(t *testing.T) {
	c := newTestContext(t)

	var charges, ledger, emails atomic.Int32

	chargeCard := func(_ context.Context, req paymentRequest) (string, error) {
		charges.Add(1)
		return "charge-" + req.OrderID, nil
	}
	insertLedger := func(_ context.Context, _ paymentRequest, chargeID string) (string, error) {
		ledger.Add(1)
		return "ledger-" + chargeID, nil
	}
	sendEmail := func(_ context.Context, _ paymentRequest, _ string) (bool, error) {
		emails.Add(1)
		return true, nil
	}

	processPayment := func(c *Context, req paymentRequest) (paymentResult, error) {
		chargeID, err := RunAsStep(c, func(ctx context.Context) (string, error) {
			return chargeCard(ctx, req)
		}, WithStepName("charge_card"))
		if err != nil {
			return paymentResult{}, err
		}

		entryID, err := RunAsStep(c, func(ctx context.Context) (string, error) {
			return insertLedger(ctx, req, chargeID)
		}, WithStepName("insert_ledger"))
		if err != nil {
			return paymentResult{}, err
		}

		queued, err := RunAsStep(c, func(ctx context.Context) (bool, error) {
			return sendEmail(ctx, req, chargeID)
		}, WithStepName("send_email"))
		if err != nil {
			return paymentResult{}, err
		}

		return paymentResult{
			OrderID:       req.OrderID,
			ChargeID:      chargeID,
			LedgerEntryID: entryID,
			EmailQueued:   queued,
		}, nil
	}

	RegisterWorkflow[paymentRequest, paymentResult](c, processPayment, WithWorkflowName("process_payment"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	req := paymentRequest{OrderID: "ord-123", Cents: 9999, Card: "4111-1111-1111-1111"}
	h, err := RunWorkflow[paymentRequest, paymentResult](c, processPayment, req)
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if res.ChargeID != "charge-ord-123" {
		t.Errorf("unexpected charge id: %s", res.ChargeID)
	}
	if !res.EmailQueued {
		t.Error("email not queued")
	}
	if got := charges.Load() + ledger.Load() + emails.Load(); got != 3 {
		t.Errorf("expected 3 step calls, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Example 2: Idempotency under Re-run
//
// Demonstrates that re-invoking the *same* workflow ID with the same input
// returns the existing result rather than running the workflow again.
// ---------------------------------------------------------------------------

func TestExample_IdempotentRun(t *testing.T) {
	c := newTestContext(t)
	var calls atomic.Int32
	wf := func(c *Context, n int) (int, error) {
		calls.Add(1)
		return n * 10, nil
	}
	RegisterWorkflow[int, int](c, wf, WithWorkflowName("multiply_by_10"))
	_ = Launch(c)

	const id = "idempotent-1"
	for i := 0; i < 5; i++ {
		h, err := RunWorkflow[int, int](c, wf, 7, WithWorkflowID(id))
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.GetResult(WithHandleTimeout(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if got != 70 {
			t.Fatalf("got %d", got)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("expected workflow to run exactly once, ran %d times", calls.Load())
	}
}

// ---------------------------------------------------------------------------
// Example 3: Order Fulfillment Queue with Concurrency
//
// Many orders are enqueued; a queue with WorkerConcurrency=2 ensures that
// at most two orders are processed in parallel. We assert that the high
// watermark of concurrent in-flight work never exceeds the configured limit.
// ---------------------------------------------------------------------------

func TestExample_OrderFulfillmentQueue(t *testing.T) {
	c := newTestContext(t)

	var (
		current  atomic.Int32
		highMark atomic.Int32
	)

	fulfillOrder := func(c *Context, orderID string) (string, error) {
		now := current.Add(1)
		for {
			h := highMark.Load()
			if now <= h || highMark.CompareAndSwap(h, now) {
				break
			}
		}
		_, _ = Sleep(c, 30*time.Millisecond)
		current.Add(-1)
		return "fulfilled:" + orderID, nil
	}

	RegisterWorkflow[string, string](c, fulfillOrder, WithWorkflowName("fulfill_order"))
	q := NewWorkflowQueue(c, "fulfillment", WithWorkerConcurrency(2))
	_ = Launch(c)

	const total = 6
	handles := make([]WorkflowHandle[string], total)
	for i := 0; i < total; i++ {
		h, err := RunWorkflow[string, string](c, fulfillOrder, fmt.Sprintf("order-%d", i), WithQueue(q.Name))
		if err != nil {
			t.Fatal(err)
		}
		handles[i] = h
	}
	for _, h := range handles {
		if _, err := h.GetResult(WithHandleTimeout(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	if peak := highMark.Load(); peak > 2 {
		t.Errorf("expected concurrency<=2, observed peak %d", peak)
	}
}

// ---------------------------------------------------------------------------
// Example 4: Chat - two workflows talking via Send/Recv
//
// One workflow waits to receive a "greeting" message; another sends one.
// Demonstrates durable inter-workflow messaging.
// ---------------------------------------------------------------------------

func TestExample_ChatBetweenWorkflows(t *testing.T) {
	c := newTestContext(t)

	var received atomic.Value

	listener := func(c *Context, _ string) (string, error) {
		msg, err := Recv[string](c, "greetings", 2*time.Second)
		if err != nil {
			return "", err
		}
		received.Store(msg)
		return "got:" + msg, nil
	}
	speaker := func(c *Context, target string) (string, error) {
		if err := Send(c, target, "hello, world", "greetings"); err != nil {
			return "", err
		}
		return "sent", nil
	}

	RegisterWorkflow[string, string](c, listener, WithWorkflowName("chat_listener"))
	RegisterWorkflow[string, string](c, speaker, WithWorkflowName("chat_speaker"))
	_ = Launch(c)

	listenerID := "listener-1"
	hL, err := RunWorkflow[string, string](c, listener, "", WithWorkflowID(listenerID))
	if err != nil {
		t.Fatal(err)
	}
	// Make sure the listener is up before the speaker fires.
	time.Sleep(50 * time.Millisecond)

	hS, err := RunWorkflow[string, string](c, speaker, listenerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hS.GetResult(WithHandleTimeout(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := hL.GetResult(WithHandleTimeout(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	if got, _ := received.Load().(string); got != "hello, world" {
		t.Errorf("listener didn't get greeting: %v", received.Load())
	}
}

// ---------------------------------------------------------------------------
// Example 5: Long-running Approval Workflow with Events
//
// Workflow exposes its current step ("waiting", "approved") via SetEvent so
// dashboards can poll it without disturbing the workflow itself.
// ---------------------------------------------------------------------------

func TestExample_ApprovalWorkflowWithEvents(t *testing.T) {
	c := newTestContext(t)

	approvalWF := func(c *Context, _ string) (string, error) {
		_ = SetEvent(c, "stage", "waiting")
		// Pretend we're waiting for an external approver, then advance.
		_, _ = Sleep(c, 50*time.Millisecond)
		_ = SetEvent(c, "stage", "approved")
		return "approved", nil
	}
	RegisterWorkflow[string, string](c, approvalWF, WithWorkflowName("approval"))
	_ = Launch(c)

	const wfID = "approval-1"
	h, err := RunWorkflow[string, string](c, approvalWF, "", WithWorkflowID(wfID))
	if err != nil {
		t.Fatal(err)
	}

	// Poll for "waiting" state from outside the workflow.
	deadline := time.Now().Add(2 * time.Second)
	var stage string
	for time.Now().Before(deadline) {
		v, err := GetEvent[string](c, wfID, "stage", 50*time.Millisecond)
		if err == nil && v != "" {
			stage = v
			break
		}
	}
	if stage == "" {
		t.Fatal("no stage event observed")
	}

	res, err := h.GetResult(WithHandleTimeout(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if res != "approved" {
		t.Errorf("got %q", res)
	}

	final, err := GetEvent[string](c, wfID, "stage", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if final != "approved" {
		t.Errorf("final stage: %q", final)
	}
}

// ---------------------------------------------------------------------------
// Example 6: Saga with Compensation
//
// Step A succeeds; step B fails; step A's compensation is invoked. The
// compensation itself is a step so it survives restarts.
// ---------------------------------------------------------------------------

func TestExample_SagaCompensation(t *testing.T) {
	c := newTestContext(t)

	var (
		bookedHotel   atomic.Bool
		bookedFlight  atomic.Bool
		hotelRefunded atomic.Bool
	)

	bookTrip := func(c *Context, _ string) (string, error) {
		_, err := RunAsStep(c, func(_ context.Context) (string, error) {
			bookedHotel.Store(true)
			return "hotel-ok", nil
		}, WithStepName("book_hotel"))
		if err != nil {
			return "", err
		}

		_, ferr := RunAsStep(c, func(_ context.Context) (string, error) {
			return "", errors.New("flight provider unreachable")
		}, WithStepName("book_flight"))
		if ferr != nil {
			// Compensate.
			_, _ = RunAsStep(c, func(_ context.Context) (string, error) {
				if bookedHotel.Load() {
					hotelRefunded.Store(true)
				}
				return "refunded", nil
			}, WithStepName("refund_hotel"))
			return "", ferr
		}
		bookedFlight.Store(true)
		return "trip-booked", nil
	}

	RegisterWorkflow[string, string](c, bookTrip, WithWorkflowName("book_trip"))
	_ = Launch(c)

	h, err := RunWorkflow[string, string](c, bookTrip, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.GetResult(WithHandleTimeout(3 * time.Second))
	if err == nil {
		t.Fatal("expected workflow error from flight booking")
	}
	if !bookedHotel.Load() {
		t.Error("hotel was never booked")
	}
	if bookedFlight.Load() {
		t.Error("flight should not have been recorded as booked")
	}
	if !hotelRefunded.Load() {
		t.Error("compensation didn't run")
	}
}

// ---------------------------------------------------------------------------
// Example 7: Step retries with exponential backoff
//
// The step fails twice and succeeds on the 3rd attempt. The workflow should
// see the final success without ever knowing about the transient errors.
// ---------------------------------------------------------------------------

func TestExample_StepRetries(t *testing.T) {
	c := newTestContext(t)
	var attempts atomic.Int32

	flaky := func(_ context.Context) (string, error) {
		n := attempts.Add(1)
		if n < 3 {
			return "", errors.New("transient failure")
		}
		return "succeeded", nil
	}

	wf := func(c *Context, _ string) (string, error) {
		return RunAsStep(c, flaky,
			WithStepName("flaky_step"),
			WithStepMaxRetries(5),
			WithStepRetryBackoff(5*time.Millisecond, 50*time.Millisecond, 2.0),
		)
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("retry_wf"))
	_ = Launch(c)

	h, err := RunWorkflow[string, string](c, wf, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult(WithHandleTimeout(3 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "succeeded" {
		t.Errorf("got %q", got)
	}
	if attempts.Load() != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts.Load())
	}
}

// ---------------------------------------------------------------------------
// Example 8: Fan-out / Fan-in
//
// Parent workflow kicks off N child workflows in parallel via WithQueue and
// then collects their results.
// ---------------------------------------------------------------------------

func TestExample_FanOutFanIn(t *testing.T) {
	c := newTestContext(t)

	doublerCh := make(chan int, 16)

	doubler := func(c *Context, n int) (int, error) {
		doublerCh <- n
		return n * 2, nil
	}

	parent := func(c *Context, n int) (int, error) {
		q := NewWorkflowQueue(c, "fanout", WithWorkerConcurrency(4))
		handles := make([]WorkflowHandle[int], n)
		for i := 0; i < n; i++ {
			h, err := RunWorkflow[int, int](c, doubler, i, WithQueue(q.Name))
			if err != nil {
				return 0, err
			}
			handles[i] = h
		}
		sum := 0
		for _, h := range handles {
			r, err := h.GetResult(WithHandleTimeout(5 * time.Second))
			if err != nil {
				return 0, err
			}
			sum += r
		}
		return sum, nil
	}

	RegisterWorkflow[int, int](c, doubler, WithWorkflowName("doubler"))
	RegisterWorkflow[int, int](c, parent, WithWorkflowName("fanout_parent"))
	_ = Launch(c)

	h, err := RunWorkflow[int, int](c, parent, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult(WithHandleTimeout(15 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// 0..4 doubled and summed = 2*(0+1+2+3+4) = 20
	if got != 20 {
		t.Errorf("expected 20, got %d", got)
	}
	close(doublerCh)
	var seen []int
	for v := range doublerCh {
		seen = append(seen, v)
	}
	sort.Ints(seen)
	if len(seen) != 5 {
		t.Errorf("expected 5 invocations, got %v", seen)
	}
}

// ---------------------------------------------------------------------------
// Example 9: Cron-scheduled workflow
//
// Register a workflow that runs every second and assert that it actually
// fires in a small window.
// ---------------------------------------------------------------------------

func TestExample_CronSchedule(t *testing.T) {
	c := newTestContext(t)
	var fired atomic.Int32
	wf := func(c *Context, _ string) (string, error) {
		fired.Add(1)
		return "tick", nil
	}
	RegisterWorkflow[string, string](c, wf,
		WithWorkflowName("cron_wf"),
		WithSchedule("* * * * * *"), // every second
	)
	_ = Launch(c)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fired.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if fired.Load() < 1 {
		t.Errorf("scheduled workflow never fired in window, fired=%d", fired.Load())
	}
}

// ---------------------------------------------------------------------------
// Example 10: Recovery across simulated crash with side-effect tracking
//
// Two-step workflow on a file-backed DB. Crash between the steps, restart,
// and verify only the unfinished step re-runs.
// ---------------------------------------------------------------------------

func TestExample_RecoverySkipsCompletedSteps(t *testing.T) {
	var step1Calls, step2Calls atomic.Int32

	wf := func(c *Context, _ string) (string, error) {
		_, _ = RunAsStep(c, func(_ context.Context) (string, error) {
			step1Calls.Add(1)
			return "s1", nil
		}, WithStepName("s1"))
		// Sleep a long time so we shut down before step2 runs (the original
		// goroutine is killed at shutdown).
		_, _ = Sleep(c, 500*time.Millisecond)
		_, _ = RunAsStep(c, func(_ context.Context) (string, error) {
			step2Calls.Add(1)
			return "s2", nil
		}, WithStepName("s2"))
		return "done", nil
	}

	c := newTestContextFile(t, "recovery-skip.db")
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("recover_skip"))
	_ = Launch(c)
	const wfID = "rs-1"
	_, _ = RunWorkflow[string, string](c, wf, "", WithWorkflowID(wfID))

	// Give it just enough time to record step1 then crash.
	time.Sleep(100 * time.Millisecond)
	Shutdown(c, 50*time.Millisecond)

	// Reopen and recover.
	c2 := reopenContext(t, c)
	RegisterWorkflow[string, string](c2, wf, WithWorkflowName("recover_skip"))
	if err := Launch(c2); err != nil {
		t.Fatal(err)
	}

	h, err := RetrieveWorkflow[string](c2, wfID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "done" {
		t.Errorf("got %q", got)
	}
	if s1 := step1Calls.Load(); s1 != 1 {
		t.Errorf("step1 should run exactly once, ran %d times", s1)
	}
	if s2 := step2Calls.Load(); s2 < 1 {
		t.Errorf("step2 should run at least once, ran %d times", s2)
	}
}

// ---------------------------------------------------------------------------
// Example 11: Producer / Consumer with deduplication
//
// Multiple producers race to enqueue the same logical request, the queue's
// deduplication ID guarantees only one workflow row is created.
// ---------------------------------------------------------------------------

func TestExample_DedupedProducers(t *testing.T) {
	c := newTestContext(t)
	var calls atomic.Int32
	wf := func(c *Context, payload string) (string, error) {
		calls.Add(1)
		return "ok-" + payload, nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("dedup_wf"))
	q := NewWorkflowQueue(c, "dedup-queue")
	_ = Launch(c)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = RunWorkflow[string, string](c, wf, "payload",
				WithQueue(q.Name),
				WithDeduplicationID("logical-key"),
			)
		}(i)
	}
	wg.Wait()

	// Wait for the surviving workflow to complete.
	assertEventually(t, 5*time.Second, func() bool {
		ws, _ := ListWorkflows(c, WithListWorkflowQueue(q.Name),
			WithListWorkflowStatus(WorkflowStatusSuccess))
		return len(ws) == 1
	}, "expected exactly one successful workflow")

	if got := calls.Load(); got != 1 {
		t.Errorf("expected 1 call (dedup), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Example 12: Cancel an in-flight workflow
// ---------------------------------------------------------------------------

func TestExample_CancelInFlight(t *testing.T) {
	c := newTestContext(t)
	var done atomic.Bool
	wf := func(c *Context, _ string) (string, error) {
		// Workflows that want prompt cancellation should propagate the
		// error returned by Sleep / Recv / etc.
		if _, err := Sleep(c, 5*time.Second); err != nil {
			return "", err
		}
		done.Store(true)
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("cancellable"))
	_ = Launch(c)

	wfID := "cancel-1"
	h, err := RunWorkflow[string, string](c, wf, "", WithWorkflowID(wfID))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	if err := CancelWorkflow(c, wfID); err != nil {
		t.Fatal(err)
	}

	_, err = h.GetResult(WithHandleTimeout(3 * time.Second))
	if err == nil {
		t.Fatal("expected error after cancel")
	}
	// Cancellation should be proactive: the workflow should stop well
	// before the original 5s Sleep would have elapsed.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancel was not proactive: took %v to terminate", elapsed)
	}
	if done.Load() {
		t.Error("workflow finished despite cancel")
	}
}

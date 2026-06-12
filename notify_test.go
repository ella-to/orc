package orc

import (
	"testing"
	"time"
)

// slowPollConfig cranks every poll interval way up so the only way the test
// can finish quickly is via the in-process notify hub / queue wake.
func slowPollConfig(cfg *Config) {
	cfg.QueuePollInterval = 3 * time.Second
	cfg.NotificationPollInterval = 3 * time.Second
}

// TestNotifyHub_SendRecvIsImmediate: with a 3s poll interval, Send→Recv in
// the same process must still complete in well under a second because the
// hub wakes the receiver directly.
func TestNotifyHub_SendRecvIsImmediate(t *testing.T) {
	c := newTestContext(t, slowPollConfig)

	receiverID := "hub-recv-1"
	receiver := func(c *Context, _ string) (string, error) {
		return Recv[string](c, "hub-topic", 10*time.Second)
	}
	RegisterWorkflow[string, string](c, receiver, WithWorkflowName("hub-receiver"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	rh, err := RunWorkflow[string, string](c, receiver, "", WithWorkflowID(receiverID))
	if err != nil {
		t.Fatal(err)
	}
	// Give the receiver a moment to enter its Recv wait.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	if err := Send(c, receiverID, "ping", "hub-topic"); err != nil {
		t.Fatal(err)
	}
	got, err := rh.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "ping" {
		t.Fatalf("got %q", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Send→Recv took %v; the notify hub should make it near-instant", elapsed)
	}
}

// TestNotifyHub_QueueWakeIsImmediate: enqueueing in-process must nudge the
// queue runner instead of waiting out QueuePollInterval, and workflow
// completion must wake the polling handle.
func TestNotifyHub_QueueWakeIsImmediate(t *testing.T) {
	c := newTestContext(t, slowPollConfig)

	wf := func(c *Context, in string) (string, error) { return "ok:" + in, nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("hub-queued"))
	NewWorkflowQueue(c, "hub-q")
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	h, err := RunWorkflow[string, string](c, wf, "x", WithQueue("hub-q"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if out != "ok:x" {
		t.Fatalf("got %q", out)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("enqueue→result took %v; queue wake + done signal should make it fast", elapsed)
	}
}

// TestNotifyHub_GetEventIsImmediate: an external GetEvent waiter is woken the
// moment the workflow publishes the event.
func TestNotifyHub_GetEventIsImmediate(t *testing.T) {
	c := newTestContext(t, slowPollConfig)

	publisher := func(c *Context, val string) (string, error) {
		_, _ = Sleep(c, 100*time.Millisecond)
		if err := SetEvent(c, "stage", val); err != nil {
			return "", err
		}
		return val, nil
	}
	RegisterWorkflow[string, string](c, publisher, WithWorkflowName("hub-publisher"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	h, err := RunWorkflow[string, string](c, publisher, "v1", WithWorkflowID("hub-pub-1"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := GetEvent[string](c, "hub-pub-1", "stage", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "v1" {
		t.Fatalf("got %q", got)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("SetEvent→GetEvent took %v; the notify hub should make it near-instant", elapsed)
	}
	if _, err := h.GetResult(WithHandleTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

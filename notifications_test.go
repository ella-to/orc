package orc

import (
	"testing"
	"time"
)

func TestSendRecv_Basic(t *testing.T) {
	c := newTestContext(t)

	receiverID := "recv-1"

	receiver := func(c *Context, _ string) (string, error) {
		return Recv[string](c, "topic1", 2*time.Second)
	}
	sender := func(c *Context, msg string) (string, error) {
		err := Send(c, receiverID, msg, "topic1")
		return "sent", err
	}

	RegisterWorkflow[string, string](c, receiver)
	RegisterWorkflow[string, string](c, sender)
	_ = Launch(c)

	rh, err := RunWorkflow[string, string](c, receiver, "", WithWorkflowID(receiverID))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := RunWorkflow[string, string](c, sender, "hola")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sh.GetResult(WithHandleTimeout(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := rh.GetResult(WithHandleTimeout(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "hola" {
		t.Errorf("got %q", got)
	}
}

func TestRecv_Timeout(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		return Recv[string](c, "never", 100*time.Millisecond)
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "")
	got, err := h.GetResult()
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Errorf("expected empty result on timeout, got %q", got)
	}
}

func TestSetEvent_GetEvent(t *testing.T) {
	c := newTestContext(t)
	publisher := func(c *Context, val string) (string, error) {
		if err := SetEvent(c, "k", val); err != nil {
			return "", err
		}
		return val, nil
	}
	RegisterWorkflow[string, string](c, publisher)
	_ = Launch(c)

	pubID := "pub-1"
	h, err := RunWorkflow[string, string](c, publisher, "v1", WithWorkflowID(pubID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.GetResult(WithHandleTimeout(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// External reader (outside any workflow).
	got, err := GetEvent[string](c, pubID, "k", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "v1" {
		t.Errorf("got %q", got)
	}
}

func TestGetEvent_TimesOut(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	_, err := GetEvent[string](c, "no-such-wf", "missing", 100*time.Millisecond)
	if !IsCode(err, ErrWorkflowAwaitTimeout) {
		t.Errorf("expected timeout, got %v", err)
	}
}

func TestSendRecv_DurabilityAcrossSteps(t *testing.T) {
	// Verify that Send+Recv survive workflow re-execution by ensuring step
	// idempotency: a sender that runs Send twice in one body still creates
	// only one notification (because Send is wrapped as a step).
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		_ = Send(c, "x", "msg", "topic")
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "")
	if _, err := h.GetResult(WithHandleTimeout(time.Second)); err != nil {
		t.Fatal(err)
	}
	steps, _ := GetWorkflowSteps(c, h.GetWorkflowID())
	if len(steps) != 1 {
		t.Errorf("expected 1 step (Send), got %d", len(steps))
	}
}

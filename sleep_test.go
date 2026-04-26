package orc

import (
	"testing"
	"time"
)

func TestSleep_DurableInsideWorkflow(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		start := time.Now()
		if _, err := Sleep(c, 100*time.Millisecond); err != nil {
			return "", err
		}
		if time.Since(start) < 90*time.Millisecond {
			return "too fast", nil
		}
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "")
	got, err := h.GetResult(WithHandleTimeout(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got != "ok" {
		t.Errorf("got %q", got)
	}
	steps, _ := GetWorkflowSteps(c, h.GetWorkflowID())
	if len(steps) != 1 {
		t.Errorf("expected 1 sleep step, got %d", len(steps))
	}
}

func TestSleep_OutsideWorkflow_Direct(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	start := time.Now()
	d, err := Sleep(c, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if d != 50*time.Millisecond {
		t.Errorf("got %v", d)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Error("did not sleep")
	}
}

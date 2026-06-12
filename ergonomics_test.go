package orc

import (
	"strings"
	"testing"
	"time"
)

// TestRegisterWorkflow_ValidatesSignatureAtRegistration: a bad workflow
// signature must fail fast with an actionable panic instead of erroring on
// first execution.
func TestRegisterWorkflow_ValidatesSignatureAtRegistration(t *testing.T) {
	c := newTestContext(t)

	expectPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Fatalf("%s: expected panic", name)
			}
			if msg, ok := r.(string); !ok || !strings.Contains(msg, "cannot register workflow") {
				t.Fatalf("%s: unhelpful panic: %v", name, r)
			}
		}()
		fn()
	}

	expectPanic("missing error return", func() {
		RegisterWorkflow[string, string](c, func(c *Context, s string) string { return s })
	})
	expectPanic("wrong first parameter", func() {
		RegisterWorkflow[string, string](c, func(s string, n int) (string, error) { return s, nil })
	})
	expectPanic("too many parameters", func() {
		RegisterWorkflow[string, string](c, func(c *Context, a, b string) (string, error) { return a, nil })
	})
	expectPanic("type parameter mismatch", func() {
		RegisterWorkflow[int, string](c, func(c *Context, s string) (string, error) { return s, nil })
	})
}

// TestDirectHandle_GetResultTwice: repeated GetResult calls on the same
// handle must keep returning the result (served from the DB once the
// in-memory channel is drained), not an error.
func TestDirectHandle_GetResultTwice(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, in string) (string, error) { return "out:" + in, nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("double-get"))
	if err := Launch(c); err != nil {
		t.Fatal(err)
	}

	h, err := RunWorkflow[string, string](c, wf, "x")
	if err != nil {
		t.Fatal(err)
	}
	first, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.GetResult(WithHandleTimeout(5 * time.Second))
	if err != nil {
		t.Fatalf("second GetResult: %v", err)
	}
	if first != "out:x" || second != "out:x" {
		t.Fatalf("results differ: %q vs %q", first, second)
	}
}

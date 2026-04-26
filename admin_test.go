package orc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// adminTestServer wires AdminHandler under "/admin" using StripPrefix and
// returns an httptest server plus a small helper struct.
type adminTestServer struct {
	t       *testing.T
	srv     *httptest.Server
	baseURL string
}

func newAdminTestServer(t *testing.T, c *Context, opts ...AdminOption) *adminTestServer {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/admin/", http.StripPrefix("/admin", AdminHandler(c, opts...)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &adminTestServer{t: t, srv: srv, baseURL: srv.URL + "/admin"}
}

func (a *adminTestServer) do(method, path string, body any) (*http.Response, []byte) {
	a.t.Helper()
	var rd io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			a.t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequest(method, a.baseURL+path, rd)
	if err != nil {
		a.t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("do request %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		a.t.Fatalf("read body: %v", err)
	}
	return resp, data
}

func (a *adminTestServer) doRaw(method, path string, body []byte) (*http.Response, []byte) {
	a.t.Helper()
	req, err := http.NewRequest(method, a.baseURL+path, bytes.NewReader(body))
	if err != nil {
		a.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		a.t.Fatalf("read body: %v", err)
	}
	return resp, data
}

func (a *adminTestServer) doJSON(method, path string, body any, out any) *http.Response {
	a.t.Helper()
	resp, data := a.do(method, path, body)
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			a.t.Fatalf("unmarshal %s %s body=%q: %v", method, path, string(data), err)
		}
	}
	return resp
}

// ----------------------------------------------------------------------------

func TestAdmin_HealthAndIndex(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	srv := newAdminTestServer(t, c)

	resp, data := srv.do("GET", "/health", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("health status=%d body=%s", resp.StatusCode, string(data))
	}
	var health map[string]any
	if err := json.Unmarshal(data, &health); err != nil {
		t.Fatal(err)
	}
	if health["status"] != "ok" {
		t.Errorf("expected status ok, got %v", health["status"])
	}

	resp, data = srv.do("GET", "/", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("index status=%d body=%s", resp.StatusCode, string(data))
	}
	if !bytes.Contains(data, []byte(`"routes"`)) {
		t.Errorf("expected routes list, got %s", string(data))
	}
}

func TestAdmin_Info(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "ok", nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("registered_alpha"))
	NewWorkflowQueue(c, "extra-q", WithWorkerConcurrency(2))
	_ = Launch(c)

	h, _ := RunWorkflow[string, string](c, wf, "")
	_, _ = h.GetResult()

	srv := newAdminTestServer(t, c)
	var info AdminInfo
	resp := srv.doJSON("GET", "/info", nil, &info)
	if resp.StatusCode != 200 {
		t.Fatalf("info status=%d", resp.StatusCode)
	}
	if info.AppName != "orctest" {
		t.Errorf("AppName=%q", info.AppName)
	}
	if info.RegisteredCount < 1 {
		t.Errorf("RegisteredCount=%d", info.RegisteredCount)
	}
	hasAlpha := false
	for _, n := range info.Registered {
		if n == "registered_alpha" {
			hasAlpha = true
		}
	}
	if !hasAlpha {
		t.Errorf("expected registered_alpha in %v", info.Registered)
	}
	hasQ := false
	for _, q := range info.Queues {
		if q.Name == "extra-q" {
			hasQ = true
		}
	}
	if !hasQ {
		t.Errorf("expected extra-q in %v", info.Queues)
	}
	if info.StatusCounts[string(WorkflowStatusSuccess)] < 1 {
		t.Errorf("expected at least one SUCCESS, got %v", info.StatusCounts)
	}
}

func TestAdmin_ListWorkflows_Filters(t *testing.T) {
	c := newTestContext(t)
	wfA := func(c *Context, n int) (int, error) { return n, nil }
	wfB := func(c *Context, n int) (int, error) { return n, nil }
	RegisterWorkflow[int, int](c, wfA, WithWorkflowName("alpha"))
	RegisterWorkflow[int, int](c, wfB, WithWorkflowName("beta"))
	_ = Launch(c)

	for i := 0; i < 3; i++ {
		h, _ := RunWorkflow[int, int](c, wfA, i)
		_, _ = h.GetResult()
	}
	for i := 0; i < 2; i++ {
		h, _ := RunWorkflow[int, int](c, wfB, i)
		_, _ = h.GetResult()
	}

	srv := newAdminTestServer(t, c)

	// no filter: 5 total
	var all AdminListResponse
	if resp := srv.doJSON("GET", "/workflows", nil, &all); resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if all.Count != 5 {
		t.Errorf("all count=%d", all.Count)
	}

	// filter by name
	var alpha AdminListResponse
	srv.doJSON("GET", "/workflows?name=alpha", nil, &alpha)
	if alpha.Count != 3 {
		t.Errorf("alpha count=%d", alpha.Count)
	}

	// filter by status
	var ok AdminListResponse
	srv.doJSON("GET", "/workflows?status=SUCCESS", nil, &ok)
	if ok.Count != 5 {
		t.Errorf("status SUCCESS count=%d", ok.Count)
	}

	// limit
	var limited AdminListResponse
	srv.doJSON("GET", "/workflows?limit=2", nil, &limited)
	if limited.Count != 2 {
		t.Errorf("limit=2 count=%d", limited.Count)
	}
}

func TestAdmin_GetWorkflow_WithStepsAndDuration(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		_, _ = RunAsStep(c, func(_ context.Context) (string, error) { return "a", nil }, WithStepName("step_a"))
		_, _ = RunAsStep(c, func(_ context.Context) (string, error) { return "b", nil }, WithStepName("step_b"))
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("detail-1"))
	_, _ = h.GetResult(WithHandleTimeout(2 * time.Second))

	srv := newAdminTestServer(t, c)

	var detail AdminWorkflowDetail
	resp := srv.doJSON("GET", "/workflows/detail-1", nil, &detail)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if detail.ID != "detail-1" {
		t.Errorf("id=%q", detail.ID)
	}
	if detail.Status != WorkflowStatusSuccess {
		t.Errorf("status=%v", detail.Status)
	}
	if !detail.IsTerminal {
		t.Errorf("expected terminal")
	}
	if len(detail.Steps) != 2 {
		t.Errorf("steps=%d", len(detail.Steps))
	}
	if detail.DurationHuman == "" {
		t.Errorf("expected duration human readable")
	}
}

func TestAdmin_GetWorkflow_NotFound(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	srv := newAdminTestServer(t, c)
	resp, _ := srv.do("GET", "/workflows/nope", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status=%d", resp.StatusCode)
	}
}

func TestAdmin_Steps(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		_, _ = RunAsStep(c, func(_ context.Context) (string, error) { return "x", nil }, WithStepName("only"))
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("steps-1"))
	_, _ = h.GetResult()

	srv := newAdminTestServer(t, c)
	var steps []AdminStepView
	srv.doJSON("GET", "/workflows/steps-1/steps", nil, &steps)
	if len(steps) != 1 {
		t.Fatalf("steps=%d", len(steps))
	}
	if steps[0].StepName != "only" {
		t.Errorf("name=%q", steps[0].StepName)
	}
}

func TestAdmin_Cancel(t *testing.T) {
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
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("cancel-1"))
	time.Sleep(50 * time.Millisecond)

	srv := newAdminTestServer(t, c)
	resp, data := srv.do("POST", "/workflows/cancel-1/cancel", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("cancel status=%d body=%s", resp.StatusCode, string(data))
	}
	st, _ := h.GetStatus()
	if st.Status != WorkflowStatusCancelled {
		t.Errorf("status=%s", st.Status)
	}

	// 404 for unknown id
	resp, _ = srv.do("POST", "/workflows/nope/cancel", nil)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 got %d", resp.StatusCode)
	}
}

func TestAdmin_Resume(t *testing.T) {
	c := newTestContext(t)
	var attempts atomic.Int32
	wf := func(c *Context, _ string) (string, error) {
		if attempts.Add(1) == 1 {
			return "", errors.New("boom")
		}
		return "ok", nil
	}
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("resumable"))
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("resume-1"))
	if _, err := h.GetResult(); err == nil {
		t.Fatal("expected first error")
	}

	srv := newAdminTestServer(t, c)
	resp, data := srv.do("POST", "/workflows/resume-1/resume", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("resume status=%d body=%s", resp.StatusCode, string(data))
	}

	// Wait for it to come back as SUCCESS.
	assertEventually(t, 2*time.Second, func() bool {
		st, _ := h.GetStatus()
		return st.Status == WorkflowStatusSuccess
	}, "resumed workflow should reach SUCCESS")
}

func TestAdmin_Fork(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) {
		s1, _ := RunAsStep(c, func(_ context.Context) (string, error) { return "A", nil }, WithStepName("s1"))
		s2, _ := RunAsStep(c, func(_ context.Context) (string, error) { return "B", nil }, WithStepName("s2"))
		return s1 + s2, nil
	}
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)
	h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID("orig-fork"))
	if got, _ := h.GetResult(); got != "AB" {
		t.Fatalf("orig got %q", got)
	}

	srv := newAdminTestServer(t, c)
	var fr AdminForkResponse
	resp := srv.doJSON("POST", "/workflows/orig-fork/fork", AdminForkRequest{StartFromStep: 1}, &fr)
	if resp.StatusCode != 200 {
		t.Fatalf("fork status=%d", resp.StatusCode)
	}
	if fr.NewWorkflowID == "" {
		t.Fatal("missing new id")
	}

	// New workflow should also reach SUCCESS.
	assertEventually(t, 2*time.Second, func() bool {
		st, _ := c.systemDB.getWorkflowStatus(c.ctx, fr.NewWorkflowID, false)
		return st != nil && st.Status == WorkflowStatusSuccess
	}, "forked workflow should reach SUCCESS")

	// Bad body -> 400.
	resp, _ = srv.doRaw("POST", "/workflows/orig-fork/fork", []byte("not-json"))
	if resp.StatusCode != 400 {
		t.Errorf("bad json status=%d", resp.StatusCode)
	}
}

func TestAdmin_DeleteSingleAndBulk(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "ok", nil }
	RegisterWorkflow[string, string](c, wf)
	_ = Launch(c)

	for _, id := range []string{"d1", "d2", "d3"} {
		h, _ := RunWorkflow[string, string](c, wf, "", WithWorkflowID(id))
		_, _ = h.GetResult()
	}

	srv := newAdminTestServer(t, c)

	// single
	var one AdminDeleteResponse
	resp := srv.doJSON("DELETE", "/workflows/d1", nil, &one)
	if resp.StatusCode != 200 {
		t.Fatalf("delete status=%d", resp.StatusCode)
	}
	if len(one.Deleted) != 1 || one.Deleted[0] != "d1" {
		t.Errorf("deleted=%v", one.Deleted)
	}

	// bulk
	var bulk AdminDeleteResponse
	resp = srv.doJSON("POST", "/workflows/delete", AdminDeleteRequest{IDs: []string{"d2", "d3"}}, &bulk)
	if resp.StatusCode != 200 {
		t.Fatalf("bulk delete status=%d", resp.StatusCode)
	}
	if len(bulk.Deleted) != 2 {
		t.Errorf("bulk=%v", bulk.Deleted)
	}

	// follow-up GET of any deleted -> 404
	resp, _ = srv.do("GET", "/workflows/d1", nil)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404 after delete, got %d", resp.StatusCode)
	}

	// empty bulk body -> 400
	resp, _ = srv.do("POST", "/workflows/delete", AdminDeleteRequest{})
	if resp.StatusCode != 400 {
		t.Errorf("expected 400 for empty ids, got %d", resp.StatusCode)
	}
}

func TestAdmin_TreeAndChildren(t *testing.T) {
	c := newTestContext(t)
	leaf := func(c *Context, n int) (int, error) { return n * 2, nil }
	parent := func(c *Context, _ string) (string, error) {
		for i := 0; i < 3; i++ {
			h, err := RunWorkflow[int, int](c, leaf, i, WithWorkflowID(fmt.Sprintf("child-%d", i)))
			if err != nil {
				return "", err
			}
			if _, err := h.GetResult(WithHandleTimeout(2 * time.Second)); err != nil {
				return "", err
			}
		}
		return "ok", nil
	}
	RegisterWorkflow[int, int](c, leaf, WithWorkflowName("leaf"))
	RegisterWorkflow[string, string](c, parent, WithWorkflowName("parent"))
	_ = Launch(c)

	h, _ := RunWorkflow[string, string](c, parent, "", WithWorkflowID("p1"))
	if _, err := h.GetResult(WithHandleTimeout(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	srv := newAdminTestServer(t, c)

	// children endpoint
	var kids []AdminWorkflowView
	srv.doJSON("GET", "/workflows/p1/children", nil, &kids)
	if len(kids) != 3 {
		t.Errorf("children count=%d", len(kids))
	}

	// tree endpoint
	var tree AdminWorkflowTreeNode
	srv.doJSON("GET", "/workflows/p1/tree", nil, &tree)
	if tree.ID != "p1" {
		t.Errorf("tree root id=%q", tree.ID)
	}
	if len(tree.Children) != 3 {
		t.Errorf("tree children=%d", len(tree.Children))
	}

	// depth=0 -> only root, no children
	var shallow AdminWorkflowTreeNode
	srv.doJSON("GET", "/workflows/p1/tree?depth=0", nil, &shallow)
	if len(shallow.Children) != 0 {
		t.Errorf("depth=0 should have no children, got %d", len(shallow.Children))
	}
}

func TestAdmin_Queues(t *testing.T) {
	c := newTestContext(t)
	NewWorkflowQueue(c, "qa", WithWorkerConcurrency(3))
	_ = Launch(c)
	srv := newAdminTestServer(t, c)
	var qs []AdminQueueView
	srv.doJSON("GET", "/queues", nil, &qs)
	found := false
	for _, q := range qs {
		if q.Name == "qa" {
			found = true
			if q.WorkerConcurrency != 3 {
				t.Errorf("worker concurrency=%d", q.WorkerConcurrency)
			}
		}
	}
	if !found {
		t.Errorf("missing qa in %+v", qs)
	}
}

func TestAdmin_Registered(t *testing.T) {
	c := newTestContext(t)
	wf := func(c *Context, _ string) (string, error) { return "", nil }
	RegisterWorkflow[string, string](c, wf, WithWorkflowName("zz_admin"))
	_ = Launch(c)
	srv := newAdminTestServer(t, c)
	var names []string
	srv.doJSON("GET", "/registered", nil, &names)
	found := false
	for _, n := range names {
		if n == "zz_admin" {
			found = true
		}
	}
	if !found {
		t.Errorf("missing zz_admin in %v", names)
	}
}

func TestAdmin_MethodNotAllowed(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	srv := newAdminTestServer(t, c)
	// PUT on /workflows is not registered
	resp, _ := srv.do("PUT", "/workflows", nil)
	if resp.StatusCode == 200 {
		t.Errorf("expected non-200 for unsupported method, got %d", resp.StatusCode)
	}
}

func TestAdmin_Mountable_WithPrefix(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)

	mux := http.NewServeMux()
	// custom prefix to prove the handler is mountable anywhere
	mux.Handle("/foo/bar/", http.StripPrefix("/foo/bar", AdminHandler(c)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/foo/bar/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status=%d", resp.StatusCode)
	}
}

func TestAdmin_HumanizeDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{750 * time.Millisecond, "750ms"},
		{1500 * time.Millisecond, "1.5s"},
		{72 * time.Second, "1m12s"},
		{(time.Hour + 3*time.Minute + 45*time.Second), "1h3m45s"},
		{26 * time.Hour, "1d2h"},
	}
	for _, tc := range cases {
		got := humanizeDuration(tc.d)
		if got != tc.want {
			t.Errorf("humanizeDuration(%v) = %q want %q", tc.d, got, tc.want)
		}
	}
}

func TestAdmin_PrettyJSON(t *testing.T) {
	c := newTestContext(t)
	_ = Launch(c)
	srv := newAdminTestServer(t, c, WithAdminPrettyJSON())
	_, data := srv.do("GET", "/health", nil)
	if !strings.Contains(string(data), "\n  ") {
		t.Errorf("expected indented JSON, got %s", string(data))
	}
}

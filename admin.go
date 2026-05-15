package orc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// AdminOption configures the AdminHandler.
type AdminOption func(*adminOptions)

type adminOptions struct {
	// MaxScanLimit caps how many rows are scanned in-memory when building
	// workflow trees / locating children by parent id. Defaults to 5000.
	MaxScanLimit int

	// DefaultListLimit is the default page size for GET /workflows when no
	// `limit` query param is supplied. Defaults to 100.
	DefaultListLimit int

	// MaxListLimit is the upper bound on `limit` for GET /workflows.
	// Defaults to 1000.
	MaxListLimit int

	// PrettyJSON enables indented JSON responses.
	PrettyJSON bool
}

// WithAdminMaxScanLimit caps how many rows the admin tree/children endpoints
// will scan from the system DB when assembling parent/child relationships.
func WithAdminMaxScanLimit(n int) AdminOption {
	return func(o *adminOptions) {
		if n > 0 {
			o.MaxScanLimit = n
		}
	}
}

// WithAdminDefaultListLimit sets the default page size for GET /workflows.
func WithAdminDefaultListLimit(n int) AdminOption {
	return func(o *adminOptions) {
		if n > 0 {
			o.DefaultListLimit = n
		}
	}
}

// WithAdminMaxListLimit sets the maximum page size for GET /workflows.
func WithAdminMaxListLimit(n int) AdminOption {
	return func(o *adminOptions) {
		if n > 0 {
			o.MaxListLimit = n
		}
	}
}

// WithAdminPrettyJSON enables indented JSON responses (useful in browsers).
func WithAdminPrettyJSON() AdminOption {
	return func(o *adminOptions) { o.PrettyJSON = true }
}

// AdminWorkflowView is a wire-friendly projection of WorkflowStatus that
// also exposes helpful, computed fields like the running duration.
type AdminWorkflowView struct {
	ID                 string             `json:"workflow_id"`
	Name               string             `json:"name"`
	Status             WorkflowStatusType `json:"status"`
	QueueName          string             `json:"queue_name,omitempty"`
	ExecutorID         string             `json:"executor_id,omitempty"`
	ApplicationVersion string             `json:"application_version,omitempty"`
	ApplicationID      string             `json:"application_id,omitempty"`
	DeduplicationID    string             `json:"deduplication_id,omitempty"`
	Priority           int                `json:"priority,omitempty"`
	Attempts           int                `json:"attempts"`
	ParentWorkflowID   string             `json:"parent_workflow_id,omitempty"`
	ForkedFrom         string             `json:"forked_from,omitempty"`
	CronSchedule       string             `json:"cron_schedule,omitempty"`

	TimeoutMs    int64     `json:"timeout_ms,omitempty"`
	Deadline     time.Time `json:"deadline,omitempty"`
	DelayUntil   time.Time `json:"delay_until,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`

	// Computed fields:

	// DurationMs is the wall-clock time the workflow has been running, or
	// (for terminal workflows) the time spent between StartedAt and
	// UpdatedAt. Zero for workflows that have not started yet.
	DurationMs int64 `json:"duration_ms"`
	// DurationHuman is a short human-readable rendering of DurationMs
	// (e.g. "1m32s", "3.2s", "750ms").
	DurationHuman string `json:"duration_human"`
	// AgeMs is the wall-clock time since the workflow was created.
	AgeMs int64 `json:"age_ms"`
	// AgeHuman is a short human-readable rendering of AgeMs.
	AgeHuman string `json:"age_human"`
	// IsTerminal reports whether the workflow's status is terminal.
	IsTerminal bool `json:"is_terminal"`
	// IsRunning reports whether the workflow is actively executing
	// (PENDING) right now.
	IsRunning bool `json:"is_running"`

	Input  any    `json:"input,omitempty"`
	Output any    `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

// AdminStepView is a wire-friendly projection of StepInfo.
type AdminStepView struct {
	StepID          int       `json:"step_id"`
	StepName        string    `json:"step_name"`
	ChildWorkflowID string    `json:"child_workflow_id,omitempty"`
	Output          any       `json:"output,omitempty"`
	Error           string    `json:"error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

// AdminWorkflowDetail is the response for GET /workflows/{id}.
type AdminWorkflowDetail struct {
	AdminWorkflowView
	Steps    []AdminStepView     `json:"steps"`
	Children []AdminWorkflowView `json:"children"`
}

// AdminWorkflowTreeNode is a node in a parent/child workflow tree.
type AdminWorkflowTreeNode struct {
	AdminWorkflowView
	Children []*AdminWorkflowTreeNode `json:"children,omitempty"`
}

// AdminQueueView describes a registered queue.
type AdminQueueView struct {
	Name              string       `json:"name"`
	WorkerConcurrency int          `json:"worker_concurrency,omitempty"`
	GlobalConcurrency int          `json:"global_concurrency,omitempty"`
	WithPriority      bool         `json:"with_priority,omitempty"`
	RateLimiter       *RateLimiter `json:"rate_limiter,omitempty"`

	// Pending counts the workflows currently ENQUEUED on this queue.
	Pending int `json:"pending"`
	// Running counts the PENDING workflows attached to this queue
	// (i.e. actively executing).
	Running int `json:"running"`
}

// AdminInfo is the response for GET /info.
type AdminInfo struct {
	AppName            string           `json:"app_name"`
	ApplicationVersion string           `json:"application_version"`
	ExecutorID         string           `json:"executor_id"`
	DatabasePath       string           `json:"database_path,omitempty"`
	RegisteredCount    int              `json:"registered_count"`
	Registered         []string         `json:"registered_workflows"`
	Queues             []AdminQueueView `json:"queues"`
	StatusCounts       map[string]int   `json:"status_counts"`
	Now                time.Time        `json:"now"`
}

// AdminListResponse is the response for GET /workflows.
//
// The pagination model is offset/limit-based:
//
//   - Limit / Offset are echoed back from the request (after defaulting and
//     clamping).
//   - Count is the number of items returned in this batch (i.e. len(Items)).
//   - Total is the number of rows matching the request's filters across all
//     pages. Use it to compute the page count as ceil(Total / Limit).
//   - Page and PageSize are convenience fields derived from Offset and Limit
//     (Page is 1-based) so clients don't have to re-compute them.
type AdminListResponse struct {
	Items    []AdminWorkflowView `json:"items"`
	Limit    int                 `json:"limit"`
	Offset   int                 `json:"offset"`
	Count    int                 `json:"count"`
	Total    int                 `json:"total"`
	Page     int                 `json:"page"`
	PageSize int                 `json:"page_size"`
}

// AdminForkRequest is the request body for POST /workflows/{id}/fork.
type AdminForkRequest struct {
	StartFromStep int    `json:"start_from_step"`
	NewWorkflowID string `json:"new_workflow_id,omitempty"`
}

// AdminForkResponse is the response body for POST /workflows/{id}/fork.
type AdminForkResponse struct {
	NewWorkflowID string `json:"new_workflow_id"`
}

// AdminDeleteRequest is the request body for POST /workflows/delete.
type AdminDeleteRequest struct {
	IDs []string `json:"ids"`
}

// AdminDeleteResponse is the response body for delete operations.
type AdminDeleteResponse struct {
	Deleted []string `json:"deleted"`
}

// AdminGCRequest is the request body for POST /workflows/gc.
//
// Statuses must be non-empty. UpdatedBefore is required and accepts an
// RFC3339 timestamp; only workflows whose updated_at is strictly less than
// UpdatedBefore are considered. AllowNonTerminal opts in to GC-ing
// non-terminal statuses (PENDING / ENQUEUED / DELAYED) — off by default
// so a runaway request can't wipe live workflows.
type AdminGCRequest struct {
	Statuses         []WorkflowStatusType `json:"statuses"`
	UpdatedBefore    time.Time            `json:"updated_before"`
	AllowNonTerminal bool                 `json:"allow_non_terminal,omitempty"`
}

// AdminGCResponse is the response body for POST /workflows/gc, mirroring
// orc.GCWorkflowsResult.
type AdminGCResponse struct {
	Deleted int      `json:"deleted"`
	Failed  int      `json:"failed"`
	Errors  []string `json:"errors,omitempty"`
}

// adminError is the response body for any non-2xx outcome.
type adminError struct {
	Error string `json:"error"`
	Code  int    `json:"code,omitempty"`
}

// AdminHandler returns an http.Handler that exposes a JSON admin API for
// the supplied ORC Context. Authentication and authorization are out of
// scope; protect the handler at a higher layer (middleware, reverse proxy,
// internal-only listener, etc.) before exposing it externally.
//
// The handler uses the standard library's http.ServeMux (Go 1.22+ method
// matching) and is mountable under any prefix using http.StripPrefix. See
// the package README and examples/14-admin-http for an end-to-end example.
//
// All paths are relative to the handler's root. Mount it at "/admin/" and
// routes become "/admin/workflows", "/admin/workflows/{id}", etc.
//
//	mux := http.NewServeMux()
//	mux.Handle("/admin/", http.StripPrefix("/admin", orc.AdminHandler(ctx)))
//	http.ListenAndServe(":8080", mux)
func AdminHandler(c *Context, opts ...AdminOption) http.Handler {
	o := &adminOptions{
		MaxScanLimit:     5000,
		DefaultListLimit: 100,
		MaxListLimit:     1000,
	}
	for _, opt := range opts {
		opt(o)
	}

	a := &adminAPI{ctx: c, opts: o}

	mux := http.NewServeMux()

	// Discovery / health.
	mux.HandleFunc("GET /", a.handleIndex)
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /info", a.handleInfo)

	// Listing.
	mux.HandleFunc("GET /workflows", a.handleListWorkflows)
	mux.HandleFunc("POST /workflows/delete", a.handleBulkDelete)
	mux.HandleFunc("POST /workflows/gc", a.handleGC)

	// Per-workflow.
	mux.HandleFunc("GET /workflows/{id}", a.handleGetWorkflow)
	mux.HandleFunc("DELETE /workflows/{id}", a.handleDeleteWorkflow)
	mux.HandleFunc("GET /workflows/{id}/steps", a.handleListSteps)
	mux.HandleFunc("GET /workflows/{id}/children", a.handleListChildren)
	mux.HandleFunc("GET /workflows/{id}/tree", a.handleTree)
	mux.HandleFunc("POST /workflows/{id}/cancel", a.handleCancel)
	mux.HandleFunc("POST /workflows/{id}/resume", a.handleResume)
	mux.HandleFunc("POST /workflows/{id}/fork", a.handleFork)

	// Misc.
	mux.HandleFunc("GET /queues", a.handleListQueues)
	mux.HandleFunc("GET /registered", a.handleListRegistered)

	return mux
}

// ----------------------------------------------------------------------------
// implementation
// ----------------------------------------------------------------------------

type adminAPI struct {
	ctx  *Context
	opts *adminOptions
}

func (a *adminAPI) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	if a.opts.PrettyJSON {
		enc.SetIndent("", "  ")
	}
	_ = enc.Encode(v)
}

func (a *adminAPI) writeError(w http.ResponseWriter, status int, err error) {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}
	a.writeJSON(w, status, adminError{Error: msg, Code: status})
}

func (a *adminAPI) handleIndex(w http.ResponseWriter, r *http.Request) {
	type route struct {
		Method      string `json:"method"`
		Path        string `json:"path"`
		Description string `json:"description"`
	}
	routes := []route{
		{"GET", "/health", "liveness probe"},
		{"GET", "/info", "executor info, registered workflows, queue summaries, status counts"},
		{"GET", "/workflows", "list workflows (filters: status, name, queue, executor, id, start, end; pagination: limit, offset, page; sort: desc; payload: load_io). Response includes page, page_size and total."},
		{"GET", "/workflows/{id}", "workflow detail (status + duration + steps + direct children)"},
		{"DELETE", "/workflows/{id}", "delete a single workflow and dependent rows"},
		{"GET", "/workflows/{id}/steps", "list checkpointed steps"},
		{"GET", "/workflows/{id}/children", "direct child workflows spawned by this workflow"},
		{"GET", "/workflows/{id}/tree", "workflow + descendants as a tree (?depth=N to limit)"},
		{"POST", "/workflows/{id}/cancel", "cancel a workflow"},
		{"POST", "/workflows/{id}/resume", "resume a failed/cancelled workflow"},
		{"POST", "/workflows/{id}/fork", "fork from a step (body: {start_from_step, new_workflow_id})"},
		{"POST", "/workflows/delete", "bulk delete (body: {ids: [...]})"},
		{"POST", "/workflows/gc", "garbage-collect: delete rows in given statuses with updated_at < cutoff (body: {statuses: [...], updated_before: \"RFC3339\", allow_non_terminal?: bool})"},
		{"GET", "/queues", "registered queues + pending/running counts"},
		{"GET", "/registered", "registered workflow names"},
	}
	a.writeJSON(w, http.StatusOK, map[string]any{
		"name":   "orc admin api",
		"routes": routes,
	})
}

func (a *adminAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	if a.ctx == nil || a.ctx.systemDB == nil {
		a.writeError(w, http.StatusServiceUnavailable, errors.New("orc context not initialized"))
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"now":    time.Now().UTC(),
	})
}

func (a *adminAPI) handleInfo(w http.ResponseWriter, r *http.Request) {
	c := a.ctx

	registered := dedupeStrings(ListRegisteredWorkflows(c))
	sort.Strings(registered)

	queues := a.queueViews()

	// Aggregate status counts. We intentionally use a small per-status query
	// rather than fetching every workflow from the database.
	statuses := []WorkflowStatusType{
		WorkflowStatusPending,
		WorkflowStatusEnqueued,
		WorkflowStatusDelayed,
		WorkflowStatusSuccess,
		WorkflowStatusError,
		WorkflowStatusCancelled,
		WorkflowStatusMaxRecoveryAttemptsExceeded,
	}
	counts := make(map[string]int, len(statuses))
	for _, s := range statuses {
		ws, err := c.systemDB.listWorkflows(c.ctx, listWorkflowsInput{
			Status: []WorkflowStatusType{s},
			Limit:  a.opts.MaxScanLimit,
		})
		if err != nil {
			a.writeError(w, http.StatusInternalServerError, err)
			return
		}
		counts[string(s)] = len(ws)
	}

	a.writeJSON(w, http.StatusOK, AdminInfo{
		AppName:            c.cfg.AppName,
		ApplicationVersion: c.cfg.ApplicationVersion,
		ExecutorID:         c.cfg.ExecutorID,
		DatabasePath:       c.cfg.DatabasePath,
		RegisteredCount:    len(registered),
		Registered:         registered,
		Queues:             queues,
		StatusCounts:       counts,
		Now:                time.Now().UTC(),
	})
}

func (a *adminAPI) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	in, httpErr := a.parseListInput(r)
	if httpErr != nil {
		a.writeError(w, http.StatusBadRequest, httpErr)
		return
	}
	total, err := a.ctx.systemDB.countWorkflows(a.ctx.ctx, in)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	ws, err := a.ctx.systemDB.listWorkflows(a.ctx.ctx, in)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	now := time.Now()
	items := make([]AdminWorkflowView, 0, len(ws))
	for i := range ws {
		items = append(items, toView(&ws[i], now))
	}
	page := 1
	if in.Limit > 0 {
		page = (in.Offset / in.Limit) + 1
	}
	a.writeJSON(w, http.StatusOK, AdminListResponse{
		Items:    items,
		Limit:    in.Limit,
		Offset:   in.Offset,
		Count:    len(items),
		Total:    total,
		Page:     page,
		PageSize: in.Limit,
	})
}

func (a *adminAPI) parseListInput(r *http.Request) (listWorkflowsInput, error) {
	q := r.URL.Query()

	limit := a.opts.DefaultListLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return listWorkflowsInput{}, fmt.Errorf("invalid limit: %q", v)
		}
		if n > a.opts.MaxListLimit {
			n = a.opts.MaxListLimit
		}
		limit = n
	}

	offset := 0
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return listWorkflowsInput{}, fmt.Errorf("invalid offset: %q", v)
		}
		offset = n
	}
	// `page` is a 1-based convenience that overrides `offset` when present.
	// Pages of size `limit` are computed as offset = (page-1) * limit.
	if v := q.Get("page"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil || p < 1 {
			return listWorkflowsInput{}, fmt.Errorf("invalid page: %q", v)
		}
		offset = (p - 1) * limit
	}

	in := listWorkflowsInput{
		Limit:           limit,
		Offset:          offset,
		LoadInputOutput: parseBool(q.Get("load_io"), true),
		WorkflowName:    q.Get("name"),
		QueueName:       q.Get("queue"),
		ExecutorID:      q.Get("executor"),
		WorkflowIDs:     q["id"],
		SortDescending:  parseBool(q.Get("desc"), false),
	}
	for _, s := range q["status"] {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		in.Status = append(in.Status, WorkflowStatusType(strings.ToUpper(s)))
	}
	if v := q.Get("start"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return listWorkflowsInput{}, fmt.Errorf("invalid start (RFC3339): %q", v)
		}
		in.StartTime = t
	}
	if v := q.Get("end"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return listWorkflowsInput{}, fmt.Errorf("invalid end (RFC3339): %q", v)
		}
		in.EndTime = t
	}
	return in, nil
}

func parseBool(v string, dflt bool) bool {
	if v == "" {
		return dflt
	}
	switch strings.ToLower(v) {
	case "1", "t", "true", "y", "yes":
		return true
	case "0", "f", "false", "n", "no":
		return false
	}
	return dflt
}

func (a *adminAPI) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st, err := a.ctx.systemDB.getWorkflowStatus(a.ctx.ctx, id, true)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	if st == nil {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	stepInfos, _ := a.ctx.systemDB.listSteps(a.ctx.ctx, id)
	steps := make([]AdminStepView, 0, len(stepInfos))
	for _, s := range stepInfos {
		steps = append(steps, toStepView(&s))
	}

	children, _ := a.findChildren(id)
	childViews := make([]AdminWorkflowView, 0, len(children))
	now := time.Now()
	for i := range children {
		childViews = append(childViews, toView(&children[i], now))
	}

	a.writeJSON(w, http.StatusOK, AdminWorkflowDetail{
		AdminWorkflowView: toView(st, now),
		Steps:             steps,
		Children:          childViews,
	})
}

func (a *adminAPI) handleListSteps(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	stepInfos, err := a.ctx.systemDB.listSteps(a.ctx.ctx, id)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]AdminStepView, 0, len(stepInfos))
	for _, s := range stepInfos {
		out = append(out, toStepView(&s))
	}
	a.writeJSON(w, http.StatusOK, out)
}

func (a *adminAPI) handleListChildren(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	children, err := a.findChildren(id)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	now := time.Now()
	out := make([]AdminWorkflowView, 0, len(children))
	for i := range children {
		out = append(out, toView(&children[i], now))
	}
	a.writeJSON(w, http.StatusOK, out)
}

func (a *adminAPI) handleTree(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	st, err := a.ctx.systemDB.getWorkflowStatus(a.ctx.ctx, id, true)
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	if st == nil {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}

	maxDepth := -1
	if v := r.URL.Query().Get("depth"); v != "" {
		d, err := strconv.Atoi(v)
		if err != nil || d < 0 {
			a.writeError(w, http.StatusBadRequest, fmt.Errorf("invalid depth: %q", v))
			return
		}
		maxDepth = d
	}

	all, err := a.scanAllWorkflows()
	if err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	byParent := indexByParent(all)
	now := time.Now()
	root := buildTree(st, byParent, now, 0, maxDepth)
	a.writeJSON(w, http.StatusOK, root)
}

func (a *adminAPI) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	if err := CancelWorkflow(a.ctx, id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	st, _ := a.ctx.systemDB.getWorkflowStatus(a.ctx.ctx, id, false)
	if st == nil {
		a.writeJSON(w, http.StatusOK, map[string]any{"workflow_id": id, "cancelled": true})
		return
	}
	a.writeJSON(w, http.StatusOK, toView(st, time.Now()))
}

func (a *adminAPI) handleResume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	if _, err := ResumeWorkflow[any](a.ctx, id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	st, _ := a.ctx.systemDB.getWorkflowStatus(a.ctx.ctx, id, false)
	if st == nil {
		a.writeJSON(w, http.StatusOK, map[string]any{"workflow_id": id, "resumed": true})
		return
	}
	a.writeJSON(w, http.StatusOK, toView(st, time.Now()))
}

func (a *adminAPI) handleFork(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}

	var req AdminForkRequest
	if r.ContentLength != 0 {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			a.writeError(w, http.StatusBadRequest, err)
			return
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				a.writeError(w, http.StatusBadRequest, fmt.Errorf("invalid json body: %w", err))
				return
			}
		}
	}
	if req.StartFromStep < 0 {
		a.writeError(w, http.StatusBadRequest, errors.New("start_from_step must be >= 0"))
		return
	}

	h, err := ForkWorkflow[any](a.ctx, ForkWorkflowInput{
		OriginalWorkflowID: id,
		StartFromStep:      req.StartFromStep,
		NewWorkflowID:      req.NewWorkflowID,
	})
	if err != nil {
		if IsCode(err, ErrWorkflowNotFound) {
			a.writeError(w, http.StatusNotFound, err)
			return
		}
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	a.writeJSON(w, http.StatusOK, AdminForkResponse{NewWorkflowID: h.GetWorkflowID()})
}

func (a *adminAPI) handleDeleteWorkflow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok, err := a.workflowExists(id); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	} else if !ok {
		a.writeError(w, http.StatusNotFound, ErrWorkflowNotFoundErr)
		return
	}
	if err := DeleteWorkflows(a.ctx, []string{id}); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	a.writeJSON(w, http.StatusOK, AdminDeleteResponse{Deleted: []string{id}})
}

func (a *adminAPI) handleBulkDelete(w http.ResponseWriter, r *http.Request) {
	var req AdminDeleteRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(body) == 0 {
		a.writeError(w, http.StatusBadRequest, errors.New("empty request body"))
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, fmt.Errorf("invalid json body: %w", err))
		return
	}
	if len(req.IDs) == 0 {
		a.writeError(w, http.StatusBadRequest, errors.New("ids must be a non-empty array"))
		return
	}
	if err := DeleteWorkflows(a.ctx, req.IDs); err != nil {
		a.writeError(w, http.StatusInternalServerError, err)
		return
	}
	a.writeJSON(w, http.StatusOK, AdminDeleteResponse{Deleted: req.IDs})
}

func (a *adminAPI) handleGC(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(body) == 0 {
		a.writeError(w, http.StatusBadRequest, errors.New("empty request body"))
		return
	}
	var req AdminGCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, fmt.Errorf("invalid json body: %w", err))
		return
	}
	// Normalise statuses (uppercase + trim) before handing off to
	// GCWorkflows so users can send "success" lowercase if they like.
	statuses := make([]WorkflowStatusType, 0, len(req.Statuses))
	for _, s := range req.Statuses {
		t := strings.TrimSpace(strings.ToUpper(string(s)))
		if t == "" {
			continue
		}
		statuses = append(statuses, WorkflowStatusType(t))
	}
	if len(statuses) == 0 {
		a.writeError(w, http.StatusBadRequest, errors.New("statuses must be a non-empty array"))
		return
	}
	if req.UpdatedBefore.IsZero() {
		a.writeError(w, http.StatusBadRequest, errors.New("updated_before is required (RFC3339 timestamp)"))
		return
	}
	res, err := GCWorkflows(a.ctx, GCWorkflowsInput{
		Statuses:         statuses,
		UpdatedBefore:    req.UpdatedBefore,
		AllowNonTerminal: req.AllowNonTerminal,
	})
	if err != nil {
		// Validation errors from the public API surface as 400; anything
		// else (e.g. a true I/O failure) bubbles up as 500.
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusOK, AdminGCResponse{
		Deleted: res.Deleted,
		Failed:  res.Failed,
		Errors:  res.Errors,
	})
}

func (a *adminAPI) handleListQueues(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(w, http.StatusOK, a.queueViews())
}

func (a *adminAPI) handleListRegistered(w http.ResponseWriter, r *http.Request) {
	names := dedupeStrings(ListRegisteredWorkflows(a.ctx))
	sort.Strings(names)
	a.writeJSON(w, http.StatusOK, names)
}

func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

func (a *adminAPI) queueViews() []AdminQueueView {
	queues := ListQueues(a.ctx)
	out := make([]AdminQueueView, 0, len(queues))
	for _, q := range queues {
		v := AdminQueueView{
			Name:              q.Name,
			WorkerConcurrency: q.WorkerConcurrency,
			GlobalConcurrency: q.GlobalConcurrency,
			WithPriority:      q.WithPriority,
			RateLimiter:       q.RateLimiter,
		}
		if pending, err := a.ctx.systemDB.listWorkflows(a.ctx.ctx, listWorkflowsInput{
			QueueName: q.Name,
			Status:    []WorkflowStatusType{WorkflowStatusEnqueued},
			Limit:     a.opts.MaxScanLimit,
		}); err == nil {
			v.Pending = len(pending)
		}
		if running, err := a.ctx.systemDB.listWorkflows(a.ctx.ctx, listWorkflowsInput{
			QueueName: q.Name,
			Status:    []WorkflowStatusType{WorkflowStatusPending},
			Limit:     a.opts.MaxScanLimit,
		}); err == nil {
			v.Running = len(running)
		}
		out = append(out, v)
	}
	return out
}

func (a *adminAPI) workflowExists(id string) (*WorkflowStatus, bool, error) {
	st, err := a.ctx.systemDB.getWorkflowStatus(a.ctx.ctx, id, false)
	if err != nil {
		return nil, false, err
	}
	if st == nil {
		return nil, false, nil
	}
	return st, true, nil
}

func (a *adminAPI) findChildren(parentID string) ([]WorkflowStatus, error) {
	all, err := a.scanAllWorkflows()
	if err != nil {
		return nil, err
	}
	out := make([]WorkflowStatus, 0)
	for i := range all {
		if all[i].ParentWorkflowID == parentID {
			out = append(out, all[i])
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (a *adminAPI) scanAllWorkflows() ([]WorkflowStatus, error) {
	return a.ctx.systemDB.listWorkflows(a.ctx.ctx, listWorkflowsInput{
		Limit:           a.opts.MaxScanLimit,
		LoadInputOutput: false,
	})
}

func indexByParent(all []WorkflowStatus) map[string][]*WorkflowStatus {
	out := make(map[string][]*WorkflowStatus)
	for i := range all {
		p := all[i].ParentWorkflowID
		if p == "" {
			continue
		}
		out[p] = append(out[p], &all[i])
	}
	for k := range out {
		kids := out[k]
		sort.Slice(kids, func(i, j int) bool { return kids[i].CreatedAt.Before(kids[j].CreatedAt) })
	}
	return out
}

func buildTree(root *WorkflowStatus, byParent map[string][]*WorkflowStatus, now time.Time, depth, maxDepth int) *AdminWorkflowTreeNode {
	node := &AdminWorkflowTreeNode{AdminWorkflowView: toView(root, now)}
	if maxDepth >= 0 && depth >= maxDepth {
		return node
	}
	kids := byParent[root.ID]
	if len(kids) == 0 {
		return node
	}
	for _, k := range kids {
		node.Children = append(node.Children, buildTree(k, byParent, now, depth+1, maxDepth))
	}
	return node
}

func toView(st *WorkflowStatus, now time.Time) AdminWorkflowView {
	v := AdminWorkflowView{
		ID:                 st.ID,
		Name:               st.Name,
		Status:             st.Status,
		QueueName:          st.QueueName,
		ExecutorID:         st.ExecutorID,
		ApplicationVersion: st.ApplicationVersion,
		ApplicationID:      st.ApplicationID,
		DeduplicationID:    st.DeduplicationID,
		Priority:           st.Priority,
		Attempts:           st.Attempts,
		ParentWorkflowID:   st.ParentWorkflowID,
		ForkedFrom:         st.ForkedFrom,
		CronSchedule:       st.CronSchedule,
		Deadline:           st.Deadline,
		DelayUntil:         st.DelayUntil,
		CreatedAt:          st.CreatedAt,
		StartedAt:          st.StartedAt,
		UpdatedAt:          st.UpdatedAt,
		Input:              st.Input,
		Output:             st.Output,
		IsTerminal:         st.Status.IsTerminal(),
		IsRunning:          st.Status == WorkflowStatusPending,
	}
	if st.Timeout > 0 {
		v.TimeoutMs = st.Timeout.Milliseconds()
	}
	if st.Error != nil {
		v.Error = st.Error.Error()
	}

	// Duration: from StartedAt to UpdatedAt for terminal rows, else to now.
	var dur time.Duration
	if !st.StartedAt.IsZero() {
		end := now
		if st.Status.IsTerminal() && !st.UpdatedAt.IsZero() && st.UpdatedAt.After(st.StartedAt) {
			end = st.UpdatedAt
		}
		if end.After(st.StartedAt) {
			dur = end.Sub(st.StartedAt)
		}
	}
	v.DurationMs = dur.Milliseconds()
	v.DurationHuman = humanizeDuration(dur)

	if !st.CreatedAt.IsZero() {
		age := now.Sub(st.CreatedAt)
		if age < 0 {
			age = 0
		}
		v.AgeMs = age.Milliseconds()
		v.AgeHuman = humanizeDuration(age)
	}
	return v
}

func toStepView(s *StepInfo) AdminStepView {
	v := AdminStepView{
		StepID:          s.StepID,
		StepName:        s.StepName,
		ChildWorkflowID: s.ChildWorkflowID,
		Output:          s.Output,
		CreatedAt:       s.CreatedAt,
	}
	if s.Error != nil {
		v.Error = s.Error.Error()
	}
	return v
}

// humanizeDuration renders d as a short human-readable string. Examples:
//
//	0           -> "0s"
//	750ms       -> "750ms"
//	1.5s        -> "1.5s"
//	72s         -> "1m12s"
//	3825s       -> "1h3m45s"
//	86400s + x  -> "1d2h"
func humanizeDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		s := d.Seconds()
		return fmt.Sprintf("%.1fs", s)
	}
	if d < time.Hour {
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		return fmt.Sprintf("%dm%ds", m, s)
	}
	if d < 24*time.Hour {
		h := int(d / time.Hour)
		m := int((d % time.Hour) / time.Minute)
		s := int((d % time.Minute) / time.Second)
		return fmt.Sprintf("%dh%dm%ds", h, m, s)
	}
	days := int(d / (24 * time.Hour))
	rem := d % (24 * time.Hour)
	h := int(rem / time.Hour)
	return fmt.Sprintf("%dd%dh", days, h)
}

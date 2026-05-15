package orc

import (
	"errors"
	"fmt"
	"time"
)

// ListWorkflowsOption configures ListWorkflows.
type ListWorkflowsOption func(*listWorkflowsInput)

// WithListWorkflowName filters by workflow function name.
func WithListWorkflowName(n string) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.WorkflowName = n }
}

// WithListWorkflowStatus filters by status. Multiple statuses are OR'd.
func WithListWorkflowStatus(s ...WorkflowStatusType) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.Status = append(in.Status, s...) }
}

// WithListWorkflowQueue filters by queue.
func WithListWorkflowQueue(q string) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.QueueName = q }
}

// WithListWorkflowIDs returns only workflows with the given IDs.
func WithListWorkflowIDs(ids ...string) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.WorkflowIDs = append(in.WorkflowIDs, ids...) }
}

// WithListWorkflowExecutor filters by executor id.
func WithListWorkflowExecutor(id string) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.ExecutorID = id }
}

// WithListWorkflowLimit caps the number of returned rows.
func WithListWorkflowLimit(n int) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.Limit = n }
}

// WithListWorkflowOffset paginates results.
func WithListWorkflowOffset(n int) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.Offset = n }
}

// WithListWorkflowTimeRange filters by creation time.
func WithListWorkflowTimeRange(start, end time.Time) ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.StartTime = start; in.EndTime = end }
}

// WithListWorkflowSortDescending toggles sort order.
func WithListWorkflowSortDescending() ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.SortDescending = true }
}

// WithListWorkflowLoadIO loads inputs and outputs (slower for big payloads).
func WithListWorkflowLoadIO() ListWorkflowsOption {
	return func(in *listWorkflowsInput) { in.LoadInputOutput = true }
}

// ListWorkflows returns workflow statuses matching the supplied filters.
func ListWorkflows(c *Context, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	in := listWorkflowsInput{LoadInputOutput: true}
	for _, opt := range opts {
		opt(&in)
	}
	return c.systemDB.listWorkflows(c.ctx, in)
}

// RetrieveWorkflow returns a polling handle for an existing workflow.
func RetrieveWorkflow[O any](c *Context, workflowID string) (WorkflowHandle[O], error) {
	st, err := c.systemDB.getWorkflowStatus(c.ctx, workflowID, false)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, ErrWorkflowNotFoundErr
	}
	return &pollingHandle[O]{id: workflowID, ctx: c}, nil
}

// CancelWorkflow marks a workflow CANCELLED in the database and, if the
// workflow is currently running in this process, proactively cancels its
// per-workflow context with cause ErrWorkflowCancelled. Steps that honor
// ctx.Done() (e.g. Sleep, Recv with timeout, custom HTTP calls) will return
// promptly. For workflows running on a different executor, the remote
// cancel poller will pick up the status change and cancel locally.
func CancelWorkflow(c *Context, workflowID string) error {
	if err := c.systemDB.cancelWorkflow(c.ctx, workflowID); err != nil {
		return err
	}
	if v, ok := c.core.active.Load(workflowID); ok {
		if aw, ok := v.(*activeWorkflow); ok && aw != nil && aw.cancel != nil {
			aw.cancel(ErrWorkflowCancelledErr)
		}
	}
	return nil
}

// ResumeWorkflowOption configures ResumeWorkflow.
type ResumeWorkflowOption func()

// ResumeWorkflow re-enqueues a previously failed/cancelled workflow.
func ResumeWorkflow[O any](c *Context, workflowID string, _ ...ResumeWorkflowOption) (WorkflowHandle[O], error) {
	if err := c.systemDB.resumeWorkflow(c.ctx, workflowID); err != nil {
		return nil, err
	}
	return &pollingHandle[O]{id: workflowID, ctx: c}, nil
}

// ForkWorkflowInput configures ForkWorkflow.
type ForkWorkflowInput struct {
	OriginalWorkflowID string
	StartFromStep      int
	NewWorkflowID      string
}

// ForkWorkflow creates a new workflow that re-uses the step outputs from the
// original up to (but not including) StartFromStep, and re-runs from there.
func ForkWorkflow[O any](c *Context, in ForkWorkflowInput) (WorkflowHandle[O], error) {
	id, err := c.systemDB.forkWorkflow(c.ctx, forkWorkflowInput{
		OriginalWorkflowID: in.OriginalWorkflowID,
		StartFromStep:      in.StartFromStep,
		NewWorkflowID:      in.NewWorkflowID,
	})
	if err != nil {
		return nil, err
	}
	return &pollingHandle[O]{id: id, ctx: c}, nil
}

// DeleteWorkflows hard-deletes the named workflows and all dependent rows
// (steps, notifications, events).
func DeleteWorkflows(c *Context, ids []string) error {
	return c.systemDB.deleteWorkflows(c.ctx, ids)
}

// GCWorkflowsInput configures GCWorkflows.
//
// At least one Status must be supplied and UpdatedBefore must be set; both
// safety rails are enforced server-side. By default only terminal statuses
// are accepted (SUCCESS, ERROR, CANCELLED, MAX_RECOVERY_ATTEMPTS_EXCEEDED) so
// you can never accidentally delete a workflow that is still running. Set
// AllowNonTerminal to opt-in to deleting non-terminal rows (PENDING,
// ENQUEUED, DELAYED) — useful for cleaning up zombie / abandoned executions.
type GCWorkflowsInput struct {
	Statuses         []WorkflowStatusType
	UpdatedBefore    time.Time
	AllowNonTerminal bool
}

// GCWorkflowsResult is the outcome of a GCWorkflows run.
//
// Deleted is the number of workflow_status rows that were actually removed
// (cascading deletes of operation_outputs / notifications / workflow_events
// happen inside the DB and are not counted).
//
// Failed is the number of rows that matched the filter but could not be
// removed because the per-status DELETE returned an error. The
// corresponding error messages are appended to Errors.
type GCWorkflowsResult struct {
	Deleted int      `json:"deleted"`
	Failed  int      `json:"failed"`
	Errors  []string `json:"errors,omitempty"`
}

// GCWorkflows hard-deletes workflows matching one or more Statuses whose
// updated_at is strictly before UpdatedBefore. Cascade deletes
// (operation_outputs, notifications, workflow_events) happen via the
// schema's ON DELETE CASCADE, so it's a complete cleanup.
//
// GC runs each requested status in its own DELETE statement so a partial
// failure (e.g. one status fails to delete) does not block the others.
// The returned GCWorkflowsResult tallies deleted vs failed row counts and
// surfaces per-status errors in Errors.
//
// Use this to keep the database compact: e.g. a daily cron that calls
//
//	orc.GCWorkflows(ctx, orc.GCWorkflowsInput{
//	    Statuses:      []orc.WorkflowStatusType{orc.WorkflowStatusSuccess},
//	    UpdatedBefore: time.Now().Add(-7 * 24 * time.Hour),
//	})
//
// will purge all SUCCESS workflows older than a week.
func GCWorkflows(c *Context, in GCWorkflowsInput) (GCWorkflowsResult, error) {
	var res GCWorkflowsResult
	if len(in.Statuses) == 0 {
		return res, errors.New("orc: GCWorkflows requires at least one status")
	}
	if in.UpdatedBefore.IsZero() {
		return res, errors.New("orc: GCWorkflows requires UpdatedBefore")
	}
	// De-duplicate statuses; reject non-terminal ones unless explicitly
	// allowed. Doing both here means the systemDB layer never has to worry
	// about safety semantics.
	seen := make(map[WorkflowStatusType]struct{}, len(in.Statuses))
	statuses := make([]WorkflowStatusType, 0, len(in.Statuses))
	for _, s := range in.Statuses {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		if !in.AllowNonTerminal && !s.IsTerminal() {
			return res, fmt.Errorf("orc: GCWorkflows refuses non-terminal status %q (set AllowNonTerminal=true to override)", s)
		}
		seen[s] = struct{}{}
		statuses = append(statuses, s)
	}
	if len(statuses) == 0 {
		return res, errors.New("orc: GCWorkflows requires at least one valid status")
	}

	for _, s := range statuses {
		filter := listWorkflowsInput{
			Status:        []WorkflowStatusType{s},
			UpdatedBefore: in.UpdatedBefore,
		}
		deleted, err := c.systemDB.gcWorkflows(c.ctx, filter)
		if err != nil {
			// Best-effort: count how many rows matched so we can report
			// "this many failed to delete" rather than a vague error.
			cnt, cntErr := c.systemDB.countWorkflows(c.ctx, filter)
			if cntErr != nil {
				cnt = 0
			}
			res.Failed += cnt
			res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", s, err))
			continue
		}
		res.Deleted += deleted
	}
	return res, nil
}

// GetWorkflowSteps returns the recorded checkpointed steps for a workflow.
func GetWorkflowSteps(c *Context, workflowID string) ([]StepInfo, error) {
	return c.systemDB.listSteps(c.ctx, workflowID)
}

// ListRegisteredWorkflows enumerates all workflows registered on this context.
func ListRegisteredWorkflows(c *Context) []string {
	out := make([]string, 0)
	for _, e := range c.registry.all() {
		out = append(out, e.Name)
	}
	return out
}

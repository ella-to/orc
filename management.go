package orc

import "time"

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

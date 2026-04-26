package orc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"
)

// WorkflowOption configures a single workflow run.
type WorkflowOption func(*runWorkflowOptions)

type runWorkflowOptions struct {
	workflowID      string
	queueName       string
	deduplicationID string
	priority        int
	timeout         time.Duration
	deadline        time.Time
	parentID        string
	delay           time.Duration
}

// WithWorkflowID assigns an explicit, deterministic workflow ID. If a row
// with this ID already exists (with a matching name and input) the existing
// run is returned, providing exactly-once semantics.
func WithWorkflowID(id string) WorkflowOption {
	return func(o *runWorkflowOptions) { o.workflowID = id }
}

// WithQueue enqueues the workflow on the named queue rather than running it
// immediately in this process.
func WithQueue(name string) WorkflowOption {
	return func(o *runWorkflowOptions) { o.queueName = name }
}

// WithDeduplicationID guarantees only one workflow with this ID can be
// enqueued onto a given queue at a time.
func WithDeduplicationID(id string) WorkflowOption {
	return func(o *runWorkflowOptions) { o.deduplicationID = id }
}

// WithPriority sets the queue priority (lower = higher priority).
func WithPriority(p int) WorkflowOption {
	return func(o *runWorkflowOptions) { o.priority = p }
}

// WithWorkflowTimeout sets a durable timeout: if the workflow has not
// completed by then, it will be cancelled.
func WithWorkflowTimeout(d time.Duration) WorkflowOption {
	return func(o *runWorkflowOptions) { o.timeout = d }
}

// WithWorkflowDelay delays an enqueued workflow's first dispatch.
func WithWorkflowDelay(d time.Duration) WorkflowOption {
	return func(o *runWorkflowOptions) { o.delay = d }
}

// runOptions resolves to a runWorkflowOptions value.
func resolveOptions(opts ...WorkflowOption) *runWorkflowOptions {
	o := &runWorkflowOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// RunWorkflow starts (or attaches to) a durable workflow execution.
//
// If WithQueue is supplied the workflow is enqueued for background
// processing; otherwise it is started immediately in a new goroutine.
//
// The returned WorkflowHandle is typed by R, the workflow's output type.
func RunWorkflow[I any, O any](c *Context, fn any, input I, opts ...WorkflowOption) (WorkflowHandle[O], error) {
	if c == nil {
		return nil, errors.New("orc: nil context")
	}
	if !c.core.launched.Load() {
		return nil, newError(ErrInitialization, "Launch() not called")
	}

	opt := resolveOptions(opts...)
	name := fqn(fn)
	entry, ok := c.registry.get(name)
	if !ok {
		return nil, wrapError(ErrWorkflowNotRegistered, nil, "workflow %s not registered", name)
	}

	// Capture parent workflow id if we're inside one.
	if st := wfStateFromContext(c.ctx); st != nil {
		opt.parentID = st.workflowID
	}

	wfID := opt.workflowID
	if wfID == "" {
		wfID = newUUID()
	}

	// Encode input and persist initial row.
	inputStr, err := c.cfg.Serializer.Encode(input)
	if err != nil {
		return nil, err
	}

	status := WorkflowStatusEnqueued
	if opt.queueName == "" {
		status = WorkflowStatusEnqueued
	}
	if opt.delay > 0 {
		status = WorkflowStatusDelayed
	}

	now := time.Now().UTC()
	st := WorkflowStatus{
		ID:                 wfID,
		Status:             status,
		Name:               entry.Name,
		Input:              input,
		ExecutorID:         c.cfg.ExecutorID,
		ApplicationVersion: c.cfg.ApplicationVersion,
		QueueName:          opt.queueName,
		DeduplicationID:    opt.deduplicationID,
		Priority:           opt.priority,
		Timeout:            opt.timeout,
		ParentWorkflowID:   opt.parentID,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if !opt.deadline.IsZero() {
		st.Deadline = opt.deadline
	} else if opt.timeout > 0 {
		st.Deadline = now.Add(opt.timeout)
	}
	if opt.delay > 0 {
		st.DelayUntil = now.Add(opt.delay)
	}

	res, err := c.systemDB.insertWorkflow(c.ctx, insertWorkflowInput{Status: st})
	if err != nil {
		return nil, err
	}

	// If the workflow already existed for this ID, the dbos rule is:
	// - if same name + input -> return existing handle (idempotent)
	// - if different -> error.
	if res.AlreadyExisted {
		// Compare names. We can't easily compare deeply-typed inputs, but the
		// serialized form gives a strong-enough check.
		existingInputEnc, _ := c.cfg.Serializer.Encode(res.Status.Input)
		if res.Status.Name != entry.Name {
			return nil, wrapError(ErrConflictingInput, nil, "workflow %s already exists with different name (%s vs %s)", wfID, res.Status.Name, entry.Name)
		}
		if existingInputEnc != inputStr {
			return nil, wrapError(ErrConflictingInput, nil, "workflow %s already exists with different input", wfID)
		}
		// Return a polling handle since we're attaching to an existing run.
		return &pollingHandle[O]{id: wfID, ctx: c}, nil
	}

	// If queued, hand back a polling handle and let the queue runner pick it up.
	if opt.queueName != "" {
		return &pollingHandle[O]{id: wfID, ctx: c}, nil
	}

	// Otherwise execute right now in a new goroutine.
	resultCh := make(chan workflowOutcome, 1)
	wfCtx, cancel := context.WithCancelCause(c.ctx)
	c.core.active.Store(wfID, &activeWorkflow{resultCh: resultCh, cancel: cancel})
	c.core.workflowsWg.Add(1)
	go func() {
		defer c.core.workflowsWg.Done()
		defer c.core.active.Delete(wfID)
		defer cancel(nil)
		runWorkflowExecution(c, entry, wfID, inputStr, resultCh, wfCtx)
	}()

	return &directHandle[O]{id: wfID, ctx: c, result: resultCh}, nil
}

// runWorkflowExecution drives the workflow function with full durability:
// transitions PENDING, runs the wrapper, captures output/error, and posts
// the result.
//
// wfCtx is the per-workflow context.Context derived from the executor context.
// It can be cancelled via the activeWorkflow.cancel func to interrupt blocking
// operations inside steps.
func runWorkflowExecution(c *Context, entry *registryEntry, wfID, inputStr string, resultCh chan workflowOutcome, wfCtx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("orc: workflow panic: %v", r)
			c.logger.Error("workflow panicked", "workflow_id", wfID, "name", entry.Name, "panic", r)
			finalizeWorkflow(c, entry, wfID, "", err)
			select {
			case resultCh <- workflowOutcome{err: err}:
			default:
			}
			close(resultCh)
		}
	}()

	// Mark PENDING.
	now := time.Now().UTC()
	_ = c.systemDB.updateWorkflowStatus(c.ctx, updateWorkflowStatusInput{
		WorkflowID:   wfID,
		Status:       WorkflowStatusPending,
		BumpAttempts: true,
		StartedAtMs:  now.UnixMilli(),
		Now:          now,
	})

	// Per-workflow state tracker.
	st := &withinWorkflowState{workflowID: wfID, nextStep: 0}

	subCtx := wfCtx
	var cancel context.CancelFunc
	// Apply timeout if set on the row.
	rowSt, _ := c.systemDB.getWorkflowStatus(c.ctx, wfID, false)
	if rowSt != nil && rowSt.Timeout > 0 {
		dl := rowSt.StartedAt.Add(rowSt.Timeout)
		if !rowSt.Deadline.IsZero() {
			dl = rowSt.Deadline
		}
		subCtx, cancel = context.WithDeadline(wfCtx, dl)
		defer cancel()
	}

	wc := withWFState(c, subCtx, st)

	encOut, runErr := entry.Wrapper(wc, inputStr)

	// Translate context cancellation into the appropriate ORC error. When the
	// per-workflow context is cancelled with a cause (e.g. by CancelWorkflow),
	// surface that cause so callers see ErrWorkflowCancelled instead of a raw
	// context.Canceled.
	if runErr != nil && errors.Is(runErr, context.Canceled) {
		if cause := context.Cause(subCtx); cause != nil && cause != context.Canceled {
			runErr = cause
		} else if subCtx.Err() == context.DeadlineExceeded {
			runErr = ErrWorkflowTimedOutErr
		}
	}

	// If the workflow was explicitly cancelled (CancelWorkflow or remote
	// cancel poller cancelled wfCtx with ErrWorkflowCancelledErr) but the
	// wrapper swallowed the error and returned normally, honor the cancel.
	// Otherwise a workflow that does `_, _ = Sleep(c, ...)` could finish
	// "successfully" after a cancel.
	if runErr == nil {
		if cause := context.Cause(wfCtx); cause != nil && errors.Is(cause, ErrWorkflowCancelledErr) {
			runErr = ErrWorkflowCancelledErr
			encOut = ""
		}
	}

	finalizeWorkflow(c, entry, wfID, encOut, runErr)

	select {
	case resultCh <- workflowOutcome{output: decodedOrRaw(c, encOut), err: runErr}:
	default:
	}
	close(resultCh)
}

func decodedOrRaw(c *Context, encoded string) any {
	if encoded == "" {
		return nil
	}
	v, err := c.cfg.Serializer.Decode(encoded, nil)
	if err != nil {
		return encoded
	}
	return v
}

func finalizeWorkflow(c *Context, _ *registryEntry, wfID string, encOut string, runErr error) {
	in := updateWorkflowStatusInput{WorkflowID: wfID, Now: time.Now().UTC()}
	if runErr == nil {
		in.Status = WorkflowStatusSuccess
		o := encOut
		in.Output = &o
	} else {
		switch {
		case errors.Is(runErr, ErrWorkflowCancelledErr):
			in.Status = WorkflowStatusCancelled
		case errors.Is(runErr, ErrWorkflowTimedOutErr):
			in.Status = WorkflowStatusCancelled
			es := runErr.Error()
			in.ErrorString = &es
		default:
			in.Status = WorkflowStatusError
			es := runErr.Error()
			in.ErrorString = &es
		}
	}
	if err := c.systemDB.updateWorkflowStatus(c.ctx, in); err != nil {
		c.logger.Error("failed to finalize workflow", "workflow_id", wfID, "err", err)
	}
}

// ---------- Steps ----------

// StepOption configures a single RunAsStep call.
type StepOption func(*stepOptions)

type stepOptions struct {
	name        string
	maxRetries  int
	baseDelay   time.Duration
	maxDelay    time.Duration
	backoffMul  float64
}

// WithStepName overrides the step name (defaults to the function's FQN).
func WithStepName(name string) StepOption {
	return func(o *stepOptions) { o.name = name }
}

// WithStepMaxRetries enables retry-with-backoff for the step.
func WithStepMaxRetries(n int) StepOption {
	return func(o *stepOptions) { o.maxRetries = n }
}

// WithStepRetryBackoff configures the exponential-backoff parameters used
// when WithStepMaxRetries is set.
func WithStepRetryBackoff(base, max time.Duration, multiplier float64) StepOption {
	return func(o *stepOptions) {
		o.baseDelay = base
		o.maxDelay = max
		o.backoffMul = multiplier
	}
}

// StepFunc is the canonical step signature: a function that takes a Go
// context.Context and returns (T, error). Generic helpers in this file let
// you write strongly typed steps.
type StepFunc[T any] func(ctx context.Context) (T, error)

// RunAsStep executes a function as a durable step inside the current
// workflow. Subsequent re-executions of the workflow will skip steps whose
// outputs are already recorded in the database.
func RunAsStep[T any](c *Context, fn StepFunc[T], opts ...StepOption) (T, error) {
	st := wfStateFromContext(c.ctx)
	if st == nil {
		// allow steps to be invoked outside workflows for ergonomics
		return fn(c.ctx)
	}

	o := &stepOptions{name: stepFnName(fn)}
	for _, opt := range opts {
		opt(o)
	}

	stepID := st.takeStepID()

	// Idempotency: have we already recorded an output for (wfID, stepID)?
	rec, err := c.systemDB.checkStepOutput(c.ctx, st.workflowID, stepID)
	if err != nil {
		return zero[T](), err
	}
	if rec != nil {
		if rec.ErrorString != "" {
			return zero[T](), wrapError(ErrUnknown, errors.New(rec.ErrorString), "previously failed step %s", rec.FunctionName)
		}
		var out T
		if rec.HasOutput && rec.Output != "" {
			if _, derr := c.cfg.Serializer.Decode(rec.Output, &out); derr != nil {
				return zero[T](), derr
			}
		}
		return out, nil
	}

	// Execute, with retries if configured.
	var lastErr error
	var result T
	attempts := o.maxRetries
	if attempts <= 0 {
		attempts = 1
	}
	delay := o.baseDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	maxDelay := o.maxDelay
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}
	mult := o.backoffMul
	if mult <= 0 {
		mult = 2
	}
	for attempt := 0; attempt < attempts; attempt++ {
		result, lastErr = fn(c.ctx)
		if lastErr == nil {
			break
		}
		if attempt < attempts-1 {
			select {
			case <-c.ctx.Done():
				return zero[T](), c.ctx.Err()
			case <-time.After(delay):
			}
			delay = time.Duration(float64(delay) * mult)
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}

	// Record outcome in DB so future replays skip it.
	in := recordStepInput{
		WorkflowID:   st.workflowID,
		FunctionID:   stepID,
		FunctionName: o.name,
	}
	if lastErr == nil {
		enc, err := c.cfg.Serializer.Encode(result)
		if err != nil {
			return zero[T](), err
		}
		in.Output = &enc
	} else {
		es := lastErr.Error()
		in.ErrorString = &es
	}
	if err := c.systemDB.recordStepOutput(c.ctx, in); err != nil {
		// Best-effort: log and propagate.
		c.logger.Error("record step output failed", "workflow_id", st.workflowID, "step", stepID, "err", err)
	}

	return result, lastErr
}

func stepFnName(fn any) string {
	pc := reflect.ValueOf(fn).Pointer()
	rfn := runtime.FuncForPC(pc)
	if rfn == nil {
		return "step"
	}
	name := rfn.Name()
	name = strings.TrimSuffix(name, "-fm")
	return name
}

// runRegisteredWorkflowFromDB executes a workflow row that already exists in
// the database. Used by the queue runner and the recovery pass.
func runRegisteredWorkflowFromDB(c *Context, status WorkflowStatus) (WorkflowHandle[any], error) {
	entry, ok := c.registry.get(status.Name)
	if !ok {
		return nil, wrapError(ErrWorkflowNotRegistered, nil, "workflow %s not registered", status.Name)
	}

	encInput, err := c.cfg.Serializer.Encode(status.Input)
	if err != nil {
		return nil, err
	}

	resultCh := make(chan workflowOutcome, 1)
	wfCtx, cancel := context.WithCancelCause(c.ctx)
	c.core.active.Store(status.ID, &activeWorkflow{resultCh: resultCh, cancel: cancel})
	c.core.workflowsWg.Add(1)
	go func() {
		defer c.core.workflowsWg.Done()
		defer c.core.active.Delete(status.ID)
		defer cancel(nil)
		runWorkflowExecution(c, entry, status.ID, encInput, resultCh, wfCtx)
	}()

	return &directHandle[any]{id: status.ID, ctx: c, result: resultCh}, nil
}

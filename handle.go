package orc

import (
	"context"
	"reflect"
	"time"
)

// WorkflowHandle is a typed handle to a running or completed workflow.
type WorkflowHandle[R any] interface {
	GetWorkflowID() string
	GetStatus() (WorkflowStatus, error)
	GetResult(opts ...GetResultOption) (R, error)
}

// GetResultOption configures GetResult behaviour.
type GetResultOption func(*getResultOptions)

type getResultOptions struct {
	timeout      time.Duration
	pollInterval time.Duration
}

// WithHandleTimeout sets a timeout on GetResult.
func WithHandleTimeout(d time.Duration) GetResultOption {
	return func(o *getResultOptions) { o.timeout = d }
}

// WithHandlePollInterval overrides the default polling interval used to wait
// for a workflow to complete.
func WithHandlePollInterval(d time.Duration) GetResultOption {
	return func(o *getResultOptions) {
		if d > 0 {
			o.pollInterval = d
		}
	}
}

// directHandle is returned when the caller starts a workflow synchronously and
// the runtime hands back a channel for the result. It avoids polling when
// possible.
type directHandle[R any] struct {
	id     string
	ctx    *Context
	result chan workflowOutcome
}

type workflowOutcome struct {
	output any
	err    error
}

func (h *directHandle[R]) GetWorkflowID() string { return h.id }

func (h *directHandle[R]) GetStatus() (WorkflowStatus, error) {
	return getStatusByID(h.ctx, h.id)
}

func (h *directHandle[R]) GetResult(opts ...GetResultOption) (R, error) {
	o := &getResultOptions{pollInterval: h.ctx.cfg.NotificationPollInterval}
	for _, opt := range opts {
		opt(o)
	}

	var deadline <-chan time.Time
	if o.timeout > 0 {
		t := time.NewTimer(o.timeout)
		defer t.Stop()
		deadline = t.C
	}

	select {
	case res, ok := <-h.result:
		if !ok {
			// The buffered result was already consumed by an earlier
			// GetResult call. The workflow is finalized by now, so serve
			// repeat calls from the database.
			return pollHandleResult[R](h.ctx, h.id, o)
		}
		return castOutcome[R](res, h.ctx)
	case <-deadline:
		return zero[R](), ErrWorkflowAwaitTimeoutErr
	case <-h.ctx.ctx.Done():
		// fall back to polling DB - the workflow may still complete in another process.
		return pollHandleResult[R](h.ctx, h.id, o)
	}
}

// pollingHandle is returned for enqueued / cross-process workflows where we
// don't have a channel; we have to poll the DB.
type pollingHandle[R any] struct {
	id  string
	ctx *Context
}

func (h *pollingHandle[R]) GetWorkflowID() string { return h.id }
func (h *pollingHandle[R]) GetStatus() (WorkflowStatus, error) {
	return getStatusByID(h.ctx, h.id)
}
func (h *pollingHandle[R]) GetResult(opts ...GetResultOption) (R, error) {
	o := &getResultOptions{pollInterval: h.ctx.cfg.NotificationPollInterval}
	for _, opt := range opts {
		opt(o)
	}
	return pollHandleResult[R](h.ctx, h.id, o)
}

func getStatusByID(c *Context, id string) (WorkflowStatus, error) {
	st, err := c.systemDB.getWorkflowStatus(c.ctx, id, true)
	if err != nil {
		return WorkflowStatus{}, err
	}
	if st == nil {
		return WorkflowStatus{}, ErrWorkflowNotFoundErr
	}
	return *st, nil
}

func pollHandleResult[R any](c *Context, id string, o *getResultOptions) (R, error) {
	var deadline time.Time
	if o.timeout > 0 {
		deadline = time.Now().Add(o.timeout)
	}
	interval := o.pollInterval
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}

	// Wake immediately when this process finalizes (or cancels) the workflow,
	// instead of paying up to a full poll interval. Cross-process completion
	// is still observed via the interval poll.
	doneKey := workflowDoneKey(id)
	wake := c.core.hub.subscribe(doneKey)
	defer c.core.hub.unsubscribe(doneKey, wake)

	for {
		st, err := c.systemDB.getWorkflowStatus(c.ctx, id, true)
		if err != nil {
			if c.ctx.Err() != nil {
				return finalStatusRead[R](c, id)
			}
			return zero[R](), err
		}
		if st == nil {
			return zero[R](), ErrWorkflowNotFoundErr
		}
		if out, err, terminal := terminalOutcome[R](c, id, st); terminal {
			return out, err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return zero[R](), ErrWorkflowAwaitTimeoutErr
		}
		select {
		case <-c.ctx.Done():
			return finalStatusRead[R](c, id)
		case <-wake:
		case <-time.After(interval):
		}
	}
}

// terminalOutcome maps a terminal workflow row to a GetResult return value.
// The third return value reports whether the row was terminal at all.
func terminalOutcome[R any](c *Context, id string, st *WorkflowStatus) (R, error, bool) {
	switch st.Status {
	case WorkflowStatusSuccess:
		out, err := castOutput[R](st.Output, c)
		return out, err, true
	case WorkflowStatusError:
		if st.Error != nil {
			return zero[R](), wrapError(ErrAwaitedWorkflowFailed, st.Error, "workflow %s failed", id), true
		}
		return zero[R](), ErrAwaitedWorkflowFailedErr, true
	case WorkflowStatusCancelled:
		return zero[R](), ErrWorkflowCancelledErr, true
	case WorkflowStatusMaxRecoveryAttemptsExceeded:
		return zero[R](), ErrMaxAttemptsErr, true
	}
	return zero[R](), nil, false
}

// finalStatusRead is the last-gasp read used when the runtime context is
// already cancelled (shutdown). It uses a short standalone context — the
// runtime one would fail every query — so a result that was finalized just
// before shutdown is still observable, matching the documented directHandle
// fallback behavior.
func finalStatusRead[R any](c *Context, id string) (R, error) {
	rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := c.systemDB.getWorkflowStatus(rctx, id, true)
	if err == nil && st != nil {
		if out, terr, terminal := terminalOutcome[R](c, id, st); terminal {
			return out, terr
		}
	}
	return zero[R](), c.ctx.Err()
}

func castOutcome[R any](o workflowOutcome, c *Context) (R, error) {
	if o.err != nil {
		return zero[R](), o.err
	}
	return castOutput[R](o.output, c)
}

func castOutput[R any](v any, c *Context) (R, error) {
	if v == nil {
		return zero[R](), nil
	}
	if typed, ok := v.(R); ok {
		return typed, nil
	}
	// Try to serialize/deserialize through the configured serializer to coerce types.
	enc, err := c.cfg.Serializer.Encode(v)
	if err != nil {
		return zero[R](), err
	}
	var dst R
	out, err := c.cfg.Serializer.Decode(enc, &dst)
	if err != nil {
		return zero[R](), err
	}
	if typed, ok := out.(*R); ok {
		return *typed, nil
	}
	if typed, ok := out.(R); ok {
		return typed, nil
	}
	// Last-resort reflective copy.
	rv := reflect.ValueOf(out)
	dv := reflect.ValueOf(&dst).Elem()
	if rv.IsValid() && rv.Type().AssignableTo(dv.Type()) {
		dv.Set(rv)
		return dst, nil
	}
	return dst, nil
}

func zero[T any]() T {
	var z T
	return z
}

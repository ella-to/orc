package orc

import (
	"context"
	"time"
)

// SetEvent publishes a key/value event from inside a workflow. It is
// idempotent across re-execution because it is wrapped as a step.
func SetEvent(c *Context, key string, value any) error {
	st := wfStateFromContext(c.ctx)
	if st == nil {
		return ErrNotInWorkflowErr
	}
	enc, err := c.cfg.Serializer.Encode(value)
	if err != nil {
		return err
	}
	_, runErr := RunAsStep(c, func(_ context.Context) (struct{}, error) {
		return struct{}{}, c.systemDB.setEvent(c.ctx, setEventInput{
			WorkflowID: st.workflowID,
			Key:        key,
			Value:      enc,
		})
	}, WithStepName("orc.setEvent:"+key))
	return runErr
}

// GetEvent reads the latest value of an event published by another (or the
// current) workflow. It blocks until the event is available or the timeout
// expires.
func GetEvent[T any](c *Context, workflowID, key string, timeout time.Duration) (T, error) {
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	if st := wfStateFromContext(c.ctx); st != nil {
		// inside a workflow: wrap in a step
		return RunAsStep(c, func(_ context.Context) (T, error) {
			return pollGetEvent[T](c, workflowID, key, deadline)
		}, WithStepName("orc.getEvent:"+key))
	}
	return pollGetEvent[T](c, workflowID, key, deadline)
}

func pollGetEvent[T any](c *Context, workflowID, key string, deadline time.Time) (T, error) {
	interval := c.cfg.NotificationPollInterval
	for {
		rec, err := c.systemDB.getEvent(c.ctx, workflowID, key)
		if err != nil {
			return zero[T](), err
		}
		if rec != nil {
			var dst T
			if rec.Value == "" {
				return dst, nil
			}
			if _, derr := c.cfg.Serializer.Decode(rec.Value, &dst); derr != nil {
				return zero[T](), derr
			}
			return dst, nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return zero[T](), ErrWorkflowAwaitTimeoutErr
		}
		select {
		case <-c.ctx.Done():
			return zero[T](), c.ctx.Err()
		case <-time.After(interval):
		}
	}
}

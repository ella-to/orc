package orc

import (
	"context"
	"time"
)

// SendOption configures a Send call.
type SendOption func(*sendOptions)

type sendOptions struct{}

// Send delivers a message to a workflow's mailbox under a topic. If called
// inside a workflow, it is durable: the message is recorded as a step so a
// re-execution will not re-send.
func Send(c *Context, destinationID string, message any, topic string, opts ...SendOption) error {
	enc, err := c.cfg.Serializer.Encode(message)
	if err != nil {
		return err
	}

	st := wfStateFromContext(c.ctx)
	if st == nil {
		// Outside a workflow: just write directly.
		return c.systemDB.enqueueNotification(c.ctx, enqueueNotificationInput{
			DestinationID: destinationID,
			Topic:         topic,
			Message:       enc,
		})
	}

	// Inside a workflow: wrap as a step so the message is sent at most once.
	_, sendErr := RunAsStep(c, func(_ context.Context) (struct{}, error) {
		err := c.systemDB.enqueueNotification(c.ctx, enqueueNotificationInput{
			DestinationID: destinationID,
			Topic:         topic,
			Message:       enc,
		})
		return struct{}{}, err
	}, WithStepName("orc.send"))
	return sendErr
}

// Recv blocks until a message arrives on the given topic, or the timeout
// expires (zero timeout = wait forever). Inside a workflow, the result is
// checkpointed as a step.
func Recv[T any](c *Context, topic string, timeout time.Duration) (T, error) {
	st := wfStateFromContext(c.ctx)
	if st == nil {
		return zero[T](), ErrNotInWorkflowErr
	}

	// Encode the receiver workflow id + topic via a step.
	return RunAsStep(c, func(_ context.Context) (T, error) {
		var deadline time.Time
		if timeout > 0 {
			deadline = time.Now().Add(timeout)
		}
		interval := c.cfg.NotificationPollInterval
		for {
			rec, err := c.systemDB.popNotification(c.ctx, st.workflowID, topic)
			if err != nil {
				return zero[T](), err
			}
			if rec != nil {
				var dst T
				if rec.Message == "" {
					return dst, nil
				}
				if _, derr := c.cfg.Serializer.Decode(rec.Message, &dst); derr != nil {
					return zero[T](), derr
				}
				return dst, nil
			}
			if !deadline.IsZero() && time.Now().After(deadline) {
				return zero[T](), nil
			}
			select {
			case <-c.ctx.Done():
				return zero[T](), c.ctx.Err()
			case <-time.After(interval):
			}
		}
	}, WithStepName("orc.recv:"+topic))
}

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
		if err := c.systemDB.enqueueNotification(c.ctx, enqueueNotificationInput{
			DestinationID: destinationID,
			Topic:         topic,
			Message:       enc,
		}); err != nil {
			return err
		}
		c.core.hub.signal(notificationKey(destinationID, topic))
		return nil
	}

	// Inside a workflow: wrap as a step so the message is sent at most once.
	// The hub signal lives inside the step closure so a replayed (skipped)
	// Send doesn't spuriously wake receivers.
	_, sendErr := RunAsStep(c, func(_ context.Context) (struct{}, error) {
		err := c.systemDB.enqueueNotification(c.ctx, enqueueNotificationInput{
			DestinationID: destinationID,
			Topic:         topic,
			Message:       enc,
		})
		if err == nil {
			c.core.hub.signal(notificationKey(destinationID, topic))
		}
		return struct{}{}, err
	}, WithStepName("orc.send"))
	return sendErr
}

// Recv blocks until a message arrives on the given topic, or the timeout
// expires (zero timeout = wait forever). Inside a workflow, the result is
// checkpointed as a step.
//
// On timeout Recv returns the zero value of T and a nil error — it does not
// distinguish "no message" from a message that decodes to the zero value.
// Use a pointer or wrapper type for T if you need to tell them apart.
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

		// Subscribe before the first poll so an in-process Send between the
		// poll and the wait is never missed. The DB poll below remains the
		// fallback for messages sent from other processes.
		hubKey := notificationKey(st.workflowID, topic)
		wake := c.core.hub.subscribe(hubKey)
		defer c.core.hub.unsubscribe(hubKey, wake)

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
			case <-wake:
			case <-time.After(interval):
			}
		}
	}, WithStepName("orc.recv:"+topic))
}

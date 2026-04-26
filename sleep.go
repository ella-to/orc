package orc

import (
	"context"
	"time"
)

// Sleep durably pauses the current workflow for the given duration. The
// wakeup time is recorded as a step so re-execution after a crash skips
// already-elapsed sleeps.
//
// Returns the actual time slept (which may be less than `d` if the workflow
// is being recovered after the original sleep would have elapsed).
func Sleep(c *Context, d time.Duration) (time.Duration, error) {
	st := wfStateFromContext(c.ctx)
	if st == nil {
		// Outside a workflow: just sleep for real, but respect ctx cancel.
		select {
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		case <-time.After(d):
			return d, nil
		}
	}

	wakeUpAt, err := RunAsStep(c, func(_ context.Context) (int64, error) {
		return time.Now().Add(d).UnixMilli(), nil
	}, WithStepName("orc.sleep"))
	if err != nil {
		return 0, err
	}

	target := time.UnixMilli(wakeUpAt)
	left := time.Until(target)
	if left <= 0 {
		return 0, nil
	}
	select {
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	case <-time.After(left):
		return left, nil
	}
}

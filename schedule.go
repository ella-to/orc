package orc

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// registerScheduled wires a registered workflow to a cron expression.
// It must be called *before* Launch (registry guarantees that).
//
// On each tick the scheduler enqueues a fresh workflow; the workflow's
// argument is the scheduled wall time as a time.Time.
func registerScheduled(c *Context, entry *registryEntry, expr string) {
	c.core.schedulerMu.Lock()
	defer c.core.schedulerMu.Unlock()
	if c.core.scheduler == nil {
		c.core.scheduler = cron.New(cron.WithSeconds())
	}
	_, err := c.core.scheduler.AddFunc(expr, func() {
		// Deterministic per-tick ID so multiple processes pointing at the
		// same database deduplicate cron firings. robfig/cron's seconds field
		// fires at-most-once per second per expression, so truncating wall
		// time to whole seconds gives a stable boundary key. The PRIMARY KEY
		// on workflow_status (workflow_uuid) means concurrent inserts from
		// peer executors collapse to a single row -- the loser sees
		// AlreadyExisted=true and silently no-ops.
		now := time.Now().UTC()
		boundary := now.Truncate(time.Second)
		id := fmt.Sprintf("sched-%s-%d", entry.Name, boundary.Unix())

		// Use the internal queue so concurrency is bounded by the queue runner.
		// We cannot use generic RunWorkflow here because we don't know the input
		// type statically, so we insert directly via the system DB and let the
		// queue runner pick it up.
		ws := WorkflowStatus{
			ID:                 id,
			Name:               entry.Name,
			Status:             WorkflowStatusEnqueued,
			Input:              boundary,
			ExecutorID:         c.cfg.ExecutorID,
			ApplicationVersion: c.cfg.ApplicationVersion,
			QueueName:          internalQueueName,
			CreatedAt:          now,
			UpdatedAt:          now,
			CronSchedule:       expr,
		}
		res, err := c.systemDB.insertWorkflow(c.ctx, insertWorkflowInput{Status: ws})
		if err != nil {
			c.logger.Warn("scheduled enqueue failed", "name", entry.Name, "err", err)
			return
		}
		if res.AlreadyExisted {
			c.logger.Debug("scheduled tick deduplicated", "name", entry.Name, "id", id)
			return
		}
		c.core.wakeQueue()
	})
	if err != nil {
		panic(fmt.Sprintf("orc: invalid cron expression %q: %v", expr, err))
	}
}

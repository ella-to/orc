package orc

import (
	"time"
)

// recoverPending re-runs every workflow that is currently PENDING for this
// executor. This is invoked at Launch() so that workflows interrupted by a
// crash continue from their last checkpoint.
//
// It also picks up workflows that are stuck in ENQUEUED state with no queue
// assigned — these were started directly (RunWorkflow without WithQueue) but
// crashed before the worker goroutine could promote them to PENDING.
func recoverPending(c *Context) error {
	pending, err := c.systemDB.listPendingWorkflows(c.ctx, c.cfg.ExecutorID)
	if err != nil {
		return err
	}
	enqueued, err := c.systemDB.listWorkflows(c.ctx, listWorkflowsInput{
		Status:          []WorkflowStatusType{WorkflowStatusEnqueued},
		ExecutorID:      c.cfg.ExecutorID,
		LoadInputOutput: true,
		Limit:           10000,
	})
	if err != nil {
		return err
	}
	for _, st := range enqueued {
		// Only recover those with no queue: queued workflows are dispatched
		// by the queue runner instead.
		if st.QueueName == "" {
			pending = append(pending, st)
		}
	}
	for _, st := range pending {
		entry, ok := c.registry.get(st.Name)
		if !ok {
			c.logger.Warn("recovery skipped: workflow not registered", "name", st.Name, "workflow_id", st.ID)
			continue
		}

		// Cap recovery attempts.
		max := c.cfg.MaxRecoveryAttempts
		if entry.MaxRetries > 0 {
			max = entry.MaxRetries
		}
		if st.Attempts >= max {
			s := WorkflowStatusMaxRecoveryAttemptsExceeded
			es := "max recovery attempts exceeded"
			_ = c.systemDB.updateWorkflowStatus(c.ctx, updateWorkflowStatusInput{
				WorkflowID:  st.ID,
				Status:      s,
				ErrorString: &es,
				Now:         time.Now(),
			})
			continue
		}

		if _, err := runRegisteredWorkflowFromDB(c, st); err != nil {
			c.logger.Error("recovery failed to start workflow", "workflow_id", st.ID, "err", err)
		} else {
			c.logger.Info("recovered workflow", "workflow_id", st.ID, "name", st.Name)
		}
	}
	return nil
}

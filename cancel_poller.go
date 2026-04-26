package orc

import "time"

// cancelPoller watches for workflows whose database status has been moved to
// CANCELLED by some other process. When it finds one currently executing on
// this executor (i.e. present in core.active), it cancels the local per-
// workflow context so that any blocking step honoring ctx.Done() returns
// promptly.
//
// In-process cancellation is already handled directly by CancelWorkflow.
// This poller exists only for the cross-process case (multiple executors
// pointing at the same database).
type cancelPoller struct {
	c    *Context
	stop chan struct{}
}

func newCancelPoller(c *Context) *cancelPoller {
	return &cancelPoller{c: c, stop: make(chan struct{})}
}

func (p *cancelPoller) run() {
	defer close(p.c.core.cancelDone)
	t := time.NewTicker(p.c.cfg.CancelPollInterval)
	defer t.Stop()
	for {
		select {
		case <-p.c.ctx.Done():
			return
		case <-t.C:
			p.tick()
		}
	}
}

func (p *cancelPoller) tick() {
	// Snapshot active workflow IDs to avoid scanning the whole DB.
	var ids []string
	p.c.core.active.Range(func(k, _ any) bool {
		if id, ok := k.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	if len(ids) == 0 {
		return
	}

	rows, err := p.c.systemDB.listWorkflows(p.c.ctx, listWorkflowsInput{
		WorkflowIDs: ids,
		Status:      []WorkflowStatusType{WorkflowStatusCancelled},
		Limit:       len(ids),
	})
	if err != nil {
		return
	}
	for _, st := range rows {
		v, ok := p.c.core.active.Load(st.ID)
		if !ok {
			continue
		}
		aw, ok := v.(*activeWorkflow)
		if !ok || aw == nil || aw.cancel == nil {
			continue
		}
		aw.cancel(ErrWorkflowCancelledErr)
	}
}

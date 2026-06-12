package orc

import (
	"context"
	"sort"
	"sync"
	"time"
)

// RateLimiter constrains how many workflows a queue can dispatch within a
// rolling window.
type RateLimiter struct {
	Limit  int
	Period time.Duration
}

// WorkflowQueue is a registered, named queue. Multiple workflows enqueued
// against the same queue compete for the queue's worker slots.
type WorkflowQueue struct {
	Name              string
	WorkerConcurrency int
	GlobalConcurrency int
	RateLimiter       *RateLimiter
	WithPriority      bool
}

// QueueOption configures a queue.
type QueueOption func(*WorkflowQueue)

// WithWorkerConcurrency limits how many workflows from this queue can run
// simultaneously per process.
func WithWorkerConcurrency(n int) QueueOption {
	return func(q *WorkflowQueue) { q.WorkerConcurrency = n }
}

// WithGlobalConcurrency limits how many workflows from this queue can run
// concurrently across all processes (best-effort).
func WithGlobalConcurrency(n int) QueueOption {
	return func(q *WorkflowQueue) { q.GlobalConcurrency = n }
}

// WithRateLimiter applies a sliding-window rate limit.
func WithRateLimiter(rl *RateLimiter) QueueOption {
	return func(q *WorkflowQueue) { q.RateLimiter = rl }
}

// WithPriorityEnabled enables priority-based dispatch ordering for the queue.
func WithPriorityEnabled() QueueOption {
	return func(q *WorkflowQueue) { q.WithPriority = true }
}

// NewWorkflowQueue registers (or returns an existing) queue with the runtime.
func NewWorkflowQueue(c *Context, name string, opts ...QueueOption) *WorkflowQueue {
	if existing, ok := c.core.queues.Load(name); ok {
		return existing.(*WorkflowQueue)
	}
	q := &WorkflowQueue{Name: name}
	for _, o := range opts {
		o(q)
	}
	c.core.queues.Store(name, q)
	return q
}

// ListQueues returns all registered queues sorted by name.
func ListQueues(c *Context) []*WorkflowQueue {
	var out []*WorkflowQueue
	c.core.queues.Range(func(_, v any) bool {
		out = append(out, v.(*WorkflowQueue))
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// queueRunner is the background worker that dispatches enqueued workflows
// and (periodically) purges old rate-limit accounting rows.
type queueRunner struct {
	c    *Context
	stop chan struct{}
}

func newQueueRunner(c *Context) *queueRunner {
	return &queueRunner{c: c, stop: make(chan struct{})}
}

func (r *queueRunner) run() {
	defer close(r.c.core.queueDone)
	t := time.NewTicker(r.c.cfg.QueuePollInterval)
	defer t.Stop()
	pj := time.NewTicker(r.c.cfg.QueueDispatchLogPurgeInterval)
	defer pj.Stop()
	for {
		select {
		case <-r.c.ctx.Done():
			return
		case <-t.C:
			r.tick()
		case <-r.c.core.queueWake:
			// Something was enqueued in this process: dispatch immediately
			// instead of waiting out the poll interval.
			r.tick()
		case <-pj.C:
			r.purgeDispatchLog()
		}
	}
}

// purgeDispatchLog deletes queue_dispatch_log rows older than the longest
// retention window we need. The retention window is the larger of the
// configured QueueDispatchLogRetention and 2x the longest configured
// rate-limit period across all queues. The 2x factor leaves a safety margin
// so a wraparound at the boundary cannot under-count.
func (r *queueRunner) purgeDispatchLog() {
	retention := r.c.cfg.QueueDispatchLogRetention
	r.c.core.queues.Range(func(_, v any) bool {
		q := v.(*WorkflowQueue)
		if q.RateLimiter == nil || q.RateLimiter.Period <= 0 {
			return true
		}
		if d := 2 * q.RateLimiter.Period; d > retention {
			retention = d
		}
		return true
	})
	if retention <= 0 {
		return
	}
	cutoff := time.Now().Add(-retention)
	deleted, err := r.c.systemDB.purgeQueueDispatches(r.c.ctx, cutoff)
	if err != nil {
		r.c.logger.Warn("queue_dispatch_log purge failed", "err", err)
		return
	}
	if deleted > 0 {
		r.c.logger.Debug("queue_dispatch_log purged", "deleted", deleted, "cutoff", cutoff)
	}
}

func (r *queueRunner) tick() {
	r.c.core.queues.Range(func(_, v any) bool {
		q := v.(*WorkflowQueue)
		r.dispatchOne(q)
		return true
	})
}

func (r *queueRunner) dispatchOne(q *WorkflowQueue) {
	ctx := r.c.ctx
	if ctx.Err() != nil {
		return
	}

	limit := defaultWorkerLimit

	// Rate limit check.
	if q.RateLimiter != nil && q.RateLimiter.Limit > 0 && q.RateLimiter.Period > 0 {
		since := time.Now().Add(-q.RateLimiter.Period)
		used, err := r.c.systemDB.countQueueDispatches(ctx, q.Name, since)
		if err != nil {
			return
		}
		remaining := q.RateLimiter.Limit - used
		if remaining <= 0 {
			return
		}
		if remaining < limit {
			limit = remaining
		}
	}

	// Concurrency limits.
	if q.WorkerConcurrency > 0 {
		active, err := r.c.systemDB.countActiveQueueWorkflows(ctx, q.Name, r.c.cfg.ExecutorID)
		if err != nil {
			return
		}
		remaining := q.WorkerConcurrency - active
		if remaining <= 0 {
			return
		}
		if remaining < limit {
			limit = remaining
		}
	}
	if q.GlobalConcurrency > 0 {
		active, err := r.c.systemDB.countWorkflows(ctx, listWorkflowsInput{
			QueueName: q.Name,
			Status:    []WorkflowStatusType{WorkflowStatusPending},
		})
		if err != nil {
			return
		}
		remaining := q.GlobalConcurrency - active
		if remaining < limit {
			limit = remaining
		}
		if limit <= 0 {
			return
		}
	}

	ids, err := r.c.systemDB.dequeueWorkflows(ctx, dequeueInput{
		QueueName:    q.Name,
		Limit:        limit,
		ExecutorID:   r.c.cfg.ExecutorID,
		AppVersion:   r.c.cfg.ApplicationVersion,
		WithPriority: q.WithPriority,
	})
	if err != nil || len(ids) == 0 {
		return
	}

	_ = r.c.systemDB.recordQueueDispatch(ctx, q.Name, ids)

	for _, id := range ids {
		st, err := r.c.systemDB.getWorkflowStatus(ctx, id, true)
		if err != nil || st == nil {
			continue
		}
		_, err = runRegisteredWorkflowFromDB(r.c, *st)
		if err != nil {
			r.c.logger.Error("dispatch failed", "workflow_id", id, "name", st.Name, "err", err)
			es := err.Error()
			_ = r.c.systemDB.updateWorkflowStatus(context.Background(), updateWorkflowStatusInput{
				WorkflowID:  id,
				Status:      WorkflowStatusError,
				ErrorString: &es,
				Now:         time.Now(),
			})
		}
	}
}

const defaultWorkerLimit = 10

// guard against accidental data races on the (rare) map mutation paths.
var _ sync.Locker = (*sync.Mutex)(nil)

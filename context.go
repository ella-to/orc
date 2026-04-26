package orc

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
)

// Context is the central object handed around to workflows and steps.
// It embeds context.Context for deadlines/cancellation propagation and adds
// access to the durable runtime (workflow registry, queues, system DB).
//
// Context values are *cheap to copy* — all mutable state lives behind the
// `core` pointer. This lets us spawn per-workflow contexts that wrap a
// distinct underlying context.Context without duplicating runtime state.
type Context struct {
	ctx      context.Context
	cfg      *Config
	logger   *slog.Logger
	systemDB systemDatabase
	registry *registry
	core     *contextCore
}

// contextCore holds the mutable, shared runtime state for a Context.
type contextCore struct {
	cancel        context.CancelCauseFunc
	launched      atomic.Bool
	closed        atomic.Bool
	shutdownMu    sync.Mutex
	queues        sync.Map // map[string]*WorkflowQueue
	queueRunner   *queueRunner
	queueDone     chan struct{}
	queueStarted  atomic.Bool
	scheduler     *cron.Cron
	schedulerMu   sync.Mutex
	workflowsWg   sync.WaitGroup
	active        sync.Map // map[string]*activeWorkflow
	cancelPoller  *cancelPoller
	cancelDone    chan struct{}
	cancelStarted atomic.Bool
}

// activeWorkflow tracks a workflow currently executing in this process. The
// cancel function lets CancelWorkflow (and the cross-process cancel poller)
// proactively interrupt the worker goroutine.
type activeWorkflow struct {
	resultCh chan workflowOutcome
	cancel   context.CancelCauseFunc
}

// withinWorkflowState is stored in context.Context to track step IDs and the
// owning workflow id.
type withinWorkflowState struct {
	workflowID string
	nextStep   int
	mu         sync.Mutex
}

func (w *withinWorkflowState) takeStepID() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := w.nextStep
	w.nextStep++
	return id
}

type ctxKeyType struct{ name string }

var (
	contextKey = ctxKeyType{"orc.context"}
	wfStateKey = ctxKeyType{"orc.wfstate"}
)

// FromContext extracts the ORC Context from a standard context.Context.
// Returns nil if none is associated.
func FromContext(ctx context.Context) *Context {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(contextKey).(*Context); ok {
		return v
	}
	return nil
}

func wfStateFromContext(ctx context.Context) *withinWorkflowState {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(wfStateKey).(*withinWorkflowState); ok {
		return v
	}
	return nil
}

// NewContext constructs a new ORC Context. Call Launch() before issuing any
// workflows or queue dispatching, and Shutdown() to gracefully tear it down.
func NewContext(parent context.Context, cfg Config) (*Context, error) {
	if cfg.AppName == "" {
		return nil, newError(ErrInitialization, "AppName is required")
	}
	cfg.applyDefaults()

	core := &contextCore{}
	c := &Context{cfg: &cfg, logger: cfg.Logger, registry: newRegistry(), core: core}
	ctx, cancel := context.WithCancelCause(parent)
	core.cancel = cancel
	c.ctx = context.WithValue(ctx, contextKey, c)

	sysDB, err := newSQLiteSystemDB(parent, &cfg)
	if err != nil {
		return nil, err
	}
	c.systemDB = sysDB

	// Always make the internal queue available.
	NewWorkflowQueue(c, internalQueueName)
	return c, nil
}

const internalQueueName = "_orc_internal"

// Launch starts background processing (queue runner, scheduler) and runs a
// recovery pass for in-flight workflows assigned to this executor.
func Launch(c *Context) error {
	if c == nil {
		return errors.New("orc: nil context")
	}
	if !c.core.launched.CompareAndSwap(false, true) {
		return newError(ErrInitialization, "already launched")
	}

	c.core.queueDone = make(chan struct{})
	c.core.queueRunner = newQueueRunner(c)
	go c.core.queueRunner.run()
	c.core.queueStarted.Store(true)

	// Cross-process cancel poller: notices workflows marked CANCELLED in the
	// database (potentially by another process) and proactively cancels the
	// matching local worker goroutine. In-process CancelWorkflow already
	// cancels directly, so this only matters for multi-executor setups.
	c.core.cancelDone = make(chan struct{})
	c.core.cancelPoller = newCancelPoller(c)
	go c.core.cancelPoller.run()
	c.core.cancelStarted.Store(true)

	if c.core.scheduler != nil {
		c.core.scheduler.Start()
	}

	if err := recoverPending(c); err != nil {
		c.logger.Warn("recovery pass failed", "err", err)
	}

	c.logger.Info("orc launched", "executor_id", c.cfg.ExecutorID, "app_version", c.cfg.ApplicationVersion)
	return nil
}

// Shutdown gracefully stops background workers and closes the database.
func Shutdown(c *Context, timeout time.Duration) {
	if c == nil {
		return
	}
	c.core.shutdownMu.Lock()
	defer c.core.shutdownMu.Unlock()
	if c.core.closed.Load() {
		return
	}
	if !c.core.launched.Load() {
		if c.systemDB != nil {
			_ = c.systemDB.close(context.Background())
		}
		c.core.closed.Store(true)
		return
	}

	c.core.cancel(errors.New("orc: shutting down"))

	done := make(chan struct{})
	go func() { c.core.workflowsWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		c.logger.Warn("orc: timeout waiting for workflows", "timeout", timeout)
	}

	if c.core.queueStarted.Load() {
		select {
		case <-c.core.queueDone:
		case <-time.After(timeout):
			c.logger.Warn("orc: timeout waiting for queue runner")
		}
	}

	if c.core.cancelStarted.Load() {
		select {
		case <-c.core.cancelDone:
		case <-time.After(timeout):
			c.logger.Warn("orc: timeout waiting for cancel poller")
		}
	}

	if c.core.scheduler != nil {
		stopCtx := c.core.scheduler.Stop()
		select {
		case <-stopCtx.Done():
		case <-time.After(timeout):
			c.logger.Warn("orc: timeout waiting for scheduler")
		}
	}

	_ = c.systemDB.close(context.Background())
	c.core.launched.Store(false)
	c.core.closed.Store(true)
}

// Cancel cancels the context with a cause.
func (c *Context) Cancel(cause error) { c.core.cancel(cause) }

// Logger returns the configured slog logger.
func (c *Context) Logger() *slog.Logger { return c.logger }

// ConfigSnapshot returns a copy of the active configuration (without DB pointer).
func (c *Context) ConfigSnapshot() Config { return *c.cfg }

// ExecutorID returns this process's executor identifier.
func (c *Context) ExecutorID() string { return c.cfg.ExecutorID }

// AppVersion returns the configured application version.
func (c *Context) AppVersion() string { return c.cfg.ApplicationVersion }

// Underlying returns the wrapped context.Context.
func (c *Context) Underlying() context.Context { return c.ctx }

func init() { _ = runtime.Caller }

package orc

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

// registry stores all registered workflows. Each workflow is indexed under
// both its function FQN and (optionally) its custom name, so RunWorkflow
// can find the entry whether the caller passes a function value (FQN) or a
// recovery path uses the recorded WorkflowStatus.Name (custom name).
type registry struct {
	mu      sync.RWMutex
	entries map[string]*registryEntry
}

func newRegistry() *registry {
	return &registry{entries: make(map[string]*registryEntry)}
}

// registryEntry holds the runtime metadata for one registered workflow.
// The wrapper takes the encoded input string from the DB and returns the
// encoded output (or an error). This indirection lets us register strongly
// typed Go functions while persisting/recovering them through the DB layer.
type registryEntry struct {
	Name         string
	CronSchedule string
	MaxRetries   int
	Wrapper      func(ctx *Context, encodedInput string) (encodedOutput string, runtimeErr error)
	InputType    reflect.Type
	OutputType   reflect.Type
	IsScheduled  bool
}

// fqn returns the fully qualified name of a function value, e.g.
// "github.com/me/pkg.MyWorkflow".
func fqn(fn any) string {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return ""
	}
	pc := v.Pointer()
	rfn := runtime.FuncForPC(pc)
	if rfn == nil {
		return fmt.Sprintf("anon-%v", pc)
	}
	name := rfn.Name()
	// trim "-fm" suffix that Go adds for method values
	name = strings.TrimSuffix(name, "-fm")
	return name
}

func (r *registry) get(name string) (*registryEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	return e, ok
}

func (r *registry) put(name string, e *registryEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[name] = e
}

func (r *registry) all() []*registryEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*registryEntry, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e)
	}
	return out
}

// RegisterWorkflowOption configures workflow registration.
type RegisterWorkflowOption func(*registerWorkflowOptions)

type registerWorkflowOptions struct {
	name         string
	cronSchedule string
	maxRetries   int
}

// WithWorkflowName overrides the registered workflow name (defaults to the FQN
// of the Go function). Useful for stable cross-version identifiers.
func WithWorkflowName(name string) RegisterWorkflowOption {
	return func(o *registerWorkflowOptions) { o.name = name }
}

// WithSchedule registers a cron expression that will trigger this workflow
// once Launch() is called. The schedule uses robfig/cron with seconds
// granularity (e.g. "* * * * * *" runs every second).
func WithSchedule(expr string) RegisterWorkflowOption {
	return func(o *registerWorkflowOptions) { o.cronSchedule = expr }
}

// WithMaxRecoveryAttempts overrides the global cap for this workflow.
func WithMaxRecoveryAttempts(n int) RegisterWorkflowOption {
	return func(o *registerWorkflowOptions) { o.maxRetries = n }
}

// RegisterWorkflow registers a typed Go function as a durable workflow.
// The function must have one of the signatures:
//
//	func(ctx *orc.Context, in I) (O, error)
//	func(ctx *orc.Context) (O, error)        // input must be passed as nil
//
// where I and O are JSON-serializable types.
func RegisterWorkflow[I any, O any](c *Context, fn any, opts ...RegisterWorkflowOption) {
	if c == nil {
		panic("orc: RegisterWorkflow on nil context")
	}
	if c.core.launched.Load() {
		panic("orc: cannot register workflow after Launch()")
	}

	opt := &registerWorkflowOptions{}
	for _, o := range opts {
		o(opt)
	}

	functionFQN := fqn(fn)
	name := opt.name
	if name == "" {
		name = functionFQN
	}

	entry := buildRegistryEntry[I, O](name, fn, opt)
	// Index under both the canonical (recorded) name and the function's FQN
	// so lookup works whether the caller starts a workflow by function value
	// (RunWorkflow) or by recorded name (recovery / queue dispatcher).
	c.registry.put(name, entry)
	if functionFQN != "" && functionFQN != name {
		c.registry.put(functionFQN, entry)
	}

	if opt.cronSchedule != "" {
		entry.IsScheduled = true
		registerScheduled(c, entry, opt.cronSchedule)
	}
}

// buildRegistryEntry constructs the wrapper closure for a typed workflow
// function. The function's shape is validated here, at registration time, so
// a mis-registered workflow fails fast with a clear message instead of
// erroring on its first execution.
func buildRegistryEntry[I any, O any](name string, fn any, opt *registerWorkflowOptions) *registryEntry {
	rt := reflect.TypeOf(fn)
	if rt == nil || rt.Kind() != reflect.Func {
		panic(fmt.Sprintf("orc: workflow %q is not a function (got %T)", name, fn))
	}
	validateWorkflowSignature[I, O](name, rt)

	// Fast path: the overwhelmingly common signatures are invoked directly,
	// avoiding reflect.Call on every workflow execution. Anything else (e.g.
	// interface-typed parameters) falls back to reflection.
	var invoke func(c *Context, in I) (any, error)
	switch f := fn.(type) {
	case func(*Context, I) (O, error):
		invoke = func(c *Context, in I) (any, error) { return f(c, in) }
	case func(*Context) (O, error):
		invoke = func(c *Context, _ I) (any, error) { return f(c) }
	default:
		fv := reflect.ValueOf(fn)
		takesInput := rt.NumIn() == 2
		invoke = func(c *Context, in I) (any, error) {
			args := []reflect.Value{reflect.ValueOf(c)}
			if takesInput {
				args = append(args, reflect.ValueOf(in))
			}
			out := fv.Call(args)
			var runErr error
			if !out[1].IsNil() {
				runErr = out[1].Interface().(error)
			}
			return out[0].Interface(), runErr
		}
	}

	wrapper := func(c *Context, encodedInput string) (string, error) {
		var in I
		if encodedInput != "" {
			if _, err := c.cfg.Serializer.Decode(encodedInput, &in); err != nil {
				return "", err
			}
		}
		result, runErr := invoke(c, in)
		if runErr != nil {
			return "", runErr
		}
		return c.cfg.Serializer.Encode(result)
	}

	var inT reflect.Type
	if rt.NumIn() == 2 {
		inT = rt.In(1)
	}

	return &registryEntry{
		Name:       name,
		Wrapper:    wrapper,
		MaxRetries: opt.maxRetries,
		InputType:  inT,
		OutputType: rt.Out(0),
	}
}

// validateWorkflowSignature panics with an actionable message unless fn looks
// like func(*Context, I) (O, error) or func(*Context) (O, error), with I/O
// compatible with the generic type parameters used at registration.
func validateWorkflowSignature[I any, O any](name string, rt reflect.Type) {
	fail := func(why string) {
		panic(fmt.Sprintf(
			"orc: cannot register workflow %q: %s — want func(*orc.Context, %v) (%v, error) or func(*orc.Context) (%v, error), got %v",
			name, why, reflect.TypeFor[I](), reflect.TypeFor[O](), reflect.TypeFor[O](), rt))
	}
	if rt.NumIn() < 1 || rt.NumIn() > 2 {
		fail("wrong number of parameters")
	}
	if rt.In(0) != reflect.TypeFor[*Context]() {
		fail("first parameter must be *orc.Context")
	}
	if rt.NumOut() != 2 {
		fail("must return exactly (output, error)")
	}
	if rt.Out(1) != reflect.TypeFor[error]() {
		fail("second return value must be error")
	}
	if rt.NumIn() == 2 {
		if inT := reflect.TypeFor[I](); !inT.AssignableTo(rt.In(1)) {
			fail(fmt.Sprintf("input type parameter %v is not assignable to the function's %v", inT, rt.In(1)))
		}
	}
	if outT := reflect.TypeFor[O](); !rt.Out(0).AssignableTo(outT) && !outT.AssignableTo(rt.Out(0)) {
		fail(fmt.Sprintf("output type parameter %v does not match the function's %v", outT, rt.Out(0)))
	}
}

// withWFState returns a *Context whose underlying context.Context carries
// the supplied workflow state. The shared `core` pointer is preserved so all
// runtime state (registry, queues, etc.) remains coordinated.
func withWFState(c *Context, parent context.Context, st *withinWorkflowState) *Context {
	clone := &Context{
		cfg:      c.cfg,
		logger:   c.logger,
		systemDB: c.systemDB,
		registry: c.registry,
		core:     c.core,
	}
	ctx := context.WithValue(parent, contextKey, clone)
	ctx = context.WithValue(ctx, wfStateKey, st)
	clone.ctx = ctx
	return clone
}

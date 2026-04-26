package orc

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
)

// newTestContext spins up an in-memory SQLite-backed Context and registers
// `Shutdown` on the test cleanup.
//
// If `file` is set, a file-backed DB is used instead so the test can simulate
// crash/restart by reopening from the same path.
func newTestContext(t *testing.T, opts ...func(*Config)) *Context {
	t.Helper()
	cfg := Config{
		AppName:                  "orctest",
		DatabasePath:             "",
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		QueuePollInterval:        5 * time.Millisecond,
		NotificationPollInterval: 5 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	ctx, err := NewContext(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewContext: %v", err)
	}
	t.Cleanup(func() { Shutdown(ctx, 2*time.Second) })
	return ctx
}

// newTestContextFile creates a Context backed by a file in the test's tempdir.
func newTestContextFile(t *testing.T, name string, opts ...func(*Config)) *Context {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	combined := append([]func(*Config){func(c *Config) { c.DatabasePath = path }}, opts...)
	return newTestContext(t, combined...)
}

// reopenContext closes c and opens a new Context against the same DatabasePath,
// simulating a process restart.
func reopenContext(t *testing.T, c *Context, opts ...func(*Config)) *Context {
	t.Helper()
	path := c.cfg.DatabasePath
	if path == "" {
		t.Fatal("reopenContext: requires file-backed Config (DatabasePath)")
	}
	Shutdown(c, 2*time.Second)

	cfg := Config{
		AppName:                  c.cfg.AppName,
		DatabasePath:             path,
		Logger:                   slog.New(slog.NewTextHandler(io.Discard, nil)),
		ExecutorID:               c.cfg.ExecutorID,
		QueuePollInterval:        5 * time.Millisecond,
		NotificationPollInterval: 5 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	ctx, err := NewContext(context.Background(), cfg)
	if err != nil {
		t.Fatalf("reopen NewContext: %v", err)
	}
	t.Cleanup(func() { Shutdown(ctx, 2*time.Second) })
	return ctx
}

// assertEventually polls until cond returns true or the deadline elapses.
func assertEventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("eventually: %s", msg)
}

package orc

import (
	"log/slog"
	"os"
	"time"

	"ella.to/sqlite"
)

// Config holds parameters used to initialize an ORC Context.
//
// Either DatabasePath or Database must be provided; AppName is required.
type Config struct {
	// AppName identifies the application. Required.
	AppName string

	// DatabasePath is a filesystem path to the SQLite database file. If empty
	// and Database is nil, an in-memory database is used (useful for tests).
	DatabasePath string

	// Database, if non-nil, is an existing ella.to/sqlite database that ORC
	// will reuse instead of opening its own. ORC will not close it on Shutdown.
	Database *sqlite.Database

	// PoolSize controls the SQLite connection pool size used when ORC opens
	// its own database. Defaults to 8.
	PoolSize int

	// Logger is the slog logger used internally. Defaults to slog.Default().
	Logger *slog.Logger

	// Serializer encodes/decodes workflow inputs/outputs and step outputs.
	// Defaults to a JSON-based serializer.
	Serializer Serializer

	// ApplicationVersion identifies a deployment of the application. If empty
	// it defaults to the value of the DBOS__APPVERSION env var, or "dev".
	ApplicationVersion string

	// ExecutorID identifies this process. If empty it defaults to the value
	// of DBOS__VMID, or "local".
	ExecutorID string

	// MaxRecoveryAttempts caps how many times a workflow can be recovered
	// before being marked MAX_RECOVERY_ATTEMPTS_EXCEEDED. Defaults to 50.
	MaxRecoveryAttempts int

	// QueuePollInterval controls how often the queue runner polls for new
	// enqueued workflows. Defaults to 50ms.
	QueuePollInterval time.Duration

	// NotificationPollInterval controls how often Recv/GetEvent/Sleep poll
	// the database while waiting. Defaults to 50ms.
	NotificationPollInterval time.Duration

	// CancelPollInterval controls how often the cancel poller scans the DB
	// for workflows that have been CANCELLED externally and need their local
	// per-workflow context cancelled. Defaults to 250ms.
	CancelPollInterval time.Duration

	// QueueDispatchLogRetention controls how long rate-limit dispatch records
	// are retained in queue_dispatch_log. The janitor purges entries older
	// than max(this value, 2x longest configured rate-limit Period). Defaults
	// to 1 hour.
	QueueDispatchLogRetention time.Duration

	// QueueDispatchLogPurgeInterval controls how often the janitor runs.
	// Defaults to 1 minute.
	QueueDispatchLogPurgeInterval time.Duration

	// SkipMigrations disables auto-applying SQL migrations on startup.
	// Useful when migrations are managed externally.
	SkipMigrations bool
}

func (c *Config) applyDefaults() {
	if c.PoolSize <= 0 {
		c.PoolSize = 8
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Serializer == nil {
		c.Serializer = jsonSerializer{}
	}
	if c.ApplicationVersion == "" {
		if v := os.Getenv("DBOS__APPVERSION"); v != "" {
			c.ApplicationVersion = v
		} else {
			c.ApplicationVersion = "dev"
		}
	}
	if c.ExecutorID == "" {
		if v := os.Getenv("DBOS__VMID"); v != "" {
			c.ExecutorID = v
		} else {
			c.ExecutorID = "local"
		}
	}
	if c.MaxRecoveryAttempts <= 0 {
		c.MaxRecoveryAttempts = 50
	}
	if c.QueuePollInterval <= 0 {
		c.QueuePollInterval = 50 * time.Millisecond
	}
	if c.NotificationPollInterval <= 0 {
		c.NotificationPollInterval = 50 * time.Millisecond
	}
	if c.CancelPollInterval <= 0 {
		c.CancelPollInterval = 250 * time.Millisecond
	}
	if c.QueueDispatchLogRetention <= 0 {
		c.QueueDispatchLogRetention = time.Hour
	}
	if c.QueueDispatchLogPurgeInterval <= 0 {
		c.QueueDispatchLogPurgeInterval = time.Minute
	}
}

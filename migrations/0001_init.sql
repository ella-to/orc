-- ORC: Lightweight Durable Workflows on SQLite
-- Initial schema (mirrors a subset of dbos system_database, adapted for SQLite).
-- All timestamps are stored as Unix seconds (INTEGER). Durations are milliseconds.

CREATE TABLE IF NOT EXISTS workflow_status (
    workflow_uuid       TEXT PRIMARY KEY,
    status              TEXT NOT NULL,
    name                TEXT NOT NULL,
    output              TEXT,
    error               TEXT,
    executor_id         TEXT NOT NULL DEFAULT 'local',
    application_version TEXT NOT NULL DEFAULT '',
    application_id      TEXT NOT NULL DEFAULT '',
    queue_name          TEXT,
    deduplication_id    TEXT,
    priority            INTEGER NOT NULL DEFAULT 0,
    timeout_ms          INTEGER NOT NULL DEFAULT 0,
    deadline_ms         INTEGER NOT NULL DEFAULT 0,
    delay_until_ms      INTEGER NOT NULL DEFAULT 0,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    started_at_ms       INTEGER NOT NULL DEFAULT 0,
    attempts            INTEGER NOT NULL DEFAULT 0,
    input               TEXT,
    forked_from         TEXT,
    parent_workflow_id  TEXT,
    cron_schedule       TEXT,
    UNIQUE (queue_name, deduplication_id)
);

CREATE INDEX IF NOT EXISTS idx_workflow_status_status        ON workflow_status(status);
CREATE INDEX IF NOT EXISTS idx_workflow_status_queue_dequeue ON workflow_status(queue_name, status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_workflow_status_executor      ON workflow_status(executor_id, status);

-- Step / operation outputs (idempotency table). Each (workflow, step) writes
-- its result here so that re-execution after a crash skips already-finished steps.
CREATE TABLE IF NOT EXISTS operation_outputs (
    workflow_uuid     TEXT NOT NULL,
    function_id       INTEGER NOT NULL,
    function_name     TEXT NOT NULL,
    output            TEXT,
    error             TEXT,
    child_workflow_id TEXT,
    created_at        INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
);

-- Inter-workflow notifications (Send/Recv). One row per message.
CREATE TABLE IF NOT EXISTS notifications (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    destination_uuid  TEXT NOT NULL,
    topic             TEXT NOT NULL DEFAULT '',
    message           TEXT,
    created_at        INTEGER NOT NULL,
    message_uuid      TEXT NOT NULL UNIQUE,
    consumed          INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_notifications_dest_topic ON notifications(destination_uuid, topic, consumed);

-- Workflow events (SetEvent/GetEvent). Latest value per (workflow, key).
CREATE TABLE IF NOT EXISTS workflow_events (
    workflow_uuid TEXT NOT NULL,
    key           TEXT NOT NULL,
    value         TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (workflow_uuid, key),
    FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid) ON DELETE CASCADE
);

-- Queue rate-limit tracking: one row per dequeue event so a sliding window can be queried.
CREATE TABLE IF NOT EXISTS queue_dispatch_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name    TEXT NOT NULL,
    workflow_uuid TEXT NOT NULL,
    dispatched_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_dispatch_log_queue ON queue_dispatch_log(queue_name, dispatched_at);

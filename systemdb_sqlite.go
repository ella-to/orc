package orc

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"ella.to/sqlite"
	"github.com/google/uuid"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// sqliteSystemDB is the SQLite-backed implementation of systemDatabase.
type sqliteSystemDB struct {
	db         *sqlite.Database
	owns       bool // whether we should Close() the database on shutdown
	logger     *slog.Logger
	serializer Serializer
}

func newSQLiteSystemDB(ctx context.Context, cfg *Config) (*sqliteSystemDB, error) {
	var db *sqlite.Database
	owns := false

	if cfg.Database != nil {
		db = cfg.Database
	} else {
		opts := []sqlite.OptionFunc{
			sqlite.WithPoolSize(cfg.PoolSize),
		}
		if cfg.DatabasePath == "" {
			opts = append(opts, sqlite.WithMemory())
		} else {
			opts = append(opts, sqlite.WithFile(cfg.DatabasePath))
		}
		var err error
		db, err = sqlite.New(ctx, opts...)
		if err != nil {
			return nil, wrapError(ErrInitialization, err, "open sqlite")
		}
		owns = true
	}

	s := &sqliteSystemDB{db: db, owns: owns, logger: cfg.Logger, serializer: cfg.Serializer}

	if !cfg.SkipMigrations {
		if err := sqlite.Migration(ctx, db, migrationsFS, "migrations"); err != nil {
			if owns {
				_ = db.Close()
			}
			return nil, wrapError(ErrInitialization, err, "apply migrations")
		}
	}

	return s, nil
}

func (s *sqliteSystemDB) close(ctx context.Context) error {
	if s.owns && s.db != nil {
		return s.db.Close()
	}
	return nil
}

// nowMs returns the current Unix time in milliseconds.
func nowMs() int64 { return time.Now().UnixMilli() }

func tsToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func timeToMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func optStr(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

// ---------- workflow_status ----------

func (s *sqliteSystemDB) insertWorkflow(ctx context.Context, in insertWorkflowInput) (insertWorkflowResult, error) {
	res := insertWorkflowResult{}
	st := in.Status

	inputStr := in.EncodedInput
	if inputStr == "" {
		var err error
		inputStr, err = s.serializer.Encode(st.Input)
		if err != nil {
			return res, err
		}
	}

	var outStr any
	var errStr any
	if st.Output != nil {
		v, err := s.serializer.Encode(st.Output)
		if err != nil {
			return res, err
		}
		outStr = v
	}
	if st.Error != nil {
		errStr = st.Error.Error()
	}

	now := nowMs()
	if st.CreatedAt.IsZero() {
		st.CreatedAt = time.UnixMilli(now).UTC()
	}
	if st.UpdatedAt.IsZero() {
		st.UpdatedAt = st.CreatedAt
	}

	var inserted bool
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)

		// Try to insert; on conflict do nothing.
		stmt, err := conn.Prepare(ctx, `
			INSERT INTO workflow_status (
				workflow_uuid, status, name, output, error,
				executor_id, application_version, application_id,
				queue_name, deduplication_id, priority,
				timeout_ms, deadline_ms, delay_until_ms,
				created_at, updated_at, started_at_ms,
				attempts, input, forked_from, parent_workflow_id, cron_schedule
			) VALUES (
				?, ?, ?, ?, ?,
				?, ?, ?,
				?, ?, ?,
				?, ?, ?,
				?, ?, ?,
				?, ?, ?, ?, ?
			)
			ON CONFLICT(workflow_uuid) DO NOTHING;
		`,
			st.ID, string(st.Status), st.Name, outStr, errStr,
			st.ExecutorID, st.ApplicationVersion, st.ApplicationID,
			nullableStr(st.QueueName), nullableStr(st.DeduplicationID), st.Priority,
			st.Timeout.Milliseconds(), timeToMs(st.Deadline), timeToMs(st.DelayUntil),
			timeToMs(st.CreatedAt), timeToMs(st.UpdatedAt), timeToMs(st.StartedAt),
			st.Attempts, inputStr, nullableStr(st.ForkedFrom), nullableStr(st.ParentWorkflowID), nullableStr(st.CronSchedule),
		)
		if err != nil {
			return err
		}
		if _, err := stmt.Step(); err != nil {
			return err
		}

		// changes() returns the number of rows actually affected by the
		// most recent DML. If 0, the ON CONFLICT branch fired and the row
		// already existed. This is the only reliable way to detect a
		// conflict-noop because matching on createdAt/status would race
		// any concurrent transition (e.g., the queue runner moving the
		// row to PENDING between our insert and read-back).
		cs, err := conn.Prepare(ctx, `SELECT changes() AS c;`)
		if err != nil {
			return err
		}
		defer cs.Reset()
		hasRow, err := cs.Step()
		if err != nil {
			return err
		}
		if hasRow {
			inserted = cs.GetInt64("c") > 0
		}
		return nil
	})
	if err != nil {
		// Surface dedup violations specially.
		if isUniqueViolation(err) {
			return res, wrapError(ErrDuplicate, err, "duplicate dedup id")
		}
		return res, wrapError(ErrUnknown, err, "insert workflow")
	}

	if inserted {
		// Fresh row: what we wrote is canonical, no read-back needed.
		res.Status = st
		res.RawInput = inputStr
		return res, nil
	}

	// Conflict no-op: read back the existing row (including the raw input
	// text) so callers can compare for idempotency.
	got, raw, err := s.getWorkflowStatusRaw(ctx, st.ID)
	if err != nil {
		return res, err
	}
	if got == nil {
		return res, newError(ErrUnknown, "workflow row missing after insert: %s", st.ID)
	}
	res.Status = *got
	res.RawInput = raw
	res.AlreadyExisted = true
	return res, nil
}

// getWorkflowStatusRaw is getWorkflowStatus(loadIO=true) plus the raw
// (still-serialized) input text.
func (s *sqliteSystemDB) getWorkflowStatusRaw(ctx context.Context, workflowID string) (*WorkflowStatus, string, error) {
	var out *WorkflowStatus
	var raw string
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT workflow_uuid, status, name, output, error,
			       executor_id, application_version, application_id,
			       queue_name, deduplication_id, priority,
			       timeout_ms, deadline_ms, delay_until_ms,
			       created_at, updated_at, started_at_ms,
			       attempts, input, forked_from, parent_workflow_id, cron_schedule
			FROM workflow_status WHERE workflow_uuid = ?;`, workflowID)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		ws, err := s.scanWorkflowRow(stmt, true)
		if err != nil {
			return err
		}
		raw = stmt.GetText("input")
		out = ws
		return nil
	})
	if err != nil {
		return nil, "", wrapError(ErrUnknown, err, "get workflow status raw")
	}
	return out, raw, nil
}

func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed")
}

func (s *sqliteSystemDB) getWorkflowStatus(ctx context.Context, workflowID string, loadIO bool) (*WorkflowStatus, error) {
	var out *WorkflowStatus
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT workflow_uuid, status, name, output, error,
			       executor_id, application_version, application_id,
			       queue_name, deduplication_id, priority,
			       timeout_ms, deadline_ms, delay_until_ms,
			       created_at, updated_at, started_at_ms,
			       attempts, input, forked_from, parent_workflow_id, cron_schedule
			FROM workflow_status WHERE workflow_uuid = ?;`, workflowID)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		ws, err := s.scanWorkflowRow(stmt, loadIO)
		if err != nil {
			return err
		}
		out = ws
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "get workflow status")
	}
	return out, nil
}

func (s *sqliteSystemDB) scanWorkflowRow(stmt *sqlite.Stmt, loadIO bool) (*WorkflowStatus, error) {
	ws := &WorkflowStatus{
		ID:                 stmt.GetText("workflow_uuid"),
		Status:             WorkflowStatusType(stmt.GetText("status")),
		Name:               stmt.GetText("name"),
		ExecutorID:         stmt.GetText("executor_id"),
		ApplicationVersion: stmt.GetText("application_version"),
		ApplicationID:      stmt.GetText("application_id"),
		QueueName:          stmt.GetText("queue_name"),
		DeduplicationID:    stmt.GetText("deduplication_id"),
		Priority:           int(stmt.GetInt64("priority")),
		Timeout:            time.Duration(stmt.GetInt64("timeout_ms")) * time.Millisecond,
		Deadline:           tsToTime(stmt.GetInt64("deadline_ms")),
		DelayUntil:         tsToTime(stmt.GetInt64("delay_until_ms")),
		CreatedAt:          tsToTime(stmt.GetInt64("created_at")),
		UpdatedAt:          tsToTime(stmt.GetInt64("updated_at")),
		StartedAt:          tsToTime(stmt.GetInt64("started_at_ms")),
		Attempts:           int(stmt.GetInt64("attempts")),
		ForkedFrom:         stmt.GetText("forked_from"),
		ParentWorkflowID:   stmt.GetText("parent_workflow_id"),
		CronSchedule:       stmt.GetText("cron_schedule"),
	}
	if loadIO {
		if v, err := s.serializer.Decode(stmt.GetText("input"), nil); err == nil {
			ws.Input = v
		}
		if v, err := s.serializer.Decode(stmt.GetText("output"), nil); err == nil {
			ws.Output = v
		}
		if e := stmt.GetText("error"); e != "" {
			ws.Error = errors.New(e)
		}
	}
	return ws, nil
}

// listWorkflowsWhere builds the WHERE clause + bind args for the
// filter portion of listWorkflowsInput. Used by both listWorkflows
// and countWorkflows so that totals match what a paginated list
// request would return.
func listWorkflowsWhere(in listWorkflowsInput) (string, []any) {
	var (
		conds []string
		args  []any
	)
	if len(in.WorkflowIDs) > 0 {
		conds = append(conds, "workflow_uuid IN ("+sqlite.Placeholders(len(in.WorkflowIDs))+")")
		for _, id := range in.WorkflowIDs {
			args = append(args, id)
		}
	}
	if len(in.Status) > 0 {
		conds = append(conds, "status IN ("+sqlite.Placeholders(len(in.Status))+")")
		for _, st := range in.Status {
			args = append(args, string(st))
		}
	}
	// Text filters: exact equality by default. The admin HTTP API opts in to
	// substring matching (Fuzzy) for interactive search; internal callers
	// (recovery, queue accounting) must never fuzzy-match — e.g. executor
	// "node-1" must not pick up workflows owned by "node-10".
	match := func(col, val string) {
		if in.Fuzzy {
			conds = append(conds, col+" LIKE ?")
			args = append(args, "%"+val+"%")
		} else {
			conds = append(conds, col+" = ?")
			args = append(args, val)
		}
	}
	if in.WorkflowName != "" {
		match("name", in.WorkflowName)
	}
	if in.QueueName != "" {
		match("queue_name", in.QueueName)
	}
	if in.ExecutorID != "" {
		match("executor_id", in.ExecutorID)
	}
	if !in.StartTime.IsZero() {
		conds = append(conds, "created_at >= ?")
		args = append(args, timeToMs(in.StartTime))
	}
	if !in.EndTime.IsZero() {
		conds = append(conds, "created_at <= ?")
		args = append(args, timeToMs(in.EndTime))
	}
	if !in.UpdatedBefore.IsZero() {
		conds = append(conds, "updated_at < ?")
		args = append(args, timeToMs(in.UpdatedBefore))
	}
	if len(in.ExcludeQueueNames) > 0 {
		conds = append(conds, "(queue_name IS NULL OR queue_name NOT IN ("+sqlite.Placeholders(len(in.ExcludeQueueNames))+"))")
		for _, q := range in.ExcludeQueueNames {
			args = append(args, q)
		}
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	return where, args
}

func (s *sqliteSystemDB) listWorkflows(ctx context.Context, in listWorkflowsInput) ([]WorkflowStatus, error) {
	where, args := listWorkflowsWhere(in)
	order := "ASC"
	if in.SortDescending {
		order = "DESC"
	}
	var orderBy string
	switch in.SortBy {
	case "", listWorkflowSortCreated:
		orderBy = "created_at"
	case listWorkflowSortName:
		orderBy = "COALESCE(name, '')"
	case listWorkflowSortStatus:
		orderBy = "status"
	case listWorkflowSortQueue:
		orderBy = "COALESCE(queue_name, '')"
	case listWorkflowSortAttempts:
		orderBy = "attempts"
	case listWorkflowSortDuration:
		// Duration matches AdminWorkflowView duration semantics:
		// - no start timestamp => 0
		// - running (PENDING) => now - started_at
		// - terminal / stopped => updated_at - started_at (clamped at 0)
		// Timestamps are stored as unix milliseconds.
		now := nowMs()
		orderBy = fmt.Sprintf("CASE WHEN started_at_ms IS NULL OR started_at_ms = 0 THEN 0 WHEN status = '%s' THEN MAX(0, %d - started_at_ms) ELSE MAX(0, updated_at - started_at_ms) END", string(WorkflowStatusPending), now)
	default:
		orderBy = "created_at"
	}
	limit := in.Limit
	if limit <= 0 {
		limit = 1000
	}
	q := fmt.Sprintf(`
		SELECT workflow_uuid, status, name, output, error,
		       executor_id, application_version, application_id,
		       queue_name, deduplication_id, priority,
		       timeout_ms, deadline_ms, delay_until_ms,
		       created_at, updated_at, started_at_ms,
		       attempts, input, forked_from, parent_workflow_id, cron_schedule
		FROM workflow_status
		%s
		ORDER BY %s %s, created_at %s, workflow_uuid %s
		LIMIT ? OFFSET ?;`, where, orderBy, order, order, order)
	args = append(args, limit, in.Offset)

	var out []WorkflowStatus
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, q, args...)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			ws, err := s.scanWorkflowRow(stmt, in.LoadInputOutput)
			if err != nil {
				return err
			}
			out = append(out, *ws)
		}
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "list workflows")
	}
	return out, nil
}

func (s *sqliteSystemDB) countWorkflows(ctx context.Context, in listWorkflowsInput) (int, error) {
	where, args := listWorkflowsWhere(in)
	q := "SELECT COUNT(*) AS n FROM workflow_status " + where + ";"
	var n int
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, q, args...)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			n = int(stmt.GetInt64("n"))
		}
		return nil
	})
	if err != nil {
		return 0, wrapError(ErrUnknown, err, "count workflows")
	}
	return n, nil
}

func (s *sqliteSystemDB) updateWorkflowStatus(ctx context.Context, in updateWorkflowStatusInput) error {
	now := timeToMs(in.Now)
	if now == 0 {
		now = nowMs()
	}
	sets := []string{"status = ?", "updated_at = ?"}
	args := []any{string(in.Status), now}
	if in.Output != nil {
		sets = append(sets, "output = ?")
		args = append(args, *in.Output)
	}
	if in.ErrorString != nil {
		sets = append(sets, "error = ?")
		args = append(args, *in.ErrorString)
	}
	if in.ResetAttempts {
		sets = append(sets, "attempts = 0")
	}
	if in.BumpAttempts {
		sets = append(sets, "attempts = attempts + 1")
	}
	if in.StartedAtMs > 0 {
		sets = append(sets, "started_at_ms = ?")
		args = append(args, in.StartedAtMs)
	}
	args = append(args, in.WorkflowID)
	q := "UPDATE workflow_status SET " + strings.Join(sets, ", ") + " WHERE workflow_uuid = ?;"
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, q, args...)
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) cancelWorkflow(ctx context.Context, workflowID string) error {
	return s.updateWorkflowStatus(ctx, updateWorkflowStatusInput{
		WorkflowID: workflowID,
		Status:     WorkflowStatusCancelled,
	})
}

func (s *sqliteSystemDB) resumeWorkflow(ctx context.Context, workflowID string) error {
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, `
			UPDATE workflow_status
			SET status = ?, updated_at = ?, attempts = 0, error = NULL, output = NULL,
			    queue_name = COALESCE(queue_name, ?)
			WHERE workflow_uuid = ? AND status IN (?, ?, ?);`,
			string(WorkflowStatusEnqueued), nowMs(), internalQueueName, workflowID,
			string(WorkflowStatusCancelled), string(WorkflowStatusError), string(WorkflowStatusMaxRecoveryAttemptsExceeded))
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) deleteWorkflows(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		args := make([]any, 0, len(ids))
		for _, id := range ids {
			args = append(args, id)
		}
		q := "DELETE FROM workflow_status WHERE workflow_uuid IN (" + sqlite.Placeholders(len(ids)) + ");"
		stmt, err := conn.Prepare(ctx, q, args...)
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

// gcWorkflows performs a bulk DELETE matching the same WHERE clause as
// listWorkflows, returning the number of rows actually removed. The Limit /
// Offset / SortDescending fields on `in` are ignored — GC always operates on
// every matching row. Cascade deletes (operation_outputs, notifications,
// workflow_events) are handled by the schema's ON DELETE CASCADE.
//
// Callers (see GCWorkflows in management.go) typically pre-validate that the
// requested statuses are terminal so a long-running workflow cannot be
// removed mid-flight.
func (s *sqliteSystemDB) gcWorkflows(ctx context.Context, in listWorkflowsInput) (int, error) {
	where, args := listWorkflowsWhere(in)
	if where == "" {
		// Refuse an unbounded delete; this would wipe the whole table.
		return 0, wrapError(ErrUnknown, nil, "gc requires at least one filter")
	}
	q := "DELETE FROM workflow_status " + where + " RETURNING workflow_uuid;"
	var n int
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, q, args...)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			n++
		}
		return nil
	})
	if err != nil {
		return 0, wrapError(ErrUnknown, err, "gc workflows")
	}
	return n, nil
}

func (s *sqliteSystemDB) forkWorkflow(ctx context.Context, in forkWorkflowInput) (string, error) {
	newID := in.NewWorkflowID
	if newID == "" {
		newID = uuid.NewString()
	}
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)

		// Copy the original workflow row (status -> ENQUEUED, error/output cleared).
		stmt, err := conn.Prepare(ctx, `
			INSERT INTO workflow_status (
				workflow_uuid, status, name, output, error,
				executor_id, application_version, application_id,
				queue_name, deduplication_id, priority,
				timeout_ms, deadline_ms, delay_until_ms,
				created_at, updated_at, started_at_ms,
				attempts, input, forked_from, parent_workflow_id, cron_schedule
			)
			SELECT
				?, ?, name, NULL, NULL,
				executor_id, application_version, application_id,
				?, NULL, priority,
				timeout_ms, 0, 0,
				?, ?, 0,
				0, input, ?, NULL, NULL
			FROM workflow_status
			WHERE workflow_uuid = ?;`,
			newID, string(WorkflowStatusEnqueued), internalQueueName, nowMs(), nowMs(), in.OriginalWorkflowID, in.OriginalWorkflowID)
		if err != nil {
			return err
		}
		if _, err := stmt.Step(); err != nil {
			return err
		}

		// Copy step outputs up to and including (StartFromStep - 1).
		stmt2, err := conn.Prepare(ctx, `
			INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, created_at)
			SELECT ?, function_id, function_name, output, error, child_workflow_id, ?
			FROM operation_outputs
			WHERE workflow_uuid = ? AND function_id < ?;`,
			newID, nowMs(), in.OriginalWorkflowID, in.StartFromStep)
		if err != nil {
			return err
		}
		_, err = stmt2.Step()
		return err
	})
	if err != nil {
		return "", wrapError(ErrUnknown, err, "fork workflow")
	}
	return newID, nil
}

// ---------- operation_outputs ----------

func (s *sqliteSystemDB) checkStepOutput(ctx context.Context, workflowID string, functionID int) (*stepRecord, error) {
	var rec *stepRecord
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT workflow_uuid, function_id, function_name, output, error, child_workflow_id, created_at
			FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?;`,
			workflowID, functionID)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		out := stmt.GetText("output")
		hasOutput := !stmt.IsNull("output")
		rec = &stepRecord{
			WorkflowID:      stmt.GetText("workflow_uuid"),
			FunctionID:      int(stmt.GetInt64("function_id")),
			FunctionName:    stmt.GetText("function_name"),
			Output:          out,
			HasOutput:       hasOutput,
			ErrorString:     stmt.GetText("error"),
			ChildWorkflowID: stmt.GetText("child_workflow_id"),
			CreatedAt:       tsToTime(stmt.GetInt64("created_at")),
		}
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "check step output")
	}
	return rec, nil
}

func (s *sqliteSystemDB) recordStepOutput(ctx context.Context, in recordStepInput) error {
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, `
			INSERT INTO operation_outputs (workflow_uuid, function_id, function_name, output, error, child_workflow_id, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?);`,
			in.WorkflowID, in.FunctionID, in.FunctionName, optStr(in.Output), optStr(in.ErrorString), nullableStr(in.ChildWorkflowID), nowMs())
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) listSteps(ctx context.Context, workflowID string) ([]StepInfo, error) {
	var out []StepInfo
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT function_id, function_name, output, error, child_workflow_id, created_at
			FROM operation_outputs WHERE workflow_uuid = ? ORDER BY function_id ASC;`, workflowID)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			si := StepInfo{
				WorkflowID:      workflowID,
				StepID:          int(stmt.GetInt64("function_id")),
				StepName:        stmt.GetText("function_name"),
				ChildWorkflowID: stmt.GetText("child_workflow_id"),
				CreatedAt:       tsToTime(stmt.GetInt64("created_at")),
			}
			if !stmt.IsNull("output") {
				if v, err := s.serializer.Decode(stmt.GetText("output"), nil); err == nil {
					si.Output = v
				}
			}
			if e := stmt.GetText("error"); e != "" {
				si.Error = errors.New(e)
			}
			out = append(out, si)
		}
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "list steps")
	}
	return out, nil
}

// ---------- notifications ----------

func (s *sqliteSystemDB) enqueueNotification(ctx context.Context, in enqueueNotificationInput) error {
	if in.MessageUUID == "" {
		in.MessageUUID = uuid.NewString()
	}
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, `
			INSERT INTO notifications (destination_uuid, topic, message, message_uuid, created_at)
			VALUES (?, ?, ?, ?, ?);`,
			in.DestinationID, in.Topic, in.Message, in.MessageUUID, nowMs())
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) popNotification(ctx context.Context, destinationID, topic string) (*notificationRecord, error) {
	var rec *notificationRecord
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		// Find the oldest unconsumed notification.
		stmt, err := conn.Prepare(ctx, `
			SELECT id, topic, message, created_at
			FROM notifications
			WHERE destination_uuid = ? AND topic = ? AND consumed = 0
			ORDER BY id ASC LIMIT 1;`, destinationID, topic)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		id := stmt.GetInt64("id")
		rec = &notificationRecord{
			Topic:     stmt.GetText("topic"),
			Message:   stmt.GetText("message"),
			CreatedAt: tsToTime(stmt.GetInt64("created_at")),
		}
		// Mark consumed.
		stmt2, err := conn.Prepare(ctx, `UPDATE notifications SET consumed = 1 WHERE id = ?;`, id)
		if err != nil {
			return err
		}
		_, err = stmt2.Step()
		return err
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "pop notification")
	}
	return rec, nil
}

// ---------- events ----------

func (s *sqliteSystemDB) setEvent(ctx context.Context, in setEventInput) error {
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		stmt, err := conn.Prepare(ctx, `
			INSERT INTO workflow_events (workflow_uuid, key, value, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(workflow_uuid, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at;`,
			in.WorkflowID, in.Key, in.Value, nowMs(), nowMs())
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) getEvent(ctx context.Context, workflowID, key string) (*eventRecord, error) {
	var rec *eventRecord
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT value, created_at, updated_at
			FROM workflow_events WHERE workflow_uuid = ? AND key = ?;`, workflowID, key)
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		rec = &eventRecord{
			Value:     stmt.GetText("value"),
			CreatedAt: tsToTime(stmt.GetInt64("created_at")),
			UpdatedAt: tsToTime(stmt.GetInt64("updated_at")),
		}
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "get event")
	}
	return rec, nil
}

// ---------- queues ----------

func (s *sqliteSystemDB) dequeueWorkflows(ctx context.Context, in dequeueInput) ([]string, error) {
	// Cheap read-only pre-check first: in steady state most queue ticks find
	// nothing to dispatch, and skipping the write transaction (BEGIN
	// IMMEDIATE) keeps idle polling off the SQLite writer lock. A row that
	// lands between this check and the transaction below is simply picked up
	// on the next tick.
	hasWork := false
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM workflow_status
				WHERE queue_name = ?
				  AND (status = ? OR (status = ? AND delay_until_ms <= ?))
			) AS has_work;`,
			in.QueueName, string(WorkflowStatusEnqueued), string(WorkflowStatusDelayed), nowMs())
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			hasWork = stmt.GetInt64("has_work") > 0
		}
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "dequeue pre-check")
	}
	if !hasWork {
		return nil, nil
	}

	var ids []string
	err = s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)

		now := nowMs()

		// First, transition any DELAYED workflows whose delay_until has passed.
		st1, err := conn.Prepare(ctx, `
			UPDATE workflow_status
			SET status = ?, updated_at = ?
			WHERE queue_name = ? AND status = ? AND delay_until_ms <= ?;`,
			string(WorkflowStatusEnqueued), now, in.QueueName, string(WorkflowStatusDelayed), now)
		if err != nil {
			return err
		}
		if _, err := st1.Step(); err != nil {
			return err
		}

		// Pick candidates.
		order := "created_at ASC"
		if in.WithPriority {
			order = "priority ASC, created_at ASC"
		}
		q := fmt.Sprintf(`
			SELECT workflow_uuid FROM workflow_status
			WHERE queue_name = ? AND status = ?
			ORDER BY %s
			LIMIT ?;`, order)
		st2, err := conn.Prepare(ctx, q, in.QueueName, string(WorkflowStatusEnqueued), in.Limit)
		if err != nil {
			return err
		}
		var picked []string
		for {
			hasRow, err := st2.Step()
			if err != nil {
				st2.Reset()
				return err
			}
			if !hasRow {
				break
			}
			picked = append(picked, st2.GetText("workflow_uuid"))
		}
		st2.Reset()
		if len(picked) == 0 {
			return nil
		}

		// Claim them: PENDING + executor_id.
		args := []any{string(WorkflowStatusPending), in.ExecutorID, now, now}
		placeholders := sqlite.Placeholders(len(picked))
		for _, id := range picked {
			args = append(args, id)
		}
		st3, err := conn.Prepare(ctx, fmt.Sprintf(`
			UPDATE workflow_status
			SET status = ?, executor_id = ?, started_at_ms = ?, updated_at = ?
			WHERE workflow_uuid IN (%s);`, placeholders), args...)
		if err != nil {
			return err
		}
		if _, err := st3.Step(); err != nil {
			return err
		}
		ids = picked
		return nil
	})
	if err != nil {
		return nil, wrapError(ErrUnknown, err, "dequeue workflows")
	}
	return ids, nil
}

func (s *sqliteSystemDB) recordQueueDispatch(ctx context.Context, queueName string, workflowIDs []string) error {
	if len(workflowIDs) == 0 {
		return nil
	}
	return s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) (err error) {
		defer conn.Save(&err)
		now := nowMs()
		// Single multi-row INSERT instead of one statement per workflow.
		var sb strings.Builder
		sb.WriteString("INSERT INTO queue_dispatch_log (queue_name, workflow_uuid, dispatched_at) VALUES ")
		args := make([]any, 0, len(workflowIDs)*3)
		for i, id := range workflowIDs {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?)")
			args = append(args, queueName, id, now)
		}
		sb.WriteString(";")
		stmt, err := conn.Prepare(ctx, sb.String(), args...)
		if err != nil {
			return err
		}
		_, err = stmt.Step()
		return err
	})
}

func (s *sqliteSystemDB) countQueueDispatches(ctx context.Context, queueName string, since time.Time) (int, error) {
	var count int
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT COUNT(*) AS c FROM queue_dispatch_log
			WHERE queue_name = ? AND dispatched_at >= ?;`, queueName, timeToMs(since))
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			count = int(stmt.GetInt64("c"))
		}
		return nil
	})
	return count, err
}

func (s *sqliteSystemDB) purgeQueueDispatches(ctx context.Context, before time.Time) (int, error) {
	var deleted int
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		del, err := conn.Prepare(ctx, `
			DELETE FROM queue_dispatch_log
			WHERE dispatched_at < ?;`, timeToMs(before))
		if err != nil {
			return err
		}
		if _, err := del.Step(); err != nil {
			return err
		}
		// SQLite's changes() reports rows affected by the most recent DML
		// statement on this connection. Use it to surface a useful count.
		cs, err := conn.Prepare(ctx, `SELECT changes() AS c;`)
		if err != nil {
			return err
		}
		defer cs.Reset()
		hasRow, err := cs.Step()
		if err != nil {
			return err
		}
		if hasRow {
			deleted = int(cs.GetInt64("c"))
		}
		return nil
	})
	return deleted, err
}

func (s *sqliteSystemDB) countActiveQueueWorkflows(ctx context.Context, queueName, executorID string) (int, error) {
	var count int
	err := s.db.Exec(ctx, func(ctx context.Context, conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(ctx, `
			SELECT COUNT(*) AS c FROM workflow_status
			WHERE queue_name = ? AND executor_id = ? AND status = ?;`,
			queueName, executorID, string(WorkflowStatusPending))
		if err != nil {
			return err
		}
		defer stmt.Reset()
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			count = int(stmt.GetInt64("c"))
		}
		return nil
	})
	return count, err
}

// ---------- recovery ----------

func (s *sqliteSystemDB) listPendingWorkflows(ctx context.Context, executorID string) ([]WorkflowStatus, error) {
	return s.listWorkflows(ctx, listWorkflowsInput{
		Status:          []WorkflowStatusType{WorkflowStatusPending},
		ExecutorID:      executorID,
		LoadInputOutput: true,
		Limit:           10000,
	})
}

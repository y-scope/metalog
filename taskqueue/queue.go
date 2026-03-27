package taskqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/db"
)

const (
	maxDeadlockRetries       = 10
	DefaultMaxRetries        = 3
	DefaultCleanupBatchLimit = 1000
)

// Queue provides operations on the _task_queue table.
type Queue struct {
	db                *sql.DB
	log               *zap.Logger
	maxRetries        int
	cleanupBatchLimit int
}

// NewQueue creates a Queue with default settings.
func NewQueue(database *sql.DB, log *zap.Logger) *Queue {
	return &Queue{
		db:                database,
		log:               log,
		maxRetries:        DefaultMaxRetries,
		cleanupBatchLimit: DefaultCleanupBatchLimit,
	}
}

// SetMaxRetries sets the number of retries before a task is moved to dead_letter.
func (q *Queue) SetMaxRetries(n int) { q.maxRetries = n }

// SetCleanupBatchLimit sets the max rows deleted per CleanupOldTasks call.
// Values <= 0 are ignored (keeps the current limit).
func (q *Queue) SetCleanupBatchLimit(n int) {
	if n > 0 {
		q.cleanupBatchLimit = n
	}
}

// CreateTask inserts a new pending task and returns its ID.
func (q *Queue) CreateTask(ctx context.Context, tableName string, version uint8, input []byte) (int64, error) {
	query, args, err := sq.Insert(TableName).
		Columns("table_name", "created_at", "version", "input").
		Values(tableName, time.Now().UnixNano(), version, input).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("create task: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("create task: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("create task: last insert id: %w", err)
	}
	q.log.Debug("created task", zap.Int64("taskId", id), zap.String("table", tableName))
	return id, nil
}

// CreateTasks inserts multiple tasks in a single batch INSERT.
// Returns the number of rows inserted.
func (q *Queue) CreateTasks(ctx context.Context, tableName string, version uint8, inputs [][]byte) (int64, error) {
	if len(inputs) == 0 {
		return 0, nil
	}
	now := time.Now().UnixNano()
	builder := sq.Insert(TableName).Columns("table_name", "created_at", "version", "input")
	for _, input := range inputs {
		builder = builder.Values(tableName, now, version, input)
	}
	query, args, err := builder.ToSql()
	if err != nil {
		return 0, fmt.Errorf("create tasks: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("create tasks: %w", err)
	}
	n, _ := res.RowsAffected()
	q.log.Debug("created tasks", zap.Int64("count", n), zap.String("table", tableName))
	return n, nil
}

// ClaimTasks claims up to batchSize pending tasks for the given worker.
// If tableName is empty, tasks from any table are claimed.
// Uses READ COMMITTED isolation and retries on deadlock.
func (q *Queue) ClaimTasks(ctx context.Context, tableName string, workerID string, batchSize int) ([]*Task, error) {
	var result []*Task
	err := db.WithDeadlockRetry(ctx, maxDeadlockRetries, func() error {
		var err error
		result, err = q.claimTasksOnce(ctx, tableName, workerID, batchSize)
		return err
	})
	return result, err
}

func (q *Queue) claimTasksOnce(ctx context.Context, tableName string, workerID string, batchSize int) ([]*Task, error) {
	// Use a dedicated connection for transaction isolation control.
	conn, err := q.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim tasks: acquire connection: %w", err)
	}
	defer conn.Close()

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("claim tasks: begin tx: %w", err)
	}
	defer tx.Rollback()

	// SELECT ... FOR UPDATE SKIP LOCKED
	builder := sq.Select("task_id", "table_name", "state", "retry_count", "version", "input").
		From(TableName).
		Where(sq.Eq{"state": string(TaskStatePending)}).
		OrderBy("task_id ASC").
		Limit(uint64(batchSize)).
		Suffix("FOR UPDATE SKIP LOCKED")

	if tableName != "" {
		builder = builder.Where(sq.Eq{"table_name": tableName})
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("claim tasks: build query: %w", err)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select for update: %w", err)
	}

	var tasks []*Task
	var taskIDs []any
	for rows.Next() {
		t := &Task{}
		if err := rows.Scan(&t.TaskID, &t.TableName, &t.State, &t.RetryCount, &t.Version, &t.Input); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim tasks: scan: %w", err)
		}
		tasks = append(tasks, t)
		taskIDs = append(taskIDs, t.TaskID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim tasks: rows: %w", err)
	}

	if len(tasks) == 0 {
		return nil, tx.Commit()
	}

	// UPDATE claimed tasks to processing
	updateQuery, updateArgs, err := sq.Update(TableName).
		Set("state", string(TaskStateProcessing)).
		Set("worker_id", workerID).
		Set("claimed_at", time.Now().UnixNano()).
		Where(sq.Eq{"task_id": taskIDs}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("update claimed: build query: %w", err)
	}
	if _, err = tx.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return nil, fmt.Errorf("update claimed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim tasks: commit: %w", err)
	}

	for _, t := range tasks {
		t.WorkerID = sql.NullString{String: workerID, Valid: true}
		t.State = TaskStateProcessing
	}

	q.log.Debug("claimed tasks", zap.String("workerId", workerID), zap.Int("count", len(tasks)))
	return tasks, nil
}

// CompleteTask marks a task as completed with optional output.
func (q *Queue) CompleteTask(ctx context.Context, taskID int64, output []byte) (int64, error) {
	builder := sq.Update(TableName).
		Set("state", string(TaskStateCompleted)).
		Set("completed_at", time.Now().UnixNano()).
		Where(sq.Eq{"task_id": taskID, "state": string(TaskStateProcessing)})
	if output != nil {
		builder = builder.Set("output", output)
	}
	query, args, err := builder.ToSql()
	if err != nil {
		return 0, fmt.Errorf("complete task: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("complete task: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, fmt.Errorf("complete task %d: no matching task in processing state", taskID)
	}
	q.log.Debug("task completed", zap.Int64("taskId", taskID))
	return n, nil
}

// FailTask marks a task as failed. If the task has exceeded the maximum retry
// count, it is moved to dead_letter instead.
// Uses a single atomic UPDATE with conditional state selection to avoid TOCTOU races.
// Note: In a single UPDATE, all SET expressions evaluate against the pre-update row,
// so the IF must compare retry_count + 1 (the post-increment value) against maxRetries.
func (q *Queue) FailTask(ctx context.Context, taskID int64) (int64, error) {
	query, args, err := sq.Update(TableName).
		Set("retry_count", sq.Expr("retry_count + 1")).
		Set("state", sq.Expr("IF(retry_count + 1 >= ?, 'dead_letter', 'failed')", q.maxRetries)).
		Set("completed_at", time.Now().UnixNano()).
		Where(sq.Eq{"task_id": taskID, "state": string(TaskStateProcessing)}).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("fail task: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("fail task: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, fmt.Errorf("fail task %d: no matching task in processing state", taskID)
	}

	// Check if it ended up in dead_letter for logging purposes.
	checkQuery, checkArgs, _ := sq.Select("state").
		From(TableName).
		Where(sq.Eq{"task_id": taskID}).
		ToSql()
	var state string
	if scanErr := q.db.QueryRowContext(ctx, checkQuery, checkArgs...).Scan(&state); scanErr == nil && state == "dead_letter" {
		q.log.Warn("task moved to dead letter on failure", zap.Int64("taskId", taskID))
	}

	return n, nil
}

// FindStaleTasks returns processing tasks older than the given timeout.
func (q *Queue) FindStaleTasks(ctx context.Context, tableName string, timeout time.Duration) ([]*Task, error) {
	cutoff := time.Now().Add(-timeout).UnixNano()
	query, args, err := sq.Select("task_id", "table_name", "state", "retry_count", "input", "worker_id").
		From(TableName).
		Where(sq.Eq{"table_name": tableName, "state": string(TaskStateProcessing)}).
		Where(sq.LtOrEq{"claimed_at": cutoff}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("find stale tasks: build query: %w", err)
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find stale tasks: %w", err)
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		t := &Task{}
		if err := rows.Scan(&t.TaskID, &t.TableName, &t.State, &t.RetryCount, &t.Input, &t.WorkerID); err != nil {
			return nil, fmt.Errorf("find stale tasks: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("find stale tasks: rows: %w", err)
	}
	return tasks, nil
}

// ReclaimTask marks a stale processing task as timed_out and re-enqueues it,
// or moves it to dead_letter if max retries are exceeded.
// Uses an atomic IF() expression to read retry_count from the row itself,
// avoiding a TOCTOU race with concurrent FailTask calls.
func (q *Queue) ReclaimTask(ctx context.Context, taskID int64) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reclaim task: begin tx: %w", err)
	}
	defer tx.Rollback()

	nowNano := time.Now().UnixNano()
	updateQuery, updateArgs, err := sq.Update(TableName).
		Set("retry_count", sq.Expr("retry_count + 1")).
		Set("state", sq.Expr("IF(retry_count + 1 >= ?, 'dead_letter', 'timed_out')", q.maxRetries)).
		Set("completed_at", nowNano).
		Where(sq.Eq{"task_id": taskID, "state": string(TaskStateProcessing)}).
		ToSql()
	if err != nil {
		return fmt.Errorf("reclaim update: build query: %w", err)
	}
	res, err := tx.ExecContext(ctx, updateQuery, updateArgs...)
	if err != nil {
		return fmt.Errorf("reclaim update: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("reclaim task %d: no matching task in processing state", taskID)
	}

	// Check resulting state: if dead_letter, commit without re-enqueue.
	var state string
	if err := tx.QueryRowContext(ctx,
		"SELECT state FROM "+TableName+" WHERE task_id = ?", taskID).Scan(&state); err != nil {
		return fmt.Errorf("reclaim task: check state: %w", err)
	}
	if state == "dead_letter" {
		q.log.Warn("task moved to dead letter on reclaim", zap.Int64("taskId", taskID))
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("reclaim task: commit: %w", err)
		}
		return nil
	}

	// Re-enqueue: copy input from old task into new pending task.
	// retry_count was already incremented on the timed_out row, so copy it as-is.
	insertQuery, insertArgs, err := sq.Insert(TableName).
		Columns("table_name", "created_at", "input", "retry_count", "version").
		Select(
			sq.Select("table_name", fmt.Sprintf("%d", nowNano), "input", "retry_count", "version").
				From(TableName).
				Where(sq.Eq{"task_id": taskID}),
		).
		ToSql()
	if err != nil {
		return fmt.Errorf("reclaim re-enqueue: build query: %w", err)
	}
	if _, err = tx.ExecContext(ctx, insertQuery, insertArgs...); err != nil {
		return fmt.Errorf("reclaim re-enqueue: %w", err)
	}

	q.log.Info("reclaimed stale task", zap.Int64("taskId", taskID))
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("reclaim task: commit: %w", err)
	}
	return nil
}

// CleanupOldTasks deletes terminal tasks (completed, failed, timed_out) older
// than maxAge. Dead-letter tasks are preserved for manual investigation.
func (q *Queue) CleanupOldTasks(ctx context.Context, tableName string, maxAge time.Duration) (int64, error) {
	cutoff := time.Now().Add(-maxAge).UnixNano()
	query, args, err := sq.Delete(TableName).
		Where(sq.Eq{
			"table_name": tableName,
			"state":      []string{string(TaskStateCompleted), string(TaskStateFailed), string(TaskStateTimedOut)},
		}).
		Where(sq.Lt{"completed_at": cutoff}).
		Suffix(fmt.Sprintf("LIMIT %d", q.cleanupBatchLimit)).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("cleanup old tasks: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("cleanup old tasks: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// GetTaskCounts returns task counts grouped by state for a table.
func (q *Queue) GetTaskCounts(ctx context.Context, tableName string) (*TaskCounts, error) {
	query, args, err := sq.Select("state", "COUNT(*)").
		From(TableName).
		Where(sq.Eq{"table_name": tableName}).
		GroupBy("state").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("get task counts: build query: %w", err)
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get task counts: %w", err)
	}
	defer rows.Close()

	counts := &TaskCounts{}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("get task counts: scan: %w", err)
		}
		switch TaskState(state) {
		case TaskStatePending:
			counts.Pending = count
		case TaskStateProcessing:
			counts.Processing = count
		case TaskStateCompleted:
			counts.Completed = count
		case TaskStateFailed:
			counts.Failed = count
		case TaskStateTimedOut:
			counts.TimedOut = count
		case TaskStateDeadLetter:
			counts.DeadLetter = count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get task counts: rows: %w", err)
	}
	return counts, nil
}

// terminalStates are the states considered terminal for planner processing.
// Note: timed_out is excluded because ReclaimTask handles re-enqueueing those
// tasks. They are cleaned up by CleanupOldTasks if re-enqueue fails.
var terminalStates = []string{
	string(TaskStateCompleted),
	string(TaskStateFailed),
	string(TaskStateDeadLetter),
}

// TerminalTask holds the ID and payloads of a completed, failed, or dead-letter task.
type TerminalTask struct {
	TaskID int64
	Input  []byte
	Output []byte
}

// FindTerminalTasks returns up to limit tasks in terminal states for the given table.
func (q *Queue) FindTerminalTasks(ctx context.Context, tableName string, limit int) ([]TerminalTask, error) {
	query, args, err := sq.Select("task_id", "input", "output").
		From(TableName).
		Where(sq.Eq{"table_name": tableName, "state": terminalStates}).
		Limit(uint64(limit)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("find terminal tasks: build query: %w", err)
	}
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("find terminal tasks: %w", err)
	}
	defer rows.Close()

	var tasks []TerminalTask
	for rows.Next() {
		var t TerminalTask
		if err := rows.Scan(&t.TaskID, &t.Input, &t.Output); err != nil {
			return nil, fmt.Errorf("find terminal tasks: scan: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// DeleteTerminalTask deletes a single task that is in a terminal state.
func (q *Queue) DeleteTerminalTask(ctx context.Context, taskID int64) error {
	query, args, _ := sq.Delete(TableName).
		Where(sq.Eq{"task_id": taskID, "state": terminalStates}).
		ToSql()
	_, err := q.db.ExecContext(ctx, query, args...)
	return err
}

// DeleteAllTasks removes all tasks for a table (used on coordinator restart).
func (q *Queue) DeleteAllTasks(ctx context.Context, tableName string) (int64, error) {
	query, args, err := sq.Delete(TableName).
		Where(sq.Eq{"table_name": tableName}).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("delete all tasks: build query: %w", err)
	}
	res, err := q.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete all tasks: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

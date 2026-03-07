package taskqueue

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"go.uber.org/zap"

	"github.com/y-scope/metalog/internal/db"
)

const (
	maxDeadlockRetries = 10
	defaultMaxRetries  = 3
)

// Queue provides operations on the _task_queue table.
type Queue struct {
	db  *sql.DB
	log *zap.Logger
}

// NewQueue creates a Queue.
func NewQueue(database *sql.DB, log *zap.Logger) *Queue {
	return &Queue{db: database, log: log}
}

// CreateTask inserts a new pending task and returns its ID.
func (q *Queue) CreateTask(ctx context.Context, tableName string, input []byte) (int64, error) {
	res, err := q.db.ExecContext(ctx,
		"INSERT INTO "+TableName+" (table_name, input) VALUES (?, ?)",
		tableName, input,
	)
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

	// SELECT ... FOR UPDATE
	builder := sq.Select("task_id", "table_name", "state", "retry_count", "input").
		From(TableName).
		Where(sq.Eq{"state": string(TaskStatePending)}).
		OrderBy("task_id ASC").
		Limit(uint64(batchSize))

	if tableName != "" {
		builder = builder.Where(sq.Eq{"table_name": tableName})
	}

	query, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("claim tasks: build query: %w", err)
	}
	query += " FOR UPDATE SKIP LOCKED"

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select for update: %w", err)
	}

	var tasks []*Task
	var taskIDs []any
	for rows.Next() {
		t := &Task{}
		if err := rows.Scan(&t.TaskID, &t.TableName, &t.State, &t.RetryCount, &t.Input); err != nil {
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
		_ = tx.Commit()
		return nil, nil
	}

	// UPDATE claimed tasks to processing
	placeholders := joinRepeat("?", len(taskIDs), ",")
	updateArgs := make([]any, 0, 2+len(taskIDs))
	updateArgs = append(updateArgs, workerID)
	updateArgs = append(updateArgs, taskIDs...)

	_, err = tx.ExecContext(ctx,
		"UPDATE "+TableName+" SET state = 'processing', worker_id = ?, claimed_at = UNIX_TIMESTAMP() WHERE task_id IN ("+placeholders+")",
		updateArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("update claimed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim tasks: commit: %w", err)
	}

	for _, t := range tasks {
		t.WorkerID = sql.NullString{String: workerID, Valid: true}
		t.State = TaskStateProcessing
	}

	q.log.Debug("claimed tasks", zap.String("worker", workerID), zap.Int("count", len(tasks)))
	return tasks, nil
}

// CompleteTask marks a task as completed with optional output.
func (q *Queue) CompleteTask(ctx context.Context, taskID int64, output []byte) (int64, error) {
	var res sql.Result
	var err error
	if output != nil {
		res, err = q.db.ExecContext(ctx,
			"UPDATE "+TableName+" SET state = 'completed', completed_at = UNIX_TIMESTAMP(), output = ? WHERE task_id = ? AND state = 'processing'",
			output, taskID,
		)
	} else {
		res, err = q.db.ExecContext(ctx,
			"UPDATE "+TableName+" SET state = 'completed', completed_at = UNIX_TIMESTAMP() WHERE task_id = ? AND state = 'processing'",
			taskID,
		)
	}
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
func (q *Queue) FailTask(ctx context.Context, taskID int64) (int64, error) {
	res, err := q.db.ExecContext(ctx,
		"UPDATE "+TableName+" SET "+
			"retry_count = retry_count + 1, "+
			"state = IF(retry_count >= ?, 'dead_letter', 'failed'), "+
			"completed_at = UNIX_TIMESTAMP() "+
			"WHERE task_id = ? AND state = 'processing'",
		defaultMaxRetries, taskID,
	)
	if err != nil {
		return 0, fmt.Errorf("fail task: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, fmt.Errorf("fail task %d: no matching task in processing state", taskID)
	}

	// Check if it ended up in dead_letter for logging purposes.
	var state string
	if scanErr := q.db.QueryRowContext(ctx,
		"SELECT state FROM "+TableName+" WHERE task_id = ?", taskID,
	).Scan(&state); scanErr == nil && state == "dead_letter" {
		q.log.Warn("task moved to dead letter on failure", zap.Int64("taskId", taskID))
	}

	return n, nil
}

// FindStaleTasks returns processing tasks older than timeoutSeconds.
func (q *Queue) FindStaleTasks(ctx context.Context, tableName string, timeoutSeconds int) ([]*Task, error) {
	rows, err := q.db.QueryContext(ctx,
		"SELECT task_id, table_name, state, retry_count, input, worker_id FROM "+TableName+
			" WHERE table_name = ? AND state = 'processing' AND claimed_at <= UNIX_TIMESTAMP() - ?",
		tableName, timeoutSeconds,
	)
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

// ReclaimTask marks a stale processing task as timed_out and creates a new
// pending task with incremented retry count. If max retries exceeded, moves
// to dead_letter.
func (q *Queue) ReclaimTask(ctx context.Context, taskID int64, retryCount uint8) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("reclaim task: begin tx: %w", err)
	}
	defer tx.Rollback()

	newState := "timed_out"
	if int(retryCount) >= defaultMaxRetries {
		newState = "dead_letter"
	}

	res, err := tx.ExecContext(ctx,
		"UPDATE "+TableName+" SET state = ?, completed_at = UNIX_TIMESTAMP() WHERE task_id = ? AND state = 'processing'",
		newState, taskID,
	)
	if err != nil {
		return fmt.Errorf("reclaim update: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("reclaim task %d: no matching task in processing state", taskID)
	}

	if newState == "dead_letter" {
		q.log.Warn("task moved to dead letter", zap.Int64("taskId", taskID), zap.Uint8("retries", retryCount))
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("reclaim task: commit: %w", err)
		}
		return nil
	}

	// Re-enqueue: copy input from old task into new pending task
	_, err = tx.ExecContext(ctx,
		"INSERT INTO "+TableName+" (table_name, input, retry_count) "+
			"SELECT table_name, input, retry_count + 1 FROM "+TableName+" WHERE task_id = ?",
		taskID,
	)
	if err != nil {
		return fmt.Errorf("reclaim re-enqueue: %w", err)
	}

	q.log.Info("reclaimed stale task", zap.Int64("taskId", taskID), zap.Uint8("retry", retryCount+1))
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("reclaim task: commit: %w", err)
	}
	return nil
}

// CleanupOldTasks deletes completed/failed/timed_out tasks older than maxAge.
func (q *Queue) CleanupOldTasks(ctx context.Context, tableName string, maxAge time.Duration) (int64, error) {
	cutoff := time.Now().Add(-maxAge).Unix()
	res, err := q.db.ExecContext(ctx,
		"DELETE FROM "+TableName+
			" WHERE table_name = ? AND state IN ('completed','failed','timed_out') AND completed_at < ? LIMIT 1000",
		tableName, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup old tasks: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// GetTaskCounts returns task counts grouped by state for a table.
func (q *Queue) GetTaskCounts(ctx context.Context, tableName string) (*TaskCounts, error) {
	rows, err := q.db.QueryContext(ctx,
		"SELECT state, COUNT(*) FROM "+TableName+" WHERE table_name = ? GROUP BY state",
		tableName,
	)
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

// DeleteAllTasks removes all tasks for a table (used on coordinator restart).
func (q *Queue) DeleteAllTasks(ctx context.Context, tableName string) (int64, error) {
	res, err := q.db.ExecContext(ctx,
		"DELETE FROM "+TableName+" WHERE table_name = ?", tableName,
	)
	if err != nil {
		return 0, fmt.Errorf("delete all tasks: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// joinRepeat repeats s n times, separated by sep.
func joinRepeat(s string, n int, sep string) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := range parts {
		parts[i] = s
	}
	return strings.Join(parts, sep)
}

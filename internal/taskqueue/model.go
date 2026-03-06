package taskqueue

import (
	"database/sql"
)

// TaskState represents the state of a task in the task queue.
type TaskState string

const (
	TaskStatePending    TaskState = "pending"
	TaskStateProcessing TaskState = "processing"
	TaskStateCompleted  TaskState = "completed"
	TaskStateFailed     TaskState = "failed"
	TaskStateTimedOut   TaskState = "timed_out"
	TaskStateDeadLetter TaskState = "dead_letter"
)

// Task represents a row in the _task_queue table.
type Task struct {
	TaskID      int64
	TableName   string
	State       TaskState
	WorkerID    sql.NullString
	CreatedAt   uint32
	ClaimedAt   sql.NullInt32
	CompletedAt sql.NullInt32
	RetryCount  uint8
	Input       []byte
	Output      []byte
}

// TaskCounts holds counts of tasks grouped by state.
type TaskCounts struct {
	Pending    int
	Processing int
	Completed  int
	Failed     int
	TimedOut   int
	DeadLetter int
}

// TableName is the SQL table name for the task queue.
const TableName = "_task_queue"

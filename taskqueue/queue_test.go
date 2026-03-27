package taskqueue

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"go.uber.org/zap"
)

func newMockQueue(t *testing.T) (*Queue, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	q := NewQueue(db, zap.NewNop())
	return q, mock, db
}

func TestSetMaxRetries(t *testing.T) {
	q, _, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck
	q.SetMaxRetries(5)
	if q.maxRetries != 5 {
		t.Errorf("maxRetries = %d, want 5", q.maxRetries)
	}
}

func TestSetCleanupBatchLimit(t *testing.T) {
	q, _, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	q.SetCleanupBatchLimit(500)
	if q.cleanupBatchLimit != 500 {
		t.Errorf("cleanupBatchLimit = %d, want 500", q.cleanupBatchLimit)
	}

	// Zero or negative should be ignored.
	q.SetCleanupBatchLimit(0)
	if q.cleanupBatchLimit != 500 {
		t.Errorf("cleanupBatchLimit changed to %d, should remain 500", q.cleanupBatchLimit)
	}
	q.SetCleanupBatchLimit(-1)
	if q.cleanupBatchLimit != 500 {
		t.Errorf("cleanupBatchLimit changed to %d, should remain 500", q.cleanupBatchLimit)
	}
}

func TestCreateTask_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(42, 1))

	id, err := q.CreateTask(context.Background(), "table1", 1, []byte("data"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
}

func TestCreateTask_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("db error"))

	_, err := q.CreateTask(context.Background(), "table1", 1, []byte("data"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCreateTasks_Empty(t *testing.T) {
	q, _, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	n, err := q.CreateTasks(context.Background(), "table1", 1, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
}

func TestCreateTasks_Multiple(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 3))

	inputs := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	n, err := q.CreateTasks(context.Background(), "table1", 1, inputs)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 3 {
		t.Errorf("n = %d, want 3", n)
	}
}

func TestCreateTasks_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("db error"))

	_, err := q.CreateTasks(context.Background(), "table1", 1, [][]byte{[]byte("a")})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClaimTasks_EmptyResult(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	cols := []string{"task_id", "table_name", "state", "retry_count", "version", "input"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectCommit()

	tasks, err := q.ClaimTasks(context.Background(), "table1", "worker-1", 10)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if tasks != nil {
		t.Errorf("expected nil, got %d tasks", len(tasks))
	}
}

func TestClaimTasks_WithTasks(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	cols := []string{"task_id", "table_name", "state", "retry_count", "version", "input"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "table1", "pending", 0, 1, []byte("data1")).
		AddRow(2, "table1", "pending", 0, 1, []byte("data2"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	tasks, err := q.ClaimTasks(context.Background(), "table1", "worker-1", 10)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	if tasks[0].State != TaskStateProcessing {
		t.Errorf("task state = %q, want processing", tasks[0].State)
	}
}

func TestClaimTasks_AllTables(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	cols := []string{"task_id", "table_name", "state", "retry_count", "version", "input"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))
	mock.ExpectCommit()

	// Empty tableName = claim from any table.
	_, err := q.ClaimTasks(context.Background(), "", "worker-1", 10)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestCompleteTask_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := q.CompleteTask(context.Background(), 1, []byte("output"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestCompleteTask_NilOutput(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := q.CompleteTask(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestCompleteTask_NotFound(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := q.CompleteTask(context.Background(), 99, nil)
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestFailTask_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	// State check query (to detect dead_letter)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"state"}).AddRow("failed"),
	)

	n, err := q.FailTask(context.Background(), 1)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestFailTask_DeadLetter(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"state"}).AddRow("dead_letter"),
	)

	n, err := q.FailTask(context.Background(), 1)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 1 {
		t.Errorf("n = %d, want 1", n)
	}
}

func TestFailTask_NotFound(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := q.FailTask(context.Background(), 99)
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestFindStaleTasks_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	cols := []string{"task_id", "table_name", "state", "retry_count", "input", "worker_id"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "table1", "processing", 0, []byte("data"),
			sql.NullString{String: "w1", Valid: true})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tasks, err := q.FindStaleTasks(context.Background(), "table1", 1*time.Hour)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
}

func TestFindStaleTasks_Empty(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	cols := []string{"task_id", "table_name", "state", "retry_count", "input", "worker_id"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(cols))

	tasks, err := q.FindStaleTasks(context.Background(), "table1", 1*time.Hour)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("got %d tasks, want 0", len(tasks))
	}
}

func TestFindStaleTasks_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("db error"))

	_, err := q.FindStaleTasks(context.Background(), "table1", 1*time.Hour)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimTask_ReEnqueue(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	// State check: timed_out (not dead_letter)
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"state"}).AddRow("timed_out"),
	)
	// Re-enqueue INSERT INTO ... SELECT
	mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()

	err := q.ReclaimTask(context.Background(), 1)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestReclaimTask_DeadLetter(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"state"}).AddRow("dead_letter"),
	)
	mock.ExpectCommit()

	err := q.ReclaimTask(context.Background(), 1)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestReclaimTask_NotFound(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := q.ReclaimTask(context.Background(), 99)
	if err == nil {
		t.Fatal("expected error for non-existent task")
	}
}

func TestCleanupOldTasks_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 10))

	n, err := q.CleanupOldTasks(context.Background(), "table1", 24*time.Hour)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 10 {
		t.Errorf("n = %d, want 10", n)
	}
}

func TestCleanupOldTasks_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("DELETE FROM").WillReturnError(errors.New("db error"))

	_, err := q.CleanupOldTasks(context.Background(), "table1", 24*time.Hour)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTaskCounts_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	rows := sqlmock.NewRows([]string{"state", "count"}).
		AddRow("pending", 5).
		AddRow("processing", 3).
		AddRow("completed", 10).
		AddRow("failed", 2).
		AddRow("timed_out", 1).
		AddRow("dead_letter", 0)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	counts, err := q.GetTaskCounts(context.Background(), "table1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if counts.Pending != 5 {
		t.Errorf("Pending = %d, want 5", counts.Pending)
	}
	if counts.Processing != 3 {
		t.Errorf("Processing = %d, want 3", counts.Processing)
	}
	if counts.Completed != 10 {
		t.Errorf("Completed = %d, want 10", counts.Completed)
	}
	if counts.Failed != 2 {
		t.Errorf("Failed = %d, want 2", counts.Failed)
	}
	if counts.TimedOut != 1 {
		t.Errorf("TimedOut = %d, want 1", counts.TimedOut)
	}
}

func TestGetTaskCounts_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("db error"))

	_, err := q.GetTaskCounts(context.Background(), "table1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFindTerminalTasks_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	cols := []string{"task_id", "input", "output"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, []byte("in1"), []byte("out1")).
		AddRow(2, []byte("in2"), nil)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	tasks, err := q.FindTerminalTasks(context.Background(), "table1", 10)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	if tasks[0].TaskID != 1 {
		t.Errorf("task 0 ID = %d, want 1", tasks[0].TaskID)
	}
}

func TestFindTerminalTasks_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectQuery("SELECT").WillReturnError(errors.New("db error"))

	_, err := q.FindTerminalTasks(context.Background(), "table1", 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDeleteTerminalTask_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 1))

	err := q.DeleteTerminalTask(context.Background(), 1)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
}

func TestDeleteAllTasks_Success(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(0, 5))

	n, err := q.DeleteAllTasks(context.Background(), "table1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if n != 5 {
		t.Errorf("n = %d, want 5", n)
	}
}

func TestDeleteAllTasks_Error(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("DELETE FROM").WillReturnError(errors.New("db error"))

	_, err := q.DeleteAllTasks(context.Background(), "table1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUnmarshalPayload_Error(t *testing.T) {
	_, err := UnmarshalPayload([]byte("invalid"))
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

func TestUnmarshalResult_Error(t *testing.T) {
	_, err := UnmarshalResult([]byte("invalid"))
	if err == nil {
		t.Fatal("expected error for invalid result")
	}
}

func TestClaimTasks_QueryError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("select error"))
	mock.ExpectRollback()

	_, err := q.ClaimTasks(context.Background(), "table1", "w1", 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClaimTasks_UpdateError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	cols := []string{"task_id", "table_name", "state", "retry_count", "version", "input"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "table1", "pending", 0, 1, []byte("data"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("UPDATE").WillReturnError(errors.New("update error"))
	mock.ExpectRollback()

	_, err := q.ClaimTasks(context.Background(), "table1", "w1", 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClaimTasks_CommitError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	cols := []string{"task_id", "table_name", "state", "retry_count", "version", "input"}
	rows := sqlmock.NewRows(cols).
		AddRow(1, "table1", "pending", 0, 1, []byte("data"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("commit error"))

	_, err := q.ClaimTasks(context.Background(), "table1", "w1", 10)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCompleteTask_ExecError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnError(errors.New("exec error"))

	_, err := q.CompleteTask(context.Background(), 1, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFailTask_ExecError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectExec("UPDATE").WillReturnError(errors.New("exec error"))

	_, err := q.FailTask(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimTask_BeginTxError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin().WillReturnError(errors.New("begin error"))

	err := q.ReclaimTask(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimTask_UpdateError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnError(errors.New("update error"))
	mock.ExpectRollback()

	err := q.ReclaimTask(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimTask_StateCheckError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("select error"))
	mock.ExpectRollback()

	err := q.ReclaimTask(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestReclaimTask_ReEnqueueError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"state"}).AddRow("timed_out"),
	)
	mock.ExpectExec("INSERT INTO").WillReturnError(errors.New("insert error"))
	mock.ExpectRollback()

	err := q.ReclaimTask(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTaskCounts_ScanError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	// Return rows with wrong column types to trigger scan error
	rows := sqlmock.NewRows([]string{"state", "count"}).
		AddRow(nil, nil) // nil will cause scan error for string
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	_, err := q.GetTaskCounts(context.Background(), "table1")
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestFindTerminalTasks_ScanError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	// Wrong number of columns to trigger scan error
	rows := sqlmock.NewRows([]string{"task_id", "input"}).
		AddRow(1, []byte("data"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	_, err := q.FindTerminalTasks(context.Background(), "table1", 10)
	if err == nil {
		t.Fatal("expected scan error")
	}
}

func TestFindStaleTasks_ScanError(t *testing.T) {
	q, mock, db := newMockQueue(t)
	defer db.Close() //nolint:errcheck

	// Wrong columns to trigger scan error
	rows := sqlmock.NewRows([]string{"task_id"}).AddRow(1)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	_, err := q.FindStaleTasks(context.Background(), "table1", time.Hour)
	if err == nil {
		t.Fatal("expected scan error")
	}
}

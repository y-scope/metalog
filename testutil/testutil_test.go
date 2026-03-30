package testutil

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/y-scope/metalog/schema"
)

func TestConcatCompressor_Compress(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Create two input files.
	if err := os.WriteFile(filepath.Join(inputDir, "a.log"), []byte("aaa"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputDir, "b.log"), []byte("bbb"), 0644); err != nil {
		t.Fatal(err)
	}
	// Create a subdirectory (should be skipped).
	if err := os.Mkdir(filepath.Join(inputDir, "subdir"), 0755); err != nil {
		t.Fatal(err)
	}

	c := &ConcatCompressor{}
	if err := c.Compress(context.Background(), inputDir, outputDir); err != nil {
		t.Fatal(err)
	}

	out, err := os.ReadFile(filepath.Join(outputDir, "archive.clp.zst"))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 6 {
		t.Errorf("output size = %d, want 6", len(out))
	}
}

func TestConcatCompressor_Compress_BadInputDir(t *testing.T) {
	c := &ConcatCompressor{}
	err := c.Compress(context.Background(), "/nonexistent", t.TempDir())
	if err == nil {
		t.Error("expected error for nonexistent input dir")
	}
}

func TestConcatCompressor_Compress_BadOutputDir(t *testing.T) {
	c := &ConcatCompressor{}
	err := c.Compress(context.Background(), t.TempDir(), "/nonexistent/path")
	if err == nil {
		t.Error("expected error for nonexistent output dir")
	}
}

func TestWaitFor_ImmediateSuccess(t *testing.T) {
	WaitFor(t, time.Second, "should pass immediately", func() bool {
		return true
	})
}

func TestWaitFor_EventualSuccess(t *testing.T) {
	start := time.Now()
	call := 0
	WaitFor(t, time.Second, "should pass on second call", func() bool {
		call++
		return call >= 2
	})
	if time.Since(start) < 50*time.Millisecond {
		t.Error("should have polled at least once")
	}
}

func TestWaitFor_Timeout(t *testing.T) {
	if waitFor(150*time.Millisecond, func() bool { return false }) {
		t.Error("expected waitFor to return false on timeout")
	}
}

// --- splitStatements / truncate unit tests ---

func TestSplitStatements(t *testing.T) {
	input := "-- comment\nCREATE TABLE t1 (id INT);\nINSERT INTO t1 VALUES (1);"
	stmts := splitStatements(input)

	// Should strip comments and split on semicolons
	nonEmpty := 0
	for _, s := range stmts {
		if len(s) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty < 2 {
		t.Errorf("expected at least 2 non-empty statements, got %d", nonEmpty)
	}
}

func TestSplitStatements_NoComments(t *testing.T) {
	input := "SELECT 1;\nSELECT 2;"
	stmts := splitStatements(input)
	if len(stmts) < 2 {
		t.Errorf("expected at least 2 statements, got %d", len(stmts))
	}
}

func TestSplitStatements_AllComments(t *testing.T) {
	input := "-- line1\n-- line2\n"
	stmts := splitStatements(input)
	// All lines are comments; split on ";" of empty string produces [""]
	for _, s := range stmts {
		trimmed := ""
		for _, c := range s {
			if c != '\n' && c != ' ' {
				trimmed += string(c)
			}
		}
		if trimmed != "" {
			t.Errorf("expected only empty statements after stripping comments, got %q", s)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input  string
		want   string
		maxLen int
	}{
		{"short", "short", 10},
		{"exactly10!", "exactly10!", 10},
		{"this is too long", "this ...", 5},
		{"", "", 5},
	}
	for _, tc := range tests {
		got := truncate(tc.input, tc.maxLen)
		if got != tc.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tc.input, tc.maxLen, got, tc.want)
		}
	}
}

func TestDBContainer_Teardown_NilFields(t *testing.T) {
	mc := &DBContainer{}
	// Should not panic with nil DB and Container.
	mc.Teardown(t)
}

func TestDBContainer_Teardown_WithDB(t *testing.T) {
	db, _, _ := sqlmock.New()
	mc := &DBContainer{DB: db}
	mc.Teardown(t)
	// DB should be closed after Teardown.
	if err := db.Ping(); err == nil {
		t.Error("expected DB to be closed after Teardown")
	}
}

func TestDBContainer_CreateTestTable(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	// CreateTestTable executes 4 statements.
	mock.ExpectExec("INSERT IGNORE INTO _table").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_config").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT IGNORE INTO _table_assignment").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS").WillReturnResult(sqlmock.NewResult(0, 0))

	mc := &DBContainer{DB: db}
	mc.CreateTestTable(t, "my_test_table")
}

func TestSetupDB(t *testing.T) {
	mc := SetupDB(t)
	defer mc.Teardown(t)
	if mc.DB == nil || mc.DSN == "" {
		t.Fatal("DB or DSN is empty")
	}
	if err := mc.DB.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestTeardown_NilFields(t *testing.T) {
	mc := &DBContainer{}
	mc.Teardown(t)
}

func TestDBContainer_LoadSchema(t *testing.T) {
	db, mock, _ := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	defer db.Close() //nolint:errcheck

	// LoadSchema splits schema.SchemaSQL and executes each non-empty statement.
	// We can't predict the exact count, so use MatchExpectationsInOrder(false)
	// and allow any number of exec calls.
	mock.MatchExpectationsInOrder(false)

	// Get the number of expected statements
	stmts := splitStatements(schema.SchemaSQL)
	execCount := 0
	for _, s := range stmts {
		trimmed := ""
		for _, c := range s {
			if c != '\n' && c != ' ' && c != '\t' && c != '\r' {
				trimmed += string(c)
			}
		}
		if trimmed != "" {
			execCount++
			mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
		}
	}

	mc := &DBContainer{DB: db}
	mc.LoadSchema(t)
}

func TestLoadSchemaAndCreateTable(t *testing.T) {
	mc := SetupDB(t)
	defer mc.Teardown(t)
	mc.LoadSchema(t)
	mc.CreateTestTable(t, "test_logs")
	var count int
	if err := mc.DB.QueryRow("SELECT COUNT(*) FROM test_logs").Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
}

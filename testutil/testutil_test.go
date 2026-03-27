package testutil

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestSplitStatements(t *testing.T) {
	stmts := splitStatements("-- comment\nCREATE TABLE t1 (id INT);\nINSERT INTO t1 VALUES (1);")
	if len(stmts) < 2 {
		t.Errorf("expected at least 2 statements, got %d", len(stmts))
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 3); got != "hel..." {
		t.Errorf("truncate(hello,3) = %q", got)
	}
	if got := truncate("hi", 10); got != "hi" {
		t.Errorf("truncate(hi,10) = %q", got)
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

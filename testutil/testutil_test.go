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

func TestConcatCompressor_Compress_UnreadableFile(t *testing.T) {
	inputDir := t.TempDir()
	outputDir := t.TempDir()

	// Create a file then make it unreadable.
	path := filepath.Join(inputDir, "secret.log")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0000); err != nil {
		t.Fatal(err)
	}

	c := &ConcatCompressor{}
	if err := c.Compress(context.Background(), inputDir, outputDir); err == nil {
		t.Error("expected error for unreadable file")
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

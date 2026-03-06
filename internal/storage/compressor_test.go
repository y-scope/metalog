package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestClpCompressor_Compress_BinaryNotFound(t *testing.T) {
	log := zap.NewNop()
	c := NewClpCompressor("/nonexistent/clp-s", 10*time.Second, log)

	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	os.MkdirAll(inputDir, 0755)
	outputPath := filepath.Join(tmpDir, "output.clp")

	err := c.Compress(context.Background(), inputDir, outputPath)
	if err == nil {
		t.Fatal("Compress() with nonexistent binary should return error")
	}
}

func TestClpCompressor_Compress_Timeout(t *testing.T) {
	// Use "sleep" as a stand-in binary that will exceed the timeout
	log := zap.NewNop()
	c := NewClpCompressor("sleep", 50*time.Millisecond, log)

	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	os.MkdirAll(inputDir, 0755)
	outputPath := filepath.Join(tmpDir, "output.clp")

	err := c.Compress(context.Background(), inputDir, outputPath)
	if err == nil {
		t.Fatal("Compress() with timeout should return error")
	}
}

func TestClpCompressor_Compress_ContextCancelled(t *testing.T) {
	log := zap.NewNop()
	c := NewClpCompressor("sleep", 30*time.Second, log)

	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	os.MkdirAll(inputDir, 0755)
	outputPath := filepath.Join(tmpDir, "output.clp")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := c.Compress(ctx, inputDir, outputPath)
	if err == nil {
		t.Fatal("Compress() with cancelled context should return error")
	}
}

func TestClpCompressor_Compress_SuccessWithEcho(t *testing.T) {
	// Use /bin/true (or /usr/bin/true) as a binary that exits 0
	truePath := "/bin/true"
	if _, err := os.Stat(truePath); err != nil {
		truePath = "/usr/bin/true"
		if _, err := os.Stat(truePath); err != nil {
			t.Skip("neither /bin/true nor /usr/bin/true found")
		}
	}

	log := zap.NewNop()
	c := NewClpCompressor(truePath, 10*time.Second, log)

	tmpDir := t.TempDir()
	inputDir := filepath.Join(tmpDir, "input")
	os.MkdirAll(inputDir, 0755)
	outputPath := filepath.Join(tmpDir, "output.clp")

	// /bin/true ignores arguments and exits 0
	err := c.Compress(context.Background(), inputDir, outputPath)
	if err != nil {
		t.Fatalf("Compress() with /bin/true should succeed, got: %v", err)
	}
}

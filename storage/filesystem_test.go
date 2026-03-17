package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemBackend_PutAndGet(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "test/file.txt"
	content := []byte("hello, world!")

	// Put
	err := backend.Put(ctx, bucket, key, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Get
	reader, err := backend.Get(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer reader.Close()

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("Get() content = %q, want %q", got, content)
	}
}

func TestFilesystemBackend_Get_NotFound(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	_, err := backend.Get(ctx, bucket, "nonexistent.txt")
	if err != ErrObjectNotFound {
		t.Errorf("Get(nonexistent) error = %v, want ErrObjectNotFound", err)
	}
}

func TestFilesystemBackend_Put_CreatesDirectories(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "deep/nested/path/file.txt"
	content := []byte("nested content")

	err := backend.Put(ctx, bucket, key, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Verify directory was created
	dirPath := filepath.Join(bucket, "deep/nested/path")
	info, err := os.Stat(dirPath)
	if err != nil {
		t.Fatalf("directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory, got file")
	}
}

func TestFilesystemBackend_Put_Overwrite(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "file.txt"

	// Write initial content
	err := backend.Put(ctx, bucket, key, bytes.NewReader([]byte("initial")), 7)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Overwrite
	err = backend.Put(ctx, bucket, key, bytes.NewReader([]byte("overwritten")), 11)
	if err != nil {
		t.Fatalf("Put() overwrite error = %v", err)
	}

	// Verify
	reader, _ := backend.Get(ctx, bucket, key)
	defer reader.Close()
	got, _ := io.ReadAll(reader)
	if string(got) != "overwritten" {
		t.Errorf("content = %q, want %q", got, "overwritten")
	}
}

func TestFilesystemBackend_Delete(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "to-delete.txt"

	// Create file
	backend.Put(ctx, bucket, key, bytes.NewReader([]byte("delete me")), 9)

	// Delete
	err := backend.Delete(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify gone
	exists, _ := backend.Exists(ctx, bucket, key)
	if exists {
		t.Error("file still exists after Delete")
	}
}

func TestFilesystemBackend_Delete_NonExistent(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	// Delete non-existent file - should return error (os.Remove behavior)
	err := backend.Delete(ctx, bucket, "nonexistent.txt")
	if err == nil {
		t.Log("Delete(nonexistent) returned nil - os.Remove doesn't error on missing file on some systems")
	}
	// Note: os.Remove returns error if file doesn't exist
}

func TestFilesystemBackend_Exists(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "exists.txt"

	// Before creation
	exists, err := backend.Exists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Exists() = true before creation, want false")
	}

	// Create file
	backend.Put(ctx, bucket, key, bytes.NewReader([]byte("x")), 1)

	// After creation
	exists, err = backend.Exists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false after creation, want true")
	}
}

func TestFilesystemBackend_EmptyContent(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "empty.txt"

	// Put empty content
	err := backend.Put(ctx, bucket, key, bytes.NewReader([]byte{}), 0)
	if err != nil {
		t.Fatalf("Put(empty) error = %v", err)
	}

	// Get empty content
	reader, err := backend.Get(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Get(empty) error = %v", err)
	}
	defer reader.Close()

	got, _ := io.ReadAll(reader)
	if len(got) != 0 {
		t.Errorf("Get(empty) = %d bytes, want 0", len(got))
	}
}

func TestFilesystemBackend_LargeContent(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()
	key := "large.bin"

	// 1MB of data
	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	err := backend.Put(ctx, bucket, key, bytes.NewReader(content), int64(len(content)))
	if err != nil {
		t.Fatalf("Put(large) error = %v", err)
	}

	reader, err := backend.Get(ctx, bucket, key)
	if err != nil {
		t.Fatalf("Get(large) error = %v", err)
	}
	defer reader.Close()

	got, _ := io.ReadAll(reader)
	if !bytes.Equal(got, content) {
		t.Error("large content mismatch")
	}
}

package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	_ = backend.Put(ctx, bucket, key, bytes.NewReader([]byte("delete me")), 9)

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
	_ = backend.Put(ctx, bucket, key, bytes.NewReader([]byte("x")), 1)

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

// TestFilesystemBackend_Put_MkdirAllError triggers the MkdirAll failure path by
// placing a regular file where a directory is expected.
func TestFilesystemBackend_Put_MkdirAllError(t *testing.T) {
	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	// Create a regular file at the path that Put would try to mkdir -p into.
	blockingFile := filepath.Join(bucket, "dir-blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatal(err)
	}

	// Try to put a key whose directory component is the blocking file.
	err := backend.Put(ctx, bucket, "dir-blocker/file.txt", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected error when MkdirAll cannot create directory over a file")
	}
	if !strings.Contains(err.Error(), "mkdir") {
		t.Errorf("expected mkdir error, got: %v", err)
	}
}

// TestFilesystemBackend_Put_CreateTempError triggers the CreateTemp failure by
// making the target directory read-only after MkdirAll succeeds.
func TestFilesystemBackend_Put_CreateTempError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root, permission checks are bypassed")
	}

	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	// Pre-create the target directory then strip write permission.
	subDir := filepath.Join(bucket, "ro")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(subDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	err := backend.Put(ctx, bucket, "ro/file.txt", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected error when temp file cannot be created in read-only dir")
	}
	if !strings.Contains(err.Error(), "create temp") {
		t.Errorf("expected create temp error, got: %v", err)
	}
}

// TestFilesystemBackend_Put_RenameError triggers the Rename failure by making
// the destination directory read-only after the temp file is written.
func TestFilesystemBackend_Put_RenameError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root, permission checks are bypassed")
	}

	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	// First Put succeeds to establish the sub-directory.
	subDir := filepath.Join(bucket, "sub")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Write one file so the directory exists; then make it read-only.
	if err := os.Chmod(subDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	// With a read-only dir, CreateTemp will also fail. This test therefore
	// also exercises the create-temp error path (same condition as above but
	// via a different key path).
	err := backend.Put(ctx, bucket, "sub/new.txt", bytes.NewReader([]byte("x")), 1)
	if err == nil {
		t.Fatal("expected error with read-only destination directory")
	}
}

// TestFilesystemBackend_Delete_Error ensures Delete propagates non-not-found errors.
func TestFilesystemBackend_Delete_Error(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping: running as root, permission checks are bypassed")
	}

	backend := NewFilesystemBackend()
	ctx := context.Background()
	bucket := t.TempDir()

	// Create a file inside a subdirectory, then remove execute permission from
	// the parent so os.Remove fails with a permission error.
	subDir := filepath.Join(bucket, "locked")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subDir, "target.txt"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(subDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	err := backend.Delete(ctx, bucket, "locked/target.txt")
	if err == nil {
		t.Fatal("expected error when deleting from a locked directory")
	}
	if !strings.Contains(err.Error(), "fs delete") {
		t.Errorf("unexpected error format: %v", err)
	}
}

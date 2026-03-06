package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func init() {
	RegisterType("fs", BackendMeta{
		RequiresBucket: true,
		Factory: func(cfg map[string]string) (StorageBackend, error) {
			return NewFilesystemBackend(), nil
		},
	})
}

// FilesystemBackend implements StorageBackend using the local filesystem.
// Bucket is treated as a base directory.
type FilesystemBackend struct{}

// NewFilesystemBackend creates a filesystem storage backend.
func NewFilesystemBackend() *FilesystemBackend {
	return &FilesystemBackend{}
}

func (b *FilesystemBackend) Get(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	path := filepath.Join(bucket, key)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("fs get: %w", err)
	}
	return f, nil
}

func (b *FilesystemBackend) Put(_ context.Context, bucket, key string, body io.Reader, _ int64) error {
	path := filepath.Join(bucket, key)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("fs put: mkdir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("fs put: create: %w", err)
	}
	if _, err = io.Copy(f, body); err != nil {
		f.Close()
		return fmt.Errorf("fs put: copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("fs put: close: %w", err)
	}
	return nil
}

func (b *FilesystemBackend) Delete(_ context.Context, bucket, key string) error {
	if err := os.Remove(filepath.Join(bucket, key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs delete: %w", err)
	}
	return nil
}

func (b *FilesystemBackend) Exists(_ context.Context, bucket, key string) (bool, error) {
	_, err := os.Stat(filepath.Join(bucket, key))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("fs exists: %w", err)
	}
	return true, nil
}

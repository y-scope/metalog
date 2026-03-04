package storage

import (
	"context"
	"errors"
	"io"
)

// ErrObjectNotFound is returned when a requested object does not exist.
var ErrObjectNotFound = errors.New("object not found")

// StorageBackend is the interface for object storage operations.
type StorageBackend interface {
	// Get retrieves an object. Caller must close the reader.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)

	// Put uploads an object from a reader.
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error

	// Delete removes an object.
	Delete(ctx context.Context, bucket, key string) error

	// Exists checks if an object exists.
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

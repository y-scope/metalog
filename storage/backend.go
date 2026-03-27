// Package storage provides pluggable object storage backends and archive
// creation utilities for metalog.
//
// The [Backend] interface abstracts operations (Get, Put, Delete, Exists)
// across multiple backends:
//   - [S3Backend]: AWS S3 and S3-compatible stores (MinIO, GCS interop)
//   - [FilesystemBackend]: local filesystem (development and testing)
//   - [HTTPBackend]: read-only HTTP/HTTPS fetching
//
// Backends are managed by a [Registry] that maps named backends to
// implementations. The [ArchiveCreator] orchestrates downloading IR files,
// optionally compressing them with CLP ([ClpCompressor]), and uploading
// the resulting archive.
package storage

import (
	"context"
	"errors"
	"io"
)

// ErrObjectNotFound is returned when a requested object does not exist.
var ErrObjectNotFound = errors.New("object not found")

// Backend is the interface for object storage operations.
type Backend interface {
	// Get retrieves an object. Caller must close the reader.
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)

	// Put uploads an object from a reader.
	Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error

	// Delete removes an object.
	Delete(ctx context.Context, bucket, key string) error

	// Exists checks if an object exists.
	Exists(ctx context.Context, bucket, key string) (bool, error)
}

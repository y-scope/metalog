// Package storage provides pluggable object storage backends and archive
// creation utilities for metalog.
//
// The [StorageBackend] interface abstracts operations (Get, Put, Delete, Exists)
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

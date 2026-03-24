package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// ArchiveCreator downloads IR files, runs compression, and uploads the archive.
type ArchiveCreator struct {
	registry   *Registry
	compressor Compressor
	log        *zap.Logger
}

// NewArchiveCreator creates an ArchiveCreator.
func NewArchiveCreator(registry *Registry, compressor Compressor, log *zap.Logger) *ArchiveCreator {
	return &ArchiveCreator{registry: registry, compressor: compressor, log: log}
}

// CreateArchive downloads IR files, compresses them, and uploads the archive.
// irBuckets provides a per-file bucket; if a single bucket is used for all files,
// pass a slice with one element and it will be used for every file.
func (ac *ArchiveCreator) CreateArchive(
	ctx context.Context,
	irBackend string, irBuckets []string, irPaths []string,
	archiveBackend, archiveBucket, archivePath string,
) (int64, error) {
	if ac.compressor == nil {
		return 0, fmt.Errorf("create archive: no compressor configured")
	}
	if len(irBuckets) != 1 && len(irBuckets) != len(irPaths) {
		return 0, fmt.Errorf("create archive: irBuckets length %d must be 1 or match irPaths length %d", len(irBuckets), len(irPaths))
	}

	tmpDir, err := os.MkdirTemp("", "metalog-archive-*")
	if err != nil {
		return 0, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	inputDir := filepath.Join(tmpDir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		return 0, fmt.Errorf("create archive: mkdir: %w", err)
	}

	// Download IR files
	backend, err := ac.registry.Get(irBackend)
	if err != nil {
		return 0, fmt.Errorf("create archive: get ir backend: %w", err)
	}

	for i, irPath := range irPaths {
		bucket := irBuckets[0]
		if len(irBuckets) > 1 {
			bucket = irBuckets[i]
		}
		localPath := filepath.Join(inputDir, fmt.Sprintf("%04d_%s", i, filepath.Base(irPath)))
		if err := downloadFile(ctx, backend, bucket, irPath, localPath); err != nil {
			return 0, fmt.Errorf("download IR %s: %w", irPath, err)
		}
	}

	// Compress into output directory (clp-s produces a single file with --single-file-archive).
	outputDir := filepath.Join(tmpDir, "output")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return 0, fmt.Errorf("create archive: mkdir output: %w", err)
	}
	if err := ac.compressor.Compress(ctx, inputDir, outputDir); err != nil {
		return 0, fmt.Errorf("create archive: compress: %w", err)
	}

	// Find the single archive file produced by the compressor.
	outputPath, err := findSingleFile(outputDir)
	if err != nil {
		return 0, fmt.Errorf("create archive: %w", err)
	}

	// Upload
	stat, err := os.Stat(outputPath)
	if err != nil {
		return 0, fmt.Errorf("stat archive: %w", err)
	}

	archiveStorage, err := ac.registry.Get(archiveBackend)
	if err != nil {
		return 0, fmt.Errorf("create archive: get archive backend: %w", err)
	}

	f, err := os.Open(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create archive: open output: %w", err)
	}
	defer f.Close()

	if err := archiveStorage.Put(ctx, archiveBucket, archivePath, f, stat.Size()); err != nil {
		return 0, fmt.Errorf("upload archive: %w", err)
	}

	ac.log.Info("archive created",
		zap.String("archivePath", archivePath),
		zap.Int64("sizeBytes", stat.Size()),
		zap.Int("irFiles", len(irPaths)),
	)
	return stat.Size(), nil
}

// DeleteArchive removes an archive from object storage. Returns nil if the
// archive does not exist (idempotent). This is used for orphan cleanup when
// archive creation fails after a partial upload.
func (ac *ArchiveCreator) DeleteArchive(ctx context.Context, backendName, bucket, path string) error {
	backend, err := ac.registry.Get(backendName)
	if err != nil {
		return fmt.Errorf("delete archive: get backend %q: %w", backendName, err)
	}
	if err := backend.Delete(ctx, bucket, path); err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return nil // already gone — idempotent
		}
		return fmt.Errorf("delete archive %s/%s: %w", bucket, path, err)
	}
	ac.log.Info("orphan archive deleted", zap.String("path", path), zap.String("bucket", bucket))
	return nil
}

// findSingleFile returns the path of the sole file in dir.
// Returns an error if dir contains zero or more than one file.
func findSingleFile(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", fmt.Errorf("read output dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	if len(files) == 0 {
		return "", fmt.Errorf("no output file found in %s", dir)
	}
	if len(files) > 1 {
		return "", fmt.Errorf("expected 1 output file in %s, found %d", dir, len(files))
	}
	return files[0], nil
}

func downloadFile(ctx context.Context, backend Backend, bucket, key, localPath string) error {
	reader, err := backend.Get(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("download file: get: %w", err)
	}
	defer reader.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("download file: create: %w", err)
	}

	if _, err = io.Copy(f, reader); err != nil {
		f.Close()
		return fmt.Errorf("download file: copy: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("download file: close: %w", err)
	}
	return nil
}

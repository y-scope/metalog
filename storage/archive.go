package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// ArchiveCreator downloads IR files, runs clp-s compression, and uploads the archive.
type ArchiveCreator struct {
	registry   *Registry
	compressor *ClpCompressor
	log        *zap.Logger
}

// NewArchiveCreator creates an ArchiveCreator.
func NewArchiveCreator(registry *Registry, compressor *ClpCompressor, log *zap.Logger) *ArchiveCreator {
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
		bucket := ""
		if i < len(irBuckets) {
			bucket = irBuckets[i]
		} else if len(irBuckets) == 1 {
			bucket = irBuckets[0]
		}
		localPath := filepath.Join(inputDir, filepath.Base(irPath))
		if err := downloadFile(ctx, backend, bucket, irPath, localPath); err != nil {
			return 0, fmt.Errorf("download IR %s: %w", irPath, err)
		}
	}

	// Compress
	outputPath := filepath.Join(tmpDir, "archive.clp")
	if err := ac.compressor.Compress(ctx, inputDir, outputPath); err != nil {
		return 0, fmt.Errorf("create archive: compress: %w", err)
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

func downloadFile(ctx context.Context, backend StorageBackend, bucket, key, localPath string) error {
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

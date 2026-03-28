package testutil

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// ConcatCompressor is a test compressor that concatenates all files in
// the input directory into a single file in the output directory.
// It replaces clp-s in integration tests where a real CLP binary is
// not available.
type ConcatCompressor struct{}

func (c *ConcatCompressor) Compress(_ context.Context, inputDir, outputDir string) error {
	out, err := os.Create(filepath.Join(outputDir, "archive.clp.zst"))
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := copyFile(out, filepath.Join(inputDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(dst io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(dst, f)
	return err
}

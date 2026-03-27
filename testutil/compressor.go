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
	defer out.Close()

	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		f, err := os.Open(filepath.Join(inputDir, entry.Name()))
		if err != nil {
			return err
		}
		_, err = io.Copy(out, f)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

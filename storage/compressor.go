package storage

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"go.uber.org/zap"
)

// Compressor compresses IR files into an archive.
// inputDir contains the IR files to compress. outputDir is where the
// compressor writes the result. The caller expects exactly one file
// in outputDir after Compress returns.
type Compressor interface {
	Compress(ctx context.Context, inputDir, outputDir string) error
}

// ClpCompressor runs the clp-s binary as a subprocess.
type ClpCompressor struct {
	binaryPath string
	timeout    time.Duration
	log        *zap.Logger
}

// NewClpCompressor creates a ClpCompressor.
func NewClpCompressor(binaryPath string, timeout time.Duration, log *zap.Logger) *ClpCompressor {
	return &ClpCompressor{binaryPath: binaryPath, timeout: timeout, log: log}
}

// Compress runs clp-s on the input directory, writing a single-file archive
// to outputDir. The caller should look for the single file in outputDir after
// this returns.
//
// Flags:
//   - --single-file-archive: produce one file instead of a directory of segments
//   - --remove-path-prefix: strip the staging directory prefix from stored paths
//   - --remove-leading-slash: ensure stored paths are relative
//   - --normalize-paths: normalize path separators
func (c *ClpCompressor) Compress(ctx context.Context, inputDir, outputDir string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.binaryPath, "c",
		"--single-file-archive",
		"--remove-path-prefix", inputDir,
		"--remove-leading-slash",
		"--normalize-paths",
		outputDir, inputDir,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("clp-s compress failed: %w\noutput: %s", err, string(output))
	}

	c.log.Debug("clp-s compression completed",
		zap.String("input", inputDir),
		zap.String("output", outputDir),
	)
	return nil
}

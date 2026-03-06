package storage

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"go.uber.org/zap"
)

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

// Compress runs clp-s on the input directory, writing the archive to outputPath.
func (c *ClpCompressor) Compress(ctx context.Context, inputDir, outputPath string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, c.binaryPath, "c", inputDir, "-o", outputPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("clp-s compress failed: %w\noutput: %s", err, string(output))
	}

	c.log.Debug("clp-s compression completed",
		zap.String("input", inputDir),
		zap.String("output", outputPath),
	)
	return nil
}

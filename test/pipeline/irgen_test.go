//go:build integration

package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// generateIRFiles creates count CLP KV-IR files in dir, each containing
// eventsPerFile synthetic log events. Returns the file paths.
func generateIRFiles(dir string, count, eventsPerFile int) ([]string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	var paths []string

	for i := 0; i < count; i++ {
		path := filepath.Join(dir, fmt.Sprintf("test_%03d.clp.zst", i))
		if err := writeIRFile(path, i, eventsPerFile, baseTime); err != nil {
			return nil, fmt.Errorf("write IR file %d: %w", i, err)
		}
		paths = append(paths, path)
	}

	return paths, nil
}

// writeIRFile creates a single CLP KV-IR file with synthetic log events.
func writeIRFile(path string, fileIndex, eventCount int, baseTime time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer, err := ir.NewWriter[ir.EightByteEncoding](f)
	if err != nil {
		return fmt.Errorf("create IR writer: %w", err)
	}

	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	services := []string{"api-gateway", "auth-service", "data-processor", "scheduler"}

	for j := 0; j < eventCount; j++ {
		event := ffi.NewLogEvent()
		event.AutoKvPairs["timestamp"] = baseTime.Add(
			time.Duration(fileIndex)*time.Hour + time.Duration(j)*time.Second,
		).UnixMilli()
		event.AutoKvPairs["level"] = levels[j%len(levels)]
		event.AutoKvPairs["service"] = services[fileIndex%len(services)]
		event.AutoKvPairs["host"] = fmt.Sprintf("node-%d", fileIndex)
		event.AutoKvPairs["request_id"] = fmt.Sprintf("req-%d-%d", fileIndex, j)
		event.AutoKvPairs["message"] = fmt.Sprintf(
			"Processing request %d on %s: operation completed in %dms",
			j, services[fileIndex%len(services)], 10+j%100,
		)
		event.AutoKvPairs["duration_ms"] = 10 + j%100
		event.AutoKvPairs["status_code"] = 200 + (j%5)*100 // 200, 300, 400, 500, 600

		if _, err := writer.WriteLogEvent(*event); err != nil {
			return fmt.Errorf("write event %d: %w", j, err)
		}
	}

	return writer.Close()
}

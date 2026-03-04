package consolidation

import (
	"fmt"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
	"github.com/y-scope/metalog/internal/taskqueue"
	"github.com/y-scope/metalog/internal/timeutil"
)

// ArchivePathGenerator generates deterministic archive paths based on file metadata.
type ArchivePathGenerator struct {
	// Prefix is prepended to all paths (e.g., "archive/")
	Prefix string
}

// NewArchivePathGenerator creates an ArchivePathGenerator.
func NewArchivePathGenerator(prefix string) *ArchivePathGenerator {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return &ArchivePathGenerator{Prefix: prefix}
}

// Generate creates an archive path from the task payload and a unique suffix.
// Format: {prefix}{table}/{date}/{hour}/{uniqueID}.clp
func (g *ArchivePathGenerator) Generate(payload *taskqueue.TaskPayload, uniqueID string) string {
	t := time.Unix(0, payload.MinTimestamp).UTC()
	datePath := t.Format("2006/01/02")
	hourPath := fmt.Sprintf("%02d", t.Hour())

	return fmt.Sprintf("%s%s/%s/%s/%s.clp",
		g.Prefix, payload.TableName, datePath, hourPath, uniqueID)
}

// GenerateFromRecords creates an archive path using the earliest timestamp from the records.
func (g *ArchivePathGenerator) GenerateFromRecords(tableName string, records []*metastore.FileRecord, uniqueID string) string {
	minTs := timeutil.EpochNanos()
	for _, rec := range records {
		if rec.MinTimestamp < minTs {
			minTs = rec.MinTimestamp
		}
	}

	t := time.Unix(0, minTs).UTC()
	datePath := t.Format("2006/01/02")
	hourPath := fmt.Sprintf("%02d", t.Hour())

	return fmt.Sprintf("%s%s/%s/%s/%s.clp",
		g.Prefix, tableName, datePath, hourPath, uniqueID)
}

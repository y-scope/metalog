package consolidation

import (
	"github.com/google/uuid"

	"github.com/y-scope/metalog/metastore"
)

// AggRequirement describes an aggregation column a policy needs. The three
// fields form the compound key used by schema.ColumnRegistry.ResolveAgg.
type AggRequirement struct {
	Key   string // e.g. "cpu_usage"
	Value string // e.g. "" (empty for most agg types)
	Type  string // e.g. "SUM", "AVG", "EQ"
}

// FileGroup is a set of file records selected for consolidation into a single archive.
type FileGroup struct {
	Records        []*metastore.FileRecord
	ArchivePath    string // globally unique archive object key
	ArchiveBackend string // storage backend for the archive (empty = use planner default)
	ArchiveBucket  string // storage bucket for the archive (empty = use planner default)
}

// Policy determines which files should be consolidated together.
type Policy interface {
	// SelectFiles returns groups of file records to consolidate.
	// Each group becomes a single consolidation task. The policy is
	// responsible for setting ArchivePath on each FileGroup (typically
	// via GenerateArchivePath, which policies may override).
	SelectFiles(candidates []*metastore.FileRecord) []FileGroup

	// RequiredDims returns the logical dimension keys this policy needs
	// populated on FileRecord.Dims (e.g. ["application_id"]).
	// Return nil if no dimension columns are needed.
	RequiredDims() []string

	// RequiredAggs returns the aggregation columns this policy needs
	// populated on FileRecord.Aggs. Return nil if none are needed.
	RequiredAggs() []AggRequirement
}

// GenerateArchivePath returns a default globally unique archive path: <UUIDv7>.clp.zst.
// Policies embed this as the default and may override it.
func GenerateArchivePath() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Fallback to V4 if V7 fails (should never happen).
		id = uuid.New()
	}
	return id.String() + ".clp.zst"
}

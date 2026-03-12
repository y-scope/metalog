package consolidation

import "github.com/y-scope/metalog/internal/metastore"

// AggRequirement describes an aggregation column a policy needs. The three
// fields form the compound key used by schema.ColumnRegistry.ResolveAgg.
type AggRequirement struct {
	Key   string // e.g. "cpu_usage"
	Value string // e.g. "" (empty for most agg types)
	Type  string // e.g. "SUM", "AVG", "EQ"
}

// Policy determines which files should be consolidated together.
type Policy interface {
	// SelectFiles returns groups of file records to consolidate.
	// Each group becomes a single consolidation task.
	SelectFiles(candidates []*metastore.FileRecord) [][]*metastore.FileRecord

	// RequiredDims returns the logical dimension keys this policy needs
	// populated on FileRecord.Dims (e.g. ["application_id"]).
	// Return nil if no dimension columns are needed.
	RequiredDims() []string

	// RequiredAggs returns the aggregation columns this policy needs
	// populated on FileRecord.Aggs. Return nil if none are needed.
	RequiredAggs() []AggRequirement
}

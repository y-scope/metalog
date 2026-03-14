package consolidation

import (
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func init() {
	RegisterPolicyType("audit", func(cfg PolicyConfig) (Policy, error) {
		return NewAuditPolicy(cfg.MinFiles, cfg.MaxFiles), nil
	})
}

// AuditPolicy groups files by exact day boundaries for compliance-oriented
// consolidation. Files are never merged across day boundaries.
type AuditPolicy struct {
	// MinFilesPerGroup is the minimum number of files to form a group for a given day.
	MinFilesPerGroup int
	// MaxFilesPerGroup is the maximum number of files per consolidation task.
	MaxFilesPerGroup int
}

// NewAuditPolicy creates an AuditPolicy.
func NewAuditPolicy(minFiles, maxFiles int) *AuditPolicy {
	if maxFiles <= 0 {
		maxFiles = 100
	}
	return &AuditPolicy{
		MinFilesPerGroup: minFiles,
		MaxFilesPerGroup: maxFiles,
	}
}

func (p *AuditPolicy) RequiredDims() []string { return nil }
func (p *AuditPolicy) RequiredAggs() []AggRequirement { return nil }

// SelectFiles groups candidates by day boundary (UTC).
func (p *AuditPolicy) SelectFiles(candidates []*metastore.FileRecord) []FileGroup {
	if len(candidates) == 0 {
		return nil
	}

	// Group by UTC day of min_timestamp
	buckets := make(map[string][]*metastore.FileRecord)
	for _, rec := range candidates {
		day := time.Unix(0, rec.MinTimestamp).UTC().Format("2006-01-02")
		buckets[day] = append(buckets[day], rec)
	}

	var result []FileGroup
	for _, bucket := range buckets {
		if len(bucket) < p.MinFilesPerGroup {
			continue
		}

		for i := 0; i < len(bucket); i += p.MaxFilesPerGroup {
			end := i + p.MaxFilesPerGroup
			if end > len(bucket) {
				end = len(bucket)
			}
			chunk := bucket[i:end]
			if len(chunk) >= p.MinFilesPerGroup {
				result = append(result, FileGroup{
					Records:     chunk,
					ArchivePath: GenerateArchivePath(),
				})
			}
		}
	}
	return result
}

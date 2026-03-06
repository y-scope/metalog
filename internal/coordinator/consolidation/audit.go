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
	// MinFilesPerDay is the minimum number of files to form a group for a given day.
	MinFilesPerDay int
	// MaxFilesPerGroup is the maximum number of files per consolidation task.
	MaxFilesPerGroup int
}

// NewAuditPolicy creates an AuditPolicy.
func NewAuditPolicy(minFiles, maxFiles int) *AuditPolicy {
	return &AuditPolicy{
		MinFilesPerDay:   minFiles,
		MaxFilesPerGroup: maxFiles,
	}
}

// SelectFiles groups candidates by day boundary (UTC).
func (p *AuditPolicy) SelectFiles(candidates []*metastore.FileRecord) [][]*metastore.FileRecord {
	if len(candidates) == 0 {
		return nil
	}

	// Group by UTC day of min_timestamp
	groups := make(map[string][]*metastore.FileRecord)
	for _, rec := range candidates {
		day := time.Unix(0, rec.MinTimestamp).UTC().Format("2006-01-02")
		groups[day] = append(groups[day], rec)
	}

	var result [][]*metastore.FileRecord
	for _, group := range groups {
		if len(group) < p.MinFilesPerDay {
			continue
		}

		for i := 0; i < len(group); i += p.MaxFilesPerGroup {
			end := i + p.MaxFilesPerGroup
			if end > len(group) {
				end = len(group)
			}
			chunk := group[i:end]
			if len(chunk) >= p.MinFilesPerDay {
				result = append(result, chunk)
			}
		}
	}
	return result
}

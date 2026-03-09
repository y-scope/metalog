package consolidation

import (
	"fmt"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func init() {
	RegisterPolicyType("spark_job", func(cfg PolicyConfig) (Policy, error) {
		if cfg.GroupingDimKey == "" {
			return nil, fmt.Errorf("spark_job policy requires groupingDimKey")
		}
		jt := cfg.JobTimeout
		if jt <= 0 {
			jt = 2 * time.Hour
		}
		return NewSparkJobPolicy(cfg.GroupingDimKey, cfg.MinFiles, cfg.MaxFiles, jt), nil
	})
}

// ungroupedKey is the sentinel key for records missing the grouping dimension.
const ungroupedKey = "\x00ungrouped"

// SparkJobPolicy groups files by a dimension value (e.g., application_id)
// to consolidate all IR files belonging to the same job together.
type SparkJobPolicy struct {
	// GroupingDimKey is the dimension key to group by (e.g., "application_id").
	GroupingDimKey string
	// MinFilesPerGroup is the minimum number of files to form a group.
	MinFilesPerGroup int
	// MaxFilesPerGroup is the maximum number of files per group.
	MaxFilesPerGroup int
	// JobTimeout triggers consolidation after this duration even if the job isn't complete.
	JobTimeout time.Duration
}

// NewSparkJobPolicy creates a SparkJobPolicy.
func NewSparkJobPolicy(groupingKey string, minFiles, maxFiles int, timeout time.Duration) *SparkJobPolicy {
	if maxFiles <= 0 {
		maxFiles = 100
	}
	return &SparkJobPolicy{
		GroupingDimKey:   groupingKey,
		MinFilesPerGroup: minFiles,
		MaxFilesPerGroup: maxFiles,
		JobTimeout:       timeout,
	}
}

// SelectFiles groups candidates by the grouping dimension value.
func (p *SparkJobPolicy) SelectFiles(candidates []*metastore.FileRecord) [][]*metastore.FileRecord {
	if len(candidates) == 0 {
		return nil
	}

	// Group by the dimension value
	groups := make(map[string][]*metastore.FileRecord)
	for _, rec := range candidates {
		key := ""
		if rec.Dims != nil {
			if v, ok := rec.Dims[p.GroupingDimKey]; ok {
				if s, ok := v.(string); ok {
					key = s
				}
			}
		}
		if key == "" {
			key = ungroupedKey
		}
		groups[key] = append(groups[key], rec)
	}

	var result [][]*metastore.FileRecord
	now := time.Now().UnixNano()

	for _, group := range groups {
		// Check if group meets minimum size or has timed out.
		// Uses MaxTimestamp (latest event) as a proxy for "last write time".
		// For historical data (backfills), this will always exceed the timeout,
		// which is correct: old data implies the producing job is complete.
		timedOut := false
		if p.JobTimeout > 0 {
			for _, rec := range group {
				elapsed := now - rec.MaxTimestamp
				if elapsed <= 0 {
					continue // future timestamp or clock skew — not timed out
				}
				if time.Duration(elapsed) >= p.JobTimeout {
					timedOut = true
					break
				}
			}
		}

		if len(group) < p.MinFilesPerGroup && !timedOut {
			continue
		}

		// Split into max-sized chunks
		for i := 0; i < len(group); i += p.MaxFilesPerGroup {
			end := i + p.MaxFilesPerGroup
			if end > len(group) {
				end = len(group)
			}
			chunk := group[i:end]
			if len(chunk) >= p.MinFilesPerGroup || timedOut {
				result = append(result, chunk)
			}
		}
	}
	return result
}

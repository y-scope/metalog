package consolidation

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/y-scope/metalog/metastore"
)

const defaultJobTimeout = 24 * time.Hour

func init() {
	RegisterPolicyType("spark_job", func(config json.RawMessage) (Policy, error) {
		var cfg sparkJobConfig
		if len(config) > 0 {
			if err := json.Unmarshal(config, &cfg); err != nil {
				return nil, fmt.Errorf("spark_job config: %w", err)
			}
		}
		if cfg.GroupingDimKey == "" {
			return nil, fmt.Errorf("spark_job policy requires grouping_dim_key")
		}
		return NewSparkJobPolicy(
			cfg.GroupingDimKey,
			cfg.minFiles(),
			cfg.maxFiles(),
			cfg.jobTimeout(),
		), nil
	})
}

// sparkJobConfig is the JSON-deserialized config for SparkJobPolicy.
type sparkJobConfig struct {
	GroupingDimKey string `json:"grouping_dim_key"`
	MinFiles      int    `json:"min_files"`
	MaxFiles      int    `json:"max_files"`
	JobTimeout    string `json:"job_timeout"` // e.g. "24h", "2h"
}

func (c *sparkJobConfig) minFiles() int {
	if c.MinFiles > 0 {
		return c.MinFiles
	}
	return defaultMinFiles
}

func (c *sparkJobConfig) maxFiles() int {
	if c.MaxFiles > 0 {
		return c.MaxFiles
	}
	return defaultMaxFiles
}

func (c *sparkJobConfig) jobTimeout() time.Duration {
	if c.JobTimeout != "" {
		d, err := time.ParseDuration(c.JobTimeout)
		if err == nil && d > 0 {
			return d
		}
	}
	return defaultJobTimeout
}

// ungroupedKey is the sentinel key for records missing the grouping dimension.
const ungroupedKey = "\x00ungrouped"

// SparkJobPolicy groups files by a dimension value (e.g., application_id)
// to consolidate all IR files belonging to the same job together.
type SparkJobPolicy struct {
	GroupingDimKey string
	MinFiles      int
	MaxFiles      int
	JobTimeout    time.Duration
}

// NewSparkJobPolicy creates a SparkJobPolicy.
func NewSparkJobPolicy(groupingKey string, minFiles, maxFiles int, timeout time.Duration) *SparkJobPolicy {
	return &SparkJobPolicy{
		GroupingDimKey:   groupingKey,
		MinFiles: minFiles,
		MaxFiles: maxFiles,
		JobTimeout:       timeout,
	}
}

func (p *SparkJobPolicy) RequiredDims() []string {
	if p.GroupingDimKey == "" {
		return nil
	}
	return []string{p.GroupingDimKey}
}
func (p *SparkJobPolicy) RequiredAggs() []AggRequirement { return nil }

// SelectFiles groups candidates by the grouping dimension value.
func (p *SparkJobPolicy) SelectFiles(candidates []*metastore.FileRecord) []FileGroup {
	if len(candidates) == 0 {
		return nil
	}

	buckets := make(map[string][]*metastore.FileRecord)
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
		buckets[key] = append(buckets[key], rec)
	}

	var result []FileGroup
	now := time.Now().UnixNano()

	for _, bucket := range buckets {
		timedOut := false
		if p.JobTimeout > 0 {
			for _, rec := range bucket {
				if rec.MaxTimestamp == 0 {
					continue // uninitialized — skip
				}
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

		if len(bucket) < p.MinFiles && !timedOut {
			continue
		}

		for i := 0; i < len(bucket); i += p.MaxFiles {
			end := i + p.MaxFiles
			if end > len(bucket) {
				end = len(bucket)
			}
			chunk := bucket[i:end]
			if len(chunk) >= p.MinFiles || timedOut {
				result = append(result, FileGroup{
					Records:     chunk,
					ArchivePath: GenerateArchivePath(),
				})
			}
		}
	}
	return result
}

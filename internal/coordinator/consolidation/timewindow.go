package consolidation

import (
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func init() {
	RegisterPolicyType("time_window", func(cfg PolicyConfig) (Policy, error) {
		ws := cfg.WindowSize
		if ws <= 0 {
			ws = time.Hour
		}
		return NewTimeWindowPolicy(ws, cfg.MinFiles, cfg.MaxFiles), nil
	})
}

// TimeWindowPolicy groups files by time window for consolidation.
type TimeWindowPolicy struct {
	WindowSize       time.Duration
	MinFilesPerGroup int
	MaxFilesPerGroup int
}

// NewTimeWindowPolicy creates a TimeWindowPolicy.
func NewTimeWindowPolicy(windowSize time.Duration, minFiles, maxFiles int) *TimeWindowPolicy {
	if maxFiles <= 0 {
		maxFiles = 100
	}
	return &TimeWindowPolicy{
		WindowSize:       windowSize,
		MinFilesPerGroup: minFiles,
		MaxFilesPerGroup: maxFiles,
	}
}

func (p *TimeWindowPolicy) RequiredDims() []string { return nil }
func (p *TimeWindowPolicy) RequiredAggs() []AggRequirement { return nil }

// SelectFiles groups candidates by time window.
func (p *TimeWindowPolicy) SelectFiles(candidates []*metastore.FileRecord) []FileGroup {
	if len(candidates) == 0 {
		return nil
	}

	windowNanos := p.WindowSize.Nanoseconds()
	buckets := make(map[int64][]*metastore.FileRecord)

	for _, rec := range candidates {
		windowKey := rec.MinTimestamp / windowNanos
		buckets[windowKey] = append(buckets[windowKey], rec)
	}

	var result []FileGroup
	for _, bucket := range buckets {
		if len(bucket) < p.MinFilesPerGroup {
			continue
		}
		// Split into max-sized chunks
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

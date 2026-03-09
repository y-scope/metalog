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

// SelectFiles groups candidates by time window.
func (p *TimeWindowPolicy) SelectFiles(candidates []*metastore.FileRecord) [][]*metastore.FileRecord {
	if len(candidates) == 0 {
		return nil
	}

	windowNanos := p.WindowSize.Nanoseconds()
	groups := make(map[int64][]*metastore.FileRecord)

	for _, rec := range candidates {
		windowKey := rec.MinTimestamp / windowNanos
		groups[windowKey] = append(groups[windowKey], rec)
	}

	var result [][]*metastore.FileRecord
	for _, group := range groups {
		if len(group) < p.MinFilesPerGroup {
			continue
		}
		// Split into max-sized chunks
		for i := 0; i < len(group); i += p.MaxFilesPerGroup {
			end := i + p.MaxFilesPerGroup
			if end > len(group) {
				end = len(group)
			}
			chunk := group[i:end]
			if len(chunk) >= p.MinFilesPerGroup {
				result = append(result, chunk)
			}
		}
	}
	return result
}

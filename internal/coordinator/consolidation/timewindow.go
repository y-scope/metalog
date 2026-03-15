package consolidation

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

func init() {
	RegisterPolicyType("time_window", func(config json.RawMessage) (Policy, error) {
		var cfg timeWindowConfig
		if len(config) > 0 {
			if err := json.Unmarshal(config, &cfg); err != nil {
				return nil, fmt.Errorf("time_window config: %w", err)
			}
		}
		ws := cfg.windowSize()
		minF := cfg.minFiles()
		maxF := cfg.maxFiles()
		return NewTimeWindowPolicy(ws, minF, maxF), nil
	})
}

// timeWindowConfig is the JSON-deserialized config for TimeWindowPolicy.
type timeWindowConfig struct {
	WindowSize string `json:"window_size"` // e.g. "1h", "30m"
	MinFiles   int    `json:"min_files"`
	MaxFiles   int    `json:"max_files"`
}

func (c *timeWindowConfig) windowSize() time.Duration {
	if c.WindowSize != "" {
		d, err := time.ParseDuration(c.WindowSize)
		if err == nil && d > 0 {
			return d
		}
	}
	return time.Hour
}

func (c *timeWindowConfig) minFiles() int {
	if c.MinFiles > 0 {
		return c.MinFiles
	}
	return defaultMinFilesPerGroup
}

func (c *timeWindowConfig) maxFiles() int {
	if c.MaxFiles > 0 {
		return c.MaxFiles
	}
	return defaultMaxFilesPerGroup
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
		maxFiles = defaultMaxFilesPerGroup
	}
	return &TimeWindowPolicy{
		WindowSize:       windowSize,
		MinFilesPerGroup: minFiles,
		MaxFilesPerGroup: maxFiles,
	}
}

func (p *TimeWindowPolicy) RequiredDims() []string        { return nil }
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

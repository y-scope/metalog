package consolidation

import (
	"fmt"
	"time"
)

// PolicyConfig holds configuration for creating a consolidation policy.
type PolicyConfig struct {
	Type           string        `yaml:"type"`
	WindowSize     time.Duration `yaml:"windowSize"`
	MinFiles       int           `yaml:"minFiles"`
	MaxFiles       int           `yaml:"maxFiles"`
	GroupingDimKey string        `yaml:"groupingDimKey"`
	JobTimeout     time.Duration `yaml:"jobTimeout"`
}

// DefaultPolicyConfig returns a default time-window policy config.
func DefaultPolicyConfig() PolicyConfig {
	return PolicyConfig{
		Type:       "time_window",
		WindowSize: time.Hour,
		MinFiles:   2,
		MaxFiles:   100,
	}
}

// CreatePolicy instantiates a Policy from configuration.
func CreatePolicy(cfg PolicyConfig) (Policy, error) {
	if cfg.MinFiles <= 0 {
		cfg.MinFiles = 2
	}
	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = 100
	}

	switch cfg.Type {
	case "time_window", "":
		ws := cfg.WindowSize
		if ws <= 0 {
			ws = time.Hour
		}
		return NewTimeWindowPolicy(ws, cfg.MinFiles, cfg.MaxFiles), nil

	case "spark_job":
		if cfg.GroupingDimKey == "" {
			return nil, fmt.Errorf("spark_job policy requires groupingDimKey")
		}
		jt := cfg.JobTimeout
		if jt <= 0 {
			jt = 2 * time.Hour
		}
		return NewSparkJobPolicy(cfg.GroupingDimKey, cfg.MinFiles, cfg.MaxFiles, jt), nil

	case "audit":
		return NewAuditPolicy(cfg.MinFiles, cfg.MaxFiles), nil

	default:
		return nil, fmt.Errorf("unknown policy type: %q", cfg.Type)
	}
}

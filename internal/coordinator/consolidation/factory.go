package consolidation

import (
	"fmt"
	"sync"
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

var (
	policyMu       sync.RWMutex
	policyRegistry = map[string]func(cfg PolicyConfig) (Policy, error){}
)

// RegisterPolicyType registers a policy type factory.
func RegisterPolicyType(typeName string, factory func(cfg PolicyConfig) (Policy, error)) {
	policyMu.Lock()
	defer policyMu.Unlock()
	policyRegistry[typeName] = factory
}

// CreatePolicy instantiates a Policy from configuration.
func CreatePolicy(cfg PolicyConfig) (Policy, error) {
	if cfg.MinFiles <= 0 {
		cfg.MinFiles = 2
	}
	if cfg.MaxFiles <= 0 {
		cfg.MaxFiles = 100
	}

	typeName := cfg.Type
	if typeName == "" {
		typeName = "time_window"
	}

	policyMu.RLock()
	factory, ok := policyRegistry[typeName]
	policyMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown policy type: %q", typeName)
	}
	return factory(cfg)
}

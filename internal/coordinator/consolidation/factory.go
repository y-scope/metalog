package consolidation

import (
	"encoding/json"
	"fmt"
	"sync"
)

const (
	// defaultMinFilesPerGroup is the minimum files needed to form a consolidation group.
	defaultMinFilesPerGroup = 2
	// defaultMaxFilesPerGroup is the maximum files per consolidation task.
	defaultMaxFilesPerGroup = 100
	// defaultPolicyType is used when no policies are configured.
	defaultPolicyType = "time_window"
)

var (
	policyMu       sync.RWMutex
	policyRegistry = map[string]PolicyFactory{}
)

// PolicyFactory creates a Policy from raw JSON config.
// The factory is responsible for deserializing the config into its own struct.
// A nil config means "use defaults".
type PolicyFactory func(config json.RawMessage) (Policy, error)

// RegisterPolicyType registers a policy type factory.
func RegisterPolicyType(typeName string, factory PolicyFactory) {
	policyMu.Lock()
	defer policyMu.Unlock()
	policyRegistry[typeName] = factory
}

// CreatePolicy instantiates a Policy by type name and raw JSON config.
func CreatePolicy(typeName string, config json.RawMessage) (Policy, error) {
	if typeName == "" {
		typeName = defaultPolicyType
	}

	policyMu.RLock()
	factory, ok := policyRegistry[typeName]
	policyMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown policy type: %q", typeName)
	}
	return factory(config)
}

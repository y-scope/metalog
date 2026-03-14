package consolidation

import (
	"fmt"
	"time"

	"github.com/y-scope/metalog/internal/metastore"
)

// PolicyChain implements the Policy interface by running multiple policies
// in sequence. Each policy receives only the candidates not consumed by
// earlier policies in the chain (waterfall pattern).
type PolicyChain struct {
	policies []Policy
}

// NewPolicyChain creates a PolicyChain from the given policies.
func NewPolicyChain(policies []Policy) *PolicyChain {
	return &PolicyChain{policies: policies}
}

// RequiredDims returns the union of all child policies' required dimension keys.
func (pc *PolicyChain) RequiredDims() []string {
	return unionStrings(pc.policies, func(p Policy) []string { return p.RequiredDims() })
}

// RequiredAggs returns the union of all child policies' required aggregation columns.
func (pc *PolicyChain) RequiredAggs() []AggRequirement {
	seen := make(map[AggRequirement]struct{})
	var result []AggRequirement
	for _, p := range pc.policies {
		for _, a := range p.RequiredAggs() {
			if _, ok := seen[a]; !ok {
				seen[a] = struct{}{}
				result = append(result, a)
			}
		}
	}
	return result
}

// unionStrings collects and deduplicates strings from multiple policies.
func unionStrings(policies []Policy, fn func(Policy) []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, p := range policies {
		for _, s := range fn(p) {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				result = append(result, s)
			}
		}
	}
	return result
}

// SelectFiles runs the waterfall: each policy selects from remaining candidates.
func (pc *PolicyChain) SelectFiles(candidates []*metastore.FileRecord) []FileGroup {
	var allGroups []FileGroup
	remaining := candidates

	for _, p := range pc.policies {
		if len(remaining) == 0 {
			break
		}
		groups := p.SelectFiles(remaining)
		allGroups = append(allGroups, groups...)
		consumed := collectConsumed(groups)
		remaining = subtract(remaining, consumed)
	}

	return allGroups
}

// collectConsumed flattens groups into a set of consumed FileRecord pointers.
func collectConsumed(groups []FileGroup) map[*metastore.FileRecord]struct{} {
	consumed := make(map[*metastore.FileRecord]struct{})
	for _, g := range groups {
		for _, f := range g.Records {
			consumed[f] = struct{}{}
		}
	}
	return consumed
}

// subtract returns candidates not in the consumed set, preserving order.
func subtract(candidates []*metastore.FileRecord, consumed map[*metastore.FileRecord]struct{}) []*metastore.FileRecord {
	if len(consumed) == 0 {
		return candidates
	}
	remaining := make([]*metastore.FileRecord, 0, len(candidates)-len(consumed))
	for _, c := range candidates {
		if _, ok := consumed[c]; !ok {
			remaining = append(remaining, c)
		}
	}
	return remaining
}

// CreatePolicyChain builds a Policy from a slice of ConsolidationPolicyConfig.
// Each config is converted to a PolicyConfig and instantiated via CreatePolicy.
// If the slice is empty, a default time_window policy is used.
func CreatePolicyChain(configs []metastore.ConsolidationPolicyConfig) (Policy, error) {
	if len(configs) == 0 {
		return CreatePolicy(DefaultPolicyConfig())
	}
	if len(configs) == 1 {
		pc, err := toPolicyConfig(configs[0])
		if err != nil {
			return nil, err
		}
		return CreatePolicy(pc)
	}

	policies := make([]Policy, 0, len(configs))
	for i, c := range configs {
		pc, err := toPolicyConfig(c)
		if err != nil {
			return nil, fmt.Errorf("policy %d (%s): %w", i, c.Type, err)
		}
		p, err := CreatePolicy(pc)
		if err != nil {
			return nil, fmt.Errorf("policy %d (%s): %w", i, c.Type, err)
		}
		policies = append(policies, p)
	}
	return NewPolicyChain(policies), nil
}

// toPolicyConfig converts a ConsolidationPolicyConfig (string durations from
// msgpack) to a PolicyConfig (parsed time.Duration values).
func toPolicyConfig(c metastore.ConsolidationPolicyConfig) (PolicyConfig, error) {
	cfg := PolicyConfig{
		Type:           c.Type,
		MinFiles:       c.MinFiles,
		MaxFiles:       c.MaxFiles,
		GroupingDimKey: c.GroupingDimKey,
	}
	if c.WindowSize != "" {
		d, err := time.ParseDuration(c.WindowSize)
		if err != nil {
			return PolicyConfig{}, fmt.Errorf("parse window_size %q: %w", c.WindowSize, err)
		}
		cfg.WindowSize = d
	}
	if c.JobTimeout != "" {
		d, err := time.ParseDuration(c.JobTimeout)
		if err != nil {
			return PolicyConfig{}, fmt.Errorf("parse job_timeout %q: %w", c.JobTimeout, err)
		}
		cfg.JobTimeout = d
	}
	return cfg, nil
}

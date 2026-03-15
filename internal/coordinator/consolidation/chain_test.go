package consolidation

import (
	"testing"

	"github.com/y-scope/metalog/internal/metastore"
)

// mockPolicy returns predefined groups and tracks which candidates it received.
type mockPolicy struct {
	selectFn func(candidates []*metastore.FileRecord) []FileGroup
}

func (m *mockPolicy) SelectFiles(candidates []*metastore.FileRecord) []FileGroup {
	return m.selectFn(candidates)
}

func (m *mockPolicy) RequiredDims() []string { return nil }
func (m *mockPolicy) RequiredAggs() []AggRequirement { return nil }

func makeRecords(n int) []*metastore.FileRecord {
	records := make([]*metastore.FileRecord, n)
	for i := range records {
		records[i] = &metastore.FileRecord{ID: int64(i + 1)}
	}
	return records
}

func TestPolicyChain_Waterfall(t *testing.T) {
	records := makeRecords(6)

	// First policy consumes records[0] and records[1].
	policy1 := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			// Find our target records among candidates.
			var group []*metastore.FileRecord
			for _, c := range candidates {
				if c.ID == 1 || c.ID == 2 {
					group = append(group, c)
				}
			}
			if len(group) > 0 {
				return []FileGroup{{Records: group, ArchivePath: "a.clp.zst"}}
			}
			return nil
		},
	}

	// Second policy gets the remaining 4 records, groups into pairs.
	var policy2Received int
	policy2 := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			policy2Received = len(candidates)
			if len(candidates) >= 2 {
				return []FileGroup{
					{Records: candidates[:2], ArchivePath: "b.clp.zst"},
					{Records: candidates[2:], ArchivePath: "c.clp.zst"},
				}
			}
			return nil
		},
	}

	chain := NewPolicyChain([]Policy{policy1, policy2})
	groups := chain.SelectFiles(records)

	// policy1 produced 1 group, policy2 produced 2 groups.
	if len(groups) != 3 {
		t.Errorf("groups = %d, want 3", len(groups))
	}

	// policy2 should have received 4 candidates (6 - 2 consumed by policy1).
	if policy2Received != 4 {
		t.Errorf("policy2 received %d candidates, want 4", policy2Received)
	}
}

func TestPolicyChain_SinglePolicy(t *testing.T) {
	records := makeRecords(4)

	inner := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			return []FileGroup{{Records: candidates, ArchivePath: "x.clp.zst"}}
		},
	}

	chain := NewPolicyChain([]Policy{inner})
	groups := chain.SelectFiles(records)

	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	if len(groups[0].Records) != 4 {
		t.Errorf("group[0] size = %d, want 4", len(groups[0].Records))
	}
}

func TestPolicyChain_EarlyExhaustion(t *testing.T) {
	records := makeRecords(3)

	// First policy consumes all candidates.
	policy1 := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			return []FileGroup{{Records: candidates, ArchivePath: "x.clp.zst"}}
		},
	}

	// Second policy should never be called (no remaining candidates).
	policy2Called := false
	policy2 := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			policy2Called = true
			return nil
		},
	}

	chain := NewPolicyChain([]Policy{policy1, policy2})
	groups := chain.SelectFiles(records)

	if len(groups) != 1 {
		t.Errorf("groups = %d, want 1", len(groups))
	}
	if policy2Called {
		t.Error("policy2 should not be called when all candidates are consumed")
	}
}

func TestPolicyChain_EmptyCandidates(t *testing.T) {
	policy := &mockPolicy{
		selectFn: func(candidates []*metastore.FileRecord) []FileGroup {
			return nil
		},
	}

	chain := NewPolicyChain([]Policy{policy})
	groups := chain.SelectFiles(nil)

	if len(groups) != 0 {
		t.Errorf("groups = %d, want 0", len(groups))
	}
}

func TestCreatePolicyChain_Empty(t *testing.T) {
	// Empty config should produce default time_window policy.
	p, err := CreatePolicyChain(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("expected *TimeWindowPolicy for empty config, got %T", p)
	}
}

func TestCreatePolicyChain_Single(t *testing.T) {
	// Single config should produce unwrapped policy (not wrapped in chain).
	configs := []metastore.ConsolidationPolicyConfig{
		{Type: "time_window", WindowSize: "1h", MinFiles: 2, MaxFiles: 100},
	}
	p, err := CreatePolicyChain(configs)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*TimeWindowPolicy); !ok {
		t.Errorf("expected *TimeWindowPolicy for single config, got %T", p)
	}
}

func TestCreatePolicyChain_Multi(t *testing.T) {
	configs := []metastore.ConsolidationPolicyConfig{
		{Type: "time_window", WindowSize: "1h", MinFiles: 1, MaxFiles: 500},
		{Type: "time_window", WindowSize: "30m", MinFiles: 2, MaxFiles: 50},
	}
	p, err := CreatePolicyChain(configs)
	if err != nil {
		t.Fatal(err)
	}
	chain, ok := p.(*PolicyChain)
	if !ok {
		t.Fatalf("expected *PolicyChain for multi config, got %T", p)
	}
	if len(chain.policies) != 2 {
		t.Errorf("chain has %d policies, want 2", len(chain.policies))
	}
}

func TestCreatePolicyChain_InvalidDuration(t *testing.T) {
	configs := []metastore.ConsolidationPolicyConfig{
		{Type: "time_window", WindowSize: "not-a-duration"},
	}
	_, err := CreatePolicyChain(configs)
	if err == nil {
		t.Error("expected error for invalid duration")
	}
}

func TestCreatePolicyChain_UnknownType(t *testing.T) {
	configs := []metastore.ConsolidationPolicyConfig{
		{Type: "nonexistent"},
	}
	_, err := CreatePolicyChain(configs)
	if err == nil {
		t.Error("expected error for unknown policy type")
	}
}

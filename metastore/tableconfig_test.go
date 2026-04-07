package metastore

import (
	"encoding/json"
	"testing"
)

func TestTableConfig_DefaultOnNil(t *testing.T) {
	cfg, err := DecodeTableConfig(nil)
	if err != nil {
		t.Fatalf("DecodeTableConfig(nil): %v", err)
	}
	if !cfg.Consolidation.Enabled {
		t.Error("Consolidation.Enabled: got false, want true")
	}
	if !cfg.Retention.Enabled {
		t.Error("Retention.Enabled: got false, want true")
	}
	if cfg.Retention.Type != "default" {
		t.Errorf("Retention.Type: got %q, want %q", cfg.Retention.Type, "default")
	}
	if len(cfg.Consolidation.Policies) != 0 {
		t.Errorf("Consolidation.Policies: got %d, want 0", len(cfg.Consolidation.Policies))
	}
}

func TestTableConfig_RoundTrip(t *testing.T) {
	original := TableConfig{
		Consolidation: ConsolidationConfig{
			Enabled: true,
			Policies: []ConsolidationPolicyConfig{
				{
					Type:   "time_window",
					Config: json.RawMessage(`{"window_size":"30m","min_files":3,"max_files":50}`),
				},
				{
					Type:   "spark_job",
					Config: json.RawMessage(`{"grouping_dim_key":"application_id","job_timeout":"2h","min_files":1,"max_files":200}`),
				},
			},
		},
		Retention: RetentionConfig{
			Enabled: true,
			Type:    "custom",
		},
	}

	blob, err := EncodeTableConfig(original)
	if err != nil {
		t.Fatalf("EncodeTableConfig: %v", err)
	}

	decoded, err := DecodeTableConfig(blob)
	if err != nil {
		t.Fatalf("DecodeTableConfig: %v", err)
	}

	if decoded.Consolidation.Enabled != original.Consolidation.Enabled {
		t.Errorf("Consolidation.Enabled: got %v, want %v", decoded.Consolidation.Enabled, original.Consolidation.Enabled)
	}
	if decoded.Retention.Type != original.Retention.Type {
		t.Errorf("Retention.Type: got %q, want %q", decoded.Retention.Type, original.Retention.Type)
	}
	if len(decoded.Consolidation.Policies) != len(original.Consolidation.Policies) {
		t.Fatalf("Consolidation.Policies length: got %d, want %d",
			len(decoded.Consolidation.Policies), len(original.Consolidation.Policies))
	}

	p0 := decoded.Consolidation.Policies[0]
	if p0.Type != "time_window" {
		t.Errorf("policy[0].Type = %q, want time_window", p0.Type)
	}

	p1 := decoded.Consolidation.Policies[1]
	if p1.Type != "spark_job" {
		t.Errorf("policy[1].Type = %q, want spark_job", p1.Type)
	}
}


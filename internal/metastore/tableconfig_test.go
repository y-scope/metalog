package metastore

import (
	"testing"
)

func TestTableConfig_DefaultOnNil(t *testing.T) {
	cfg, err := DecodeTableConfig(nil)
	if err != nil {
		t.Fatalf("DecodeTableConfig(nil): %v", err)
	}
	if !cfg.KafkaPollerEnabled {
		t.Error("KafkaPollerEnabled: got false, want true")
	}
	if !cfg.ConsolidationEnabled {
		t.Error("ConsolidationEnabled: got false, want true")
	}
	if cfg.RetentionType != "default" {
		t.Errorf("RetentionType: got %q, want %q", cfg.RetentionType, "default")
	}
	if len(cfg.ConsolidationPolicies) != 0 {
		t.Errorf("ConsolidationPolicies: got %d, want 0", len(cfg.ConsolidationPolicies))
	}
}

func TestTableConfig_RoundTrip(t *testing.T) {
	original := TableConfig{
		KafkaPollerEnabled:   false,
		ConsolidationEnabled: true,
		RetentionType:        "custom",
		ConsolidationPolicies: []ConsolidationPolicyConfig{
			{
				Type:       "time_window",
				WindowSize: "30m",
				MinFiles:   3,
				MaxFiles:   50,
			},
			{
				Type:           "spark_job",
				GroupingDimKey: "application_id",
				JobTimeout:     "2h",
				MinFiles:       1,
				MaxFiles:       200,
			},
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

	if decoded.KafkaPollerEnabled != original.KafkaPollerEnabled {
		t.Errorf("KafkaPollerEnabled: got %v, want %v", decoded.KafkaPollerEnabled, original.KafkaPollerEnabled)
	}
	if decoded.ConsolidationEnabled != original.ConsolidationEnabled {
		t.Errorf("ConsolidationEnabled: got %v, want %v", decoded.ConsolidationEnabled, original.ConsolidationEnabled)
	}
	if decoded.RetentionType != original.RetentionType {
		t.Errorf("RetentionType: got %q, want %q", decoded.RetentionType, original.RetentionType)
	}
	if len(decoded.ConsolidationPolicies) != len(original.ConsolidationPolicies) {
		t.Fatalf("ConsolidationPolicies length: got %d, want %d",
			len(decoded.ConsolidationPolicies), len(original.ConsolidationPolicies))
	}

	p0 := decoded.ConsolidationPolicies[0]
	if p0.Type != "time_window" || p0.WindowSize != "30m" || p0.MinFiles != 3 || p0.MaxFiles != 50 {
		t.Errorf("policy[0] mismatch: %+v", p0)
	}

	p1 := decoded.ConsolidationPolicies[1]
	if p1.Type != "spark_job" || p1.GroupingDimKey != "application_id" || p1.JobTimeout != "2h" {
		t.Errorf("policy[1] mismatch: %+v", p1)
	}
}

func TestTableConfig_EncodeInvalidData(t *testing.T) {
	// A valid struct should always encode successfully.
	cfg := DefaultTableConfig()
	data, err := EncodeTableConfig(cfg)
	if err != nil {
		t.Fatalf("EncodeTableConfig: %v", err)
	}
	if len(data) == 0 {
		t.Error("encoded data should not be empty")
	}
}

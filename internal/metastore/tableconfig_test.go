package metastore

import (
	"testing"
)

func TestTableConfig_DefaultOnNil(t *testing.T) {
	cfg, err := DecodeTableConfig(nil)
	if err != nil {
		t.Fatalf("DecodeTableConfig(nil): %v", err)
	}
	if !cfg.Kafka.Enabled {
		t.Error("Kafka.Enabled: got false, want true")
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
		Kafka: KafkaConfig{
			Enabled:          false,
			Topic:            "spark-ir",
			BootstrapServers: "kafka:29092",
		},
		Consolidation: ConsolidationConfig{
			Enabled: true,
			Policies: []ConsolidationPolicyConfig{
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

	if decoded.Kafka.Enabled != original.Kafka.Enabled {
		t.Errorf("Kafka.Enabled: got %v, want %v", decoded.Kafka.Enabled, original.Kafka.Enabled)
	}
	if decoded.Kafka.Topic != original.Kafka.Topic {
		t.Errorf("Kafka.Topic: got %q, want %q", decoded.Kafka.Topic, original.Kafka.Topic)
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
	if p0.Type != "time_window" || p0.WindowSize != "30m" || p0.MinFiles != 3 || p0.MaxFiles != 50 {
		t.Errorf("policy[0] mismatch: %+v", p0)
	}

	p1 := decoded.Consolidation.Policies[1]
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

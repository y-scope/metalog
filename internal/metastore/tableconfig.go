package metastore

import (
	"encoding/json"
	"fmt"
)

// KafkaConfig holds Kafka consumer routing and settings for a table.
type KafkaConfig struct {
	Enabled           bool   `json:"enabled"`
	Topic             string `json:"topic"`
	BootstrapServers  string `json:"bootstrap_servers"`
	RecordTransformer string `json:"record_transformer,omitempty"`
}

// ConsolidationPolicyConfig describes a single consolidation policy.
// The Type field selects the policy type; Config holds policy-specific
// parameters as raw JSON, deserialized by each policy's factory.
type ConsolidationPolicyConfig struct {
	Type   string          `json:"type"`             // "time_window", "spark_job"
	Config json.RawMessage `json:"config,omitempty"` // policy-specific parameters
}

// ConsolidationConfig holds consolidation planner settings for a table.
type ConsolidationConfig struct {
	Enabled            bool                        `json:"enabled"`
	Policies           []ConsolidationPolicyConfig `json:"policies,omitempty"`
	StaleBufferingMins int                         `json:"stale_buffering_mins,omitempty"` // default 60; negative disables
}

// RetentionConfig holds retention lifecycle settings for a table.
type RetentionConfig struct {
	Enabled bool   `json:"enabled"`
	Type    string `json:"type"`
}

// TableConfig holds per-table configuration stored as a JSON string in the
// _table_config.config MEDIUMTEXT column. A NULL value means all defaults.
//
// Each coordinator subsystem owns its enabled flag and config under a single
// key: kafka, consolidation, retention.
//
// JSON is used instead of LZ4+msgpack because this table has very few rows
// (one per managed table) and the payloads are tiny (~100 bytes). Plain JSON
// keeps the config human-readable via a simple SELECT and avoids compression
// overhead that would actually increase size at this scale.
type TableConfig struct {
	Kafka         KafkaConfig         `json:"kafka"`
	Consolidation ConsolidationConfig `json:"consolidation"`
	Retention     RetentionConfig     `json:"retention"`
}

// DefaultTableConfig returns a TableConfig with all default values.
func DefaultTableConfig() TableConfig {
	return TableConfig{
		Kafka:         KafkaConfig{Enabled: true},
		Consolidation: ConsolidationConfig{Enabled: true},
		Retention:     RetentionConfig{Enabled: true, Type: "default"},
	}
}

// DecodeTableConfig decodes a JSON config blob from the database.
// A nil or empty blob (SQL NULL) returns DefaultTableConfig().
func DecodeTableConfig(blob []byte) (TableConfig, error) {
	if len(blob) == 0 {
		return DefaultTableConfig(), nil
	}
	var cfg TableConfig
	if err := json.Unmarshal(blob, &cfg); err != nil {
		return TableConfig{}, fmt.Errorf("decode table config: %w", err)
	}
	return cfg, nil
}

// EncodeTableConfig encodes a TableConfig to JSON bytes for DB storage.
func EncodeTableConfig(cfg TableConfig) ([]byte, error) {
	data, err := json.Marshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("encode table config: %w", err)
	}
	return data, nil
}

package metastore

import (
	"encoding/json"
	"fmt"
)

// KafkaConfig holds Kafka consumer routing for a table.
type KafkaConfig struct {
	Topic             string `json:"topic"`
	BootstrapServers  string `json:"bootstrap_servers"`
	RecordTransformer string `json:"record_transformer,omitempty"`
}

// ConsolidationPolicyConfig describes a single consolidation policy.
// Durations are stored as strings because time.Duration has no native JSON
// representation. Parse with time.ParseDuration at use sites.
type ConsolidationPolicyConfig struct {
	Type           string `json:"type"`                            // "time_window", "spark_job", "audit"
	WindowSize     string `json:"window_size,omitempty"`           // e.g. "1h", "30m"
	MinFiles       int    `json:"min_files,omitempty"`             // default 2
	MaxFiles       int    `json:"max_files,omitempty"`             // default 100
	GroupingDimKey string `json:"grouping_dim_key,omitempty"`      // spark_job only
	JobTimeout     string `json:"job_timeout,omitempty"`           // spark_job only, e.g. "2h"
}

// TableConfig holds per-table configuration stored as a JSON string in the
// _table_config.config MEDIUMTEXT column. A NULL value means all defaults.
//
// JSON is used instead of LZ4+msgpack because this table has very few rows
// (one per managed table) and the payloads are tiny (~100 bytes). Plain JSON
// keeps the config human-readable via a simple SELECT and avoids compression
// overhead that would actually increase size at this scale.
type TableConfig struct {
	KafkaPollerEnabled         bool                        `json:"kafka_poller_enabled"`
	ConsolidationEnabled       bool                        `json:"consolidation_enabled"`
	RetentionManagementEnabled bool                        `json:"retention_management_enabled"`
	RetentionType              string                      `json:"retention_type"`
	Kafka                      *KafkaConfig                `json:"kafka,omitempty"`
	ConsolidationPolicies      []ConsolidationPolicyConfig `json:"consolidation_policies,omitempty"`
}

// DefaultTableConfig returns a TableConfig with all default values.
func DefaultTableConfig() TableConfig {
	return TableConfig{
		KafkaPollerEnabled:         true,
		ConsolidationEnabled:       true,
		RetentionManagementEnabled: true,
		RetentionType:        "default",
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

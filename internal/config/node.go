package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NodeConfig is the top-level configuration loaded from node.yaml.
type NodeConfig struct {
	Database    DatabaseConfig      `yaml:"database"`
	Storage     ObjectStorageConfig `yaml:"storage"`
	Server      ServerConfig        `yaml:"server"`
	Coordinator CoordinatorConfig   `yaml:"coordinator"`
	Tables      []TableConfig       `yaml:"tables"`
	Worker      WorkerConfig        `yaml:"worker"`
}

// HAStrategy selects the liveness detection mode.
type HAStrategy string

const (
	HAStrategyHeartbeat HAStrategy = "heartbeat"
	HAStrategyLease     HAStrategy = "lease"
)

// ServerConfig controls network-facing settings (gRPC + health endpoints).
type ServerConfig struct {
	Health HealthConfig `yaml:"health"`
	GRPC   GRPCConfig   `yaml:"grpc"`
}

// CoordinatorConfig holds coordinator-specific settings (HA, identity).
type CoordinatorConfig struct {
	Name         string `yaml:"name"`
	NodeIDEnvVar string `yaml:"nodeIdEnvVar"`

	ReconciliationIntervalSeconds int `yaml:"reconciliationIntervalSeconds"`

	// HA settings
	HAStrategy                  HAStrategy `yaml:"haStrategy"`
	HeartbeatIntervalSeconds    int        `yaml:"heartbeatIntervalSeconds"`
	DeadNodeThresholdSeconds    int        `yaml:"deadNodeThresholdSeconds"`
	LeaseTTLSeconds             int        `yaml:"leaseTtlSeconds"`
	LeaseRenewalIntervalSeconds int        `yaml:"leaseRenewalIntervalSeconds"`
}

// HealthConfig controls the HTTP health endpoint.
type HealthConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// GRPCConfig controls the unified gRPC server.
type GRPCConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

// TableConfig defines a table declared in the config file.
type TableConfig struct {
	Name        string           `yaml:"name"`
	DisplayName string           `yaml:"displayName"`
	Kafka       TableKafkaConfig `yaml:"kafka"`
}

// TableKafkaConfig holds Kafka settings for a single table.
type TableKafkaConfig struct {
	Topic             string `yaml:"topic"`
	BootstrapServers  string `yaml:"bootstrapServers"`
	RecordTransformer string `yaml:"recordTransformer"`
}

// WorkerConfig holds worker settings.
type WorkerConfig struct {
	Concurrency int            `yaml:"concurrency"`
	Database   *DatabaseConfig `yaml:"database"`
}

// ResolveNodeID reads the node ID from the environment variable specified
// in the config. Falls back to os.Hostname() if the env var is not set.
func (c *NodeConfig) ResolveNodeID() string {
	envVar := c.Coordinator.NodeIDEnvVar
	if envVar == "" {
		envVar = "HOSTNAME"
	}
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}

// LoadNodeConfig reads and parses a YAML node configuration file.
func LoadNodeConfig(path string) (*NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg NodeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := cfg.validateRaw(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	applyDefaults(&cfg)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// validateRaw checks user-provided values before defaults are applied.
// This catches negative or otherwise invalid values that would be masked by applyDefaults.
func (c *NodeConfig) validateRaw() error {
	if c.Coordinator.LeaseTTLSeconds < 0 {
		return fmt.Errorf("coordinator.leaseTtlSeconds must be non-negative, got %d", c.Coordinator.LeaseTTLSeconds)
	}
	if c.Coordinator.LeaseRenewalIntervalSeconds < 0 {
		return fmt.Errorf("coordinator.leaseRenewalIntervalSeconds must be non-negative, got %d", c.Coordinator.LeaseRenewalIntervalSeconds)
	}
	if c.Coordinator.HeartbeatIntervalSeconds < 0 {
		return fmt.Errorf("coordinator.heartbeatIntervalSeconds must be non-negative, got %d", c.Coordinator.HeartbeatIntervalSeconds)
	}
	if c.Coordinator.DeadNodeThresholdSeconds < 0 {
		return fmt.Errorf("coordinator.deadNodeThresholdSeconds must be non-negative, got %d", c.Coordinator.DeadNodeThresholdSeconds)
	}
	if c.Coordinator.ReconciliationIntervalSeconds < 0 {
		return fmt.Errorf("coordinator.reconciliationIntervalSeconds must be non-negative, got %d", c.Coordinator.ReconciliationIntervalSeconds)
	}
	return nil
}

func (c *NodeConfig) validate() error {
	if c.Database.Host == "" {
		return fmt.Errorf("database.host is required")
	}
	if c.Database.Port < 1 || c.Database.Port > 65535 {
		return fmt.Errorf("database.port must be 1-65535, got %d", c.Database.Port)
	}
	if c.Server.Health.Enabled {
		if c.Server.Health.Port < 1 || c.Server.Health.Port > 65535 {
			return fmt.Errorf("server.health.port must be 1-65535, got %d", c.Server.Health.Port)
		}
	}
	if c.Server.GRPC.Enabled {
		if c.Server.GRPC.Port < 1 || c.Server.GRPC.Port > 65535 {
			return fmt.Errorf("server.grpc.port must be 1-65535, got %d", c.Server.GRPC.Port)
		}
	}
	for i, t := range c.Tables {
		if t.Name == "" {
			return fmt.Errorf("tables[%d].name is required", i)
		}
	}
	switch c.Coordinator.HAStrategy {
	case HAStrategyHeartbeat, HAStrategyLease:
	default:
		return fmt.Errorf("coordinator.haStrategy must be 'heartbeat' or 'lease', got %q", c.Coordinator.HAStrategy)
	}
	if c.Coordinator.HAStrategy == HAStrategyLease {
		if c.Coordinator.LeaseRenewalIntervalSeconds >= c.Coordinator.LeaseTTLSeconds {
			return fmt.Errorf("coordinator.leaseRenewalIntervalSeconds (%d) must be less than coordinator.leaseTtlSeconds (%d)",
				c.Coordinator.LeaseRenewalIntervalSeconds, c.Coordinator.LeaseTTLSeconds)
		}
	}
	return nil
}

func applyDefaults(cfg *NodeConfig) {
	if cfg.Database.Port == 0 {
		cfg.Database.Port = 3306
	}
	if cfg.Server.Health.Port == 0 {
		cfg.Server.Health.Port = 8081
	}
	if cfg.Server.GRPC.Port == 0 {
		cfg.Server.GRPC.Port = 9090
	}
	if cfg.Coordinator.ReconciliationIntervalSeconds == 0 {
		cfg.Coordinator.ReconciliationIntervalSeconds = 60
	}
	if cfg.Worker.Concurrency == 0 {
		cfg.Worker.Concurrency = 4
	}
	if cfg.Storage.ClpProcessTimeoutSeconds == 0 {
		cfg.Storage.ClpProcessTimeoutSeconds = 300
	}
	if cfg.Coordinator.HAStrategy == "" {
		cfg.Coordinator.HAStrategy = HAStrategyHeartbeat
	}
	if cfg.Coordinator.HeartbeatIntervalSeconds == 0 {
		cfg.Coordinator.HeartbeatIntervalSeconds = 30
	}
	if cfg.Coordinator.DeadNodeThresholdSeconds == 0 {
		cfg.Coordinator.DeadNodeThresholdSeconds = 180
	}
	if cfg.Coordinator.LeaseTTLSeconds == 0 {
		cfg.Coordinator.LeaseTTLSeconds = 180
	}
	if cfg.Coordinator.LeaseRenewalIntervalSeconds == 0 {
		cfg.Coordinator.LeaseRenewalIntervalSeconds = 30
	}
}

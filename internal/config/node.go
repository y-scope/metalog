package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NodeConfig is the top-level configuration loaded from node.yaml.
type NodeConfig struct {
	Node   NodeSettings  `yaml:"node"`
	Tables []TableConfig `yaml:"tables"`
	Worker WorkerConfig  `yaml:"worker"`
}

// HAStrategy selects the liveness detection mode.
type HAStrategy string

const (
	HAStrategyHeartbeat HAStrategy = "heartbeat"
	HAStrategyLease     HAStrategy = "lease"
)

// NodeSettings contains core node configuration.
type NodeSettings struct {
	Name                          string              `yaml:"name"`
	NodeIDEnvVar                  string              `yaml:"nodeIdEnvVar"`
	Database                      DatabaseConfig      `yaml:"database"`
	Storage                       ObjectStorageConfig `yaml:"storage"`
	Health                        HealthConfig        `yaml:"health"`
	GRPC                          GRPCConfig          `yaml:"grpc"`
	ReconciliationIntervalSeconds int                 `yaml:"reconciliationIntervalSeconds"`

	// HA settings
	CoordinatorHAStrategy       HAStrategy `yaml:"coordinatorHaStrategy"`
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
	NumWorkers int             `yaml:"numWorkers"`
	Database   *DatabaseConfig `yaml:"database"`
}

// ResolveNodeID reads the node ID from the environment variable specified
// in the config. Falls back to os.Hostname() if the env var is not set.
func (c *NodeConfig) ResolveNodeID() string {
	envVar := c.Node.NodeIDEnvVar
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
	if c.Node.LeaseTTLSeconds < 0 {
		return fmt.Errorf("node.leaseTtlSeconds must be non-negative, got %d", c.Node.LeaseTTLSeconds)
	}
	if c.Node.LeaseRenewalIntervalSeconds < 0 {
		return fmt.Errorf("node.leaseRenewalIntervalSeconds must be non-negative, got %d", c.Node.LeaseRenewalIntervalSeconds)
	}
	if c.Node.HeartbeatIntervalSeconds < 0 {
		return fmt.Errorf("node.heartbeatIntervalSeconds must be non-negative, got %d", c.Node.HeartbeatIntervalSeconds)
	}
	if c.Node.DeadNodeThresholdSeconds < 0 {
		return fmt.Errorf("node.deadNodeThresholdSeconds must be non-negative, got %d", c.Node.DeadNodeThresholdSeconds)
	}
	if c.Node.ReconciliationIntervalSeconds < 0 {
		return fmt.Errorf("node.reconciliationIntervalSeconds must be non-negative, got %d", c.Node.ReconciliationIntervalSeconds)
	}
	return nil
}

func (c *NodeConfig) validate() error {
	if c.Node.Database.Host == "" {
		return fmt.Errorf("node.database.host is required")
	}
	if c.Node.Database.Port < 1 || c.Node.Database.Port > 65535 {
		return fmt.Errorf("node.database.port must be 1-65535, got %d", c.Node.Database.Port)
	}
	if c.Node.Health.Enabled {
		if c.Node.Health.Port < 1 || c.Node.Health.Port > 65535 {
			return fmt.Errorf("node.health.port must be 1-65535, got %d", c.Node.Health.Port)
		}
	}
	if c.Node.GRPC.Enabled {
		if c.Node.GRPC.Port < 1 || c.Node.GRPC.Port > 65535 {
			return fmt.Errorf("node.grpc.port must be 1-65535, got %d", c.Node.GRPC.Port)
		}
	}
	for i, t := range c.Tables {
		if t.Name == "" {
			return fmt.Errorf("tables[%d].name is required", i)
		}
	}
	switch c.Node.CoordinatorHAStrategy {
	case HAStrategyHeartbeat, HAStrategyLease:
	default:
		return fmt.Errorf("node.coordinatorHaStrategy must be 'heartbeat' or 'lease', got %q", c.Node.CoordinatorHAStrategy)
	}
	if c.Node.CoordinatorHAStrategy == HAStrategyLease {
		if c.Node.LeaseRenewalIntervalSeconds >= c.Node.LeaseTTLSeconds {
			return fmt.Errorf("node.leaseRenewalIntervalSeconds (%d) must be less than node.leaseTtlSeconds (%d)",
				c.Node.LeaseRenewalIntervalSeconds, c.Node.LeaseTTLSeconds)
		}
	}
	return nil
}

func applyDefaults(cfg *NodeConfig) {
	if cfg.Node.Database.Port == 0 {
		cfg.Node.Database.Port = 3306
	}
	if cfg.Node.Health.Port == 0 {
		cfg.Node.Health.Port = 8081
	}
	if cfg.Node.GRPC.Port == 0 {
		cfg.Node.GRPC.Port = 9090
	}
	if cfg.Node.ReconciliationIntervalSeconds == 0 {
		cfg.Node.ReconciliationIntervalSeconds = 60
	}
	if cfg.Worker.NumWorkers == 0 {
		cfg.Worker.NumWorkers = 4
	}
	if cfg.Node.Storage.ClpProcessTimeoutSeconds == 0 {
		cfg.Node.Storage.ClpProcessTimeoutSeconds = 300
	}
	if cfg.Node.CoordinatorHAStrategy == "" {
		cfg.Node.CoordinatorHAStrategy = HAStrategyHeartbeat
	}
	if cfg.Node.HeartbeatIntervalSeconds == 0 {
		cfg.Node.HeartbeatIntervalSeconds = 30
	}
	if cfg.Node.DeadNodeThresholdSeconds == 0 {
		cfg.Node.DeadNodeThresholdSeconds = 180
	}
	if cfg.Node.LeaseTTLSeconds == 0 {
		cfg.Node.LeaseTTLSeconds = 180
	}
	if cfg.Node.LeaseRenewalIntervalSeconds == 0 {
		cfg.Node.LeaseRenewalIntervalSeconds = 30
	}
}

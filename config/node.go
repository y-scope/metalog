// Package config provides configuration types and loaders for metalog processes.
//
// It supports YAML-based configuration with environment variable overrides.
// The central type is [NodeConfig], which defines database connections, storage
// backends, server settings, and coordinator HA parameters.
package config

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/y-scope/metalog/telemetry"
)

// NodeConfig is the top-level configuration loaded from node.yaml.
type NodeConfig struct {
	Database    DatabaseSection     `yaml:"database"`
	Telemetry   telemetry.Config    `yaml:"telemetry"`
	Storage     ObjectStorageConfig `yaml:"storage"`
	GRPC        GRPCConfig          `yaml:"grpc"`
	Worker      WorkerConfig        `yaml:"worker"`
	Coordinator CoordinatorConfig   `yaml:"coordinator"`
	Health      HealthConfig        `yaml:"health"`
	Logging     LoggingConfig       `yaml:"logging"`
}

// LoggingConfig holds logging tuning parameters.
type LoggingConfig struct {
	// FailureLogIntervalSeconds controls how often repeated failure warnings
	// are emitted by periodic loops (liveness, planner, retention). The first
	// failure is always logged immediately; subsequent failures repeat at
	// this interval until recovery. Default: 60.
	FailureLogIntervalSeconds int `yaml:"failureLogIntervalSeconds"`
}

// FailureLogInterval returns the failure log interval as a time.Duration.
func (c *LoggingConfig) FailureLogInterval() time.Duration {
	if c.FailureLogIntervalSeconds <= 0 {
		return 60 * time.Second
	}
	return time.Duration(c.FailureLogIntervalSeconds) * time.Second
}

// DatabaseSection holds primary (RW) and optional replica (RO) pool configs.
type DatabaseSection struct {
	Replica *DatabaseConfig `yaml:"replica" envprefix:"DB_REPLICA_"`
	Primary DatabaseConfig  `yaml:"primary" envprefix:"DB_PRIMARY_"`
}

// HAStrategy selects the liveness detection mode.
type HAStrategy string

const (
	HAStrategyHeartbeat HAStrategy = "heartbeat"
	HAStrategyLease     HAStrategy = "lease"
)

// CoordinatorConfig holds coordinator-specific settings (HA, identity).
type CoordinatorConfig struct {
	Name                          string     `yaml:"name" env:"COORDINATOR_NAME"`
	NodeIDEnvVar                  string     `yaml:"nodeIdEnvVar"`
	TableCompression              string     `yaml:"tableCompression"`
	HAStrategy                    HAStrategy `yaml:"haStrategy"`
	ReconciliationIntervalSeconds int        `yaml:"reconciliationIntervalSeconds"`
	HeartbeatIntervalSeconds      int        `yaml:"heartbeatIntervalSeconds"`
	DeadNodeThresholdSeconds      int        `yaml:"deadNodeThresholdSeconds"`
	LeaseTTLSeconds               int        `yaml:"leaseTtlSeconds"`
	LeaseRenewalIntervalSeconds   int        `yaml:"leaseRenewalIntervalSeconds"`
	Enabled                       bool       `yaml:"enabled"`
}

// HealthConfig controls the HTTP health endpoint.
type HealthConfig struct {
	Enabled bool `yaml:"enabled" env:"HEALTH_ENABLED"`
	Port    int  `yaml:"port" env:"HEALTH_PORT"`
}

// GRPCConfig controls the unified gRPC server and per-service toggles.
// The gRPC server starts if any service is enabled. If the section is absent,
// no gRPC server is started.
type GRPCConfig struct {
	BlockingIngestion *bool `yaml:"blockingIngestion"`
	Port              int   `yaml:"port" env:"GRPC_PORT"`
	Ingestion         bool  `yaml:"ingestion"`
	Admin             bool  `yaml:"admin"`
	Query             bool  `yaml:"query"`
	Metadata          bool  `yaml:"metadata"`
}

// IsBlockingIngestion returns whether gRPC ingestion uses blocking submit.
// Defaults to true if not explicitly set.
func (c *GRPCConfig) IsBlockingIngestion() bool {
	if c.BlockingIngestion == nil {
		return true
	}
	return *c.BlockingIngestion
}

// HasAnyService returns true if at least one gRPC service is enabled.
func (c *GRPCConfig) HasAnyService() bool {
	return c.Ingestion || c.Admin || c.Query || c.Metadata
}

// WorkerConfig holds worker settings.
type WorkerConfig struct {
	ClpBinaryPath            string `yaml:"clpBinaryPath"`
	Concurrency              int    `yaml:"concurrency"`
	ClpProcessTimeoutSeconds int    `yaml:"clpProcessTimeoutSeconds"`
}

// ResolveNodeID reads the node ID from the environment variable specified
// in the config. Falls back to os.Hostname() if the env var is not set.
// Returns an error if no node ID can be determined.
func (c *NodeConfig) ResolveNodeID() (string, error) {
	envVar := c.Coordinator.NodeIDEnvVar
	if envVar == "" {
		envVar = "HOSTNAME"
	}
	if v := os.Getenv(envVar); v != "" {
		return v, nil
	}
	h, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve node ID: hostname lookup failed: %w", err)
	}
	if h == "" {
		return "", fmt.Errorf("resolve node ID: hostname is empty and $%s is not set", envVar)
	}
	return h, nil
}

// LoadNodeConfig reads and parses a YAML node configuration file.
// Defaults and validation are driven by which sections are present in the YAML:
// coordinator section present → coordinator defaults/validation applied,
// worker section with concurrency > 0 → workers enabled, etc.
func LoadNodeConfig(path string) (*NodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg NodeConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if err := ApplyEnvOverrides(&cfg); err != nil {
		return nil, fmt.Errorf("env overrides: %w", err)
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

// EffectiveReplica returns the replica database config if set,
// otherwise falls back to the primary database config.
func (c *NodeConfig) EffectiveReplica() DatabaseConfig {
	if c.Database.Replica != nil && c.Database.Replica.Host != "" {
		return *c.Database.Replica
	}
	return c.Database.Primary
}

// HasCoordinator returns true if the coordinator subsystem is enabled.
func (c *NodeConfig) HasCoordinator() bool {
	return c.Coordinator.Enabled
}

// validateRaw checks user-provided values before defaults are applied.
func (c *NodeConfig) validateRaw() error {
	if c.HasCoordinator() {
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
	}
	return nil
}

func (c *NodeConfig) validate() error {
	hasPrimary := c.Database.Primary.Host != ""
	hasReplica := c.Database.Replica != nil && c.Database.Replica.Host != ""
	if !hasPrimary && !hasReplica {
		return fmt.Errorf("at least one of database.primary or database.replica must be configured")
	}
	if hasPrimary {
		if c.Database.Primary.Port < 1 || c.Database.Primary.Port > 65535 {
			return fmt.Errorf("database.primary.port must be 1-65535, got %d", c.Database.Primary.Port)
		}
	}
	if hasReplica {
		if c.Database.Replica.Port < 1 || c.Database.Replica.Port > 65535 {
			return fmt.Errorf("database.replica.port must be 1-65535, got %d", c.Database.Replica.Port)
		}
	}
	if c.Health.Enabled {
		if c.Health.Port < 1 || c.Health.Port > 65535 {
			return fmt.Errorf("health.port must be 1-65535, got %d", c.Health.Port)
		}
	}
	if c.GRPC.HasAnyService() {
		if c.GRPC.Port < 1 || c.GRPC.Port > 65535 {
			return fmt.Errorf("grpc.port must be 1-65535, got %d", c.GRPC.Port)
		}
		// Ingestion and admin require primary database
		if !hasPrimary && (c.GRPC.Ingestion || c.GRPC.Admin) {
			return fmt.Errorf("database.primary is required when grpc.ingestion or grpc.admin is enabled")
		}
		// Ingestion requires coordinator (tables + batching writer)
		if c.GRPC.Ingestion && !c.HasCoordinator() {
			return fmt.Errorf("grpc.ingestion requires coordinator.enabled=true")
		}
	}

	// Coordinator and workers require primary database
	if !hasPrimary && (c.HasCoordinator() || c.Worker.Concurrency > 0) {
		return fmt.Errorf("database.primary is required when coordinator or worker is enabled")
	}

	// Storage validation — defaultBackend must reference a known backend
	if c.Storage.DefaultBackend != "" {
		if _, ok := c.Storage.Backends[c.Storage.DefaultBackend]; !ok {
			return fmt.Errorf("storage.defaultBackend %q not found in storage.backends", c.Storage.DefaultBackend)
		}
	}

	// Workers require clp-s binary
	if c.Worker.Concurrency > 0 && c.Worker.ClpBinaryPath == "" {
		return fmt.Errorf("clp-s binary not found: set worker.clpBinaryPath or add clp-s to $PATH")
	}

	// Coordinator validation
	if c.HasCoordinator() {
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
	}

	return nil
}

func applyDefaults(cfg *NodeConfig) {
	if cfg.Health.Port == 0 {
		cfg.Health.Port = 8081
	}
	if cfg.GRPC.Port == 0 {
		cfg.GRPC.Port = 9090
	}
	if cfg.Logging.FailureLogIntervalSeconds == 0 {
		cfg.Logging.FailureLogIntervalSeconds = 60
	}

	// Coordinator defaults — only when coordinator section is present
	if cfg.HasCoordinator() {
		if cfg.Coordinator.ReconciliationIntervalSeconds == 0 {
			cfg.Coordinator.ReconciliationIntervalSeconds = 60
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

	// Storage defaults — auto-provision a local filesystem backend when
	// no storage is configured. This lets minimal configs (e.g., metadata-only
	// nodes or integration tests) work without requiring a storage section.
	if cfg.Storage.DefaultBackend == "" {
		cfg.Storage.DefaultBackend = "local"
		if cfg.Storage.Backends == nil {
			cfg.Storage.Backends = make(map[string]StorageBackendConfig)
		}
		if _, ok := cfg.Storage.Backends["local"]; !ok {
			cfg.Storage.Backends["local"] = StorageBackendConfig{
				Type:     "fs",
				BasePath: filepath.Join(os.TempDir(), "clp-storage"),
			}
		}
	}

	// No worker concurrency default — absence means disabled.
	// Users must explicitly set worker.concurrency > 0 to enable workers.

	if cfg.Worker.Concurrency > 0 {
		// Resolve clp-s binary: explicit path > $PATH lookup
		if cfg.Worker.ClpBinaryPath == "" {
			if p, err := exec.LookPath("clp-s"); err == nil {
				cfg.Worker.ClpBinaryPath = p
			}
		}
		if cfg.Worker.ClpProcessTimeoutSeconds == 0 {
			cfg.Worker.ClpProcessTimeoutSeconds = 300
		}
	}
}

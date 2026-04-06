use std::path::PathBuf;

use serde::Deserialize;

use crate::{database::DatabaseSection, storage::ObjectStorageConfig};

/// Top-level node configuration loaded from `node.yaml`.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub database: DatabaseSection,

    #[serde(default)]
    pub storage: ObjectStorageConfig,

    #[serde(default)]
    pub grpc: GRPCConfig,

    #[serde(default = "default_kafka_driver")]
    pub kafka_driver: String,

    #[serde(default)]
    pub worker: WorkerConfig,

    #[serde(default)]
    pub coordinator: CoordinatorConfig,

    #[serde(default)]
    pub health: HealthConfig,

    #[serde(default)]
    pub logging: LoggingConfig,

    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

fn default_kafka_driver() -> String {
    "rdkafka".into()
}

impl NodeConfig {
    /// Loads a `NodeConfig` from a YAML file.
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Io(path.into(), e))?;
        let config: NodeConfig =
            serde_yaml::from_str(&content).map_err(|e| ConfigError::Parse(path.into(), e))?;
        config.validate()?;
        Ok(config)
    }

    /// Resolves the node ID from environment or hostname.
    pub fn resolve_node_id(&self) -> Result<String, ConfigError> {
        let env_var = if self.coordinator.node_id_env_var.is_empty() {
            "HOSTNAME"
        } else {
            &self.coordinator.node_id_env_var
        };

        if let Ok(val) = std::env::var(env_var) {
            if !val.is_empty() {
                return Ok(val);
            }
        }

        hostname::get()
            .map_err(|e| ConfigError::Validation(format!("resolve hostname: {e}")))?
            .into_string()
            .map_err(|_| ConfigError::Validation("hostname is not valid UTF-8".into()))
    }

    /// Returns the replica config if set, otherwise the primary.
    pub fn effective_replica(&self) -> &crate::DatabaseConfig {
        self.database
            .replica
            .as_ref()
            .unwrap_or(&self.database.primary)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.health.enabled && self.health.port == 0 {
            return Err(ConfigError::Validation(
                "health.port must be non-zero when enabled".into(),
            ));
        }
        Ok(())
    }
}

/// HA strategy for coordinator liveness detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HAStrategy {
    #[default]
    Heartbeat,
    Lease,
}

/// Coordinator role configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CoordinatorConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub node_id_env_var: String,

    #[serde(default)]
    pub table_compression: String,

    #[serde(default)]
    pub ha_strategy: HAStrategy,

    #[serde(default = "default_reconciliation_interval")]
    pub reconciliation_interval_secs: u64,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    #[serde(default = "default_dead_node_threshold")]
    pub dead_node_threshold_secs: u64,

    #[serde(default = "default_lease_ttl")]
    pub lease_ttl_secs: u64,

    #[serde(default = "default_lease_renewal")]
    pub lease_renewal_interval_secs: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            name: String::new(),
            node_id_env_var: String::new(),
            table_compression: String::new(),
            ha_strategy: HAStrategy::default(),
            reconciliation_interval_secs: default_reconciliation_interval(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            dead_node_threshold_secs: default_dead_node_threshold(),
            lease_ttl_secs: default_lease_ttl(),
            lease_renewal_interval_secs: default_lease_renewal(),
        }
    }
}

fn default_reconciliation_interval() -> u64 {
    60
}

fn default_heartbeat_interval() -> u64 {
    30
}

fn default_dead_node_threshold() -> u64 {
    180
}

fn default_lease_ttl() -> u64 {
    180
}

fn default_lease_renewal() -> u64 {
    30
}

/// gRPC server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct GRPCConfig {
    #[serde(default = "default_grpc_port")]
    pub port: u16,

    /// If None or true, ingestion blocks when the channel is full.
    pub blocking_ingestion: Option<bool>,

    #[serde(default)]
    pub ingestion: ServiceConfig,

    #[serde(default)]
    pub admin: ServiceConfig,

    #[serde(default)]
    pub query: ServiceConfig,

    #[serde(default)]
    pub metadata: ServiceConfig,
}

fn default_grpc_port() -> u16 {
    9090
}

impl Default for GRPCConfig {
    fn default() -> Self {
        Self {
            port: default_grpc_port(),
            blocking_ingestion: None,
            ingestion: ServiceConfig::default(),
            admin: ServiceConfig::default(),
            query: ServiceConfig::default(),
            metadata: ServiceConfig::default(),
        }
    }
}

impl GRPCConfig {
    /// Returns true if ingestion should block (default: true).
    pub fn is_blocking_ingestion(&self) -> bool {
        self.blocking_ingestion.unwrap_or(true)
    }

    /// Returns true if any gRPC service is enabled.
    pub fn has_any_service(&self) -> bool {
        self.ingestion.enabled || self.admin.enabled || self.query.enabled || self.metadata.enabled
    }
}

/// Per-service toggle.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServiceConfig {
    #[serde(default)]
    pub enabled: bool,
}

/// Worker pool configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    #[serde(default)]
    pub clp_binary_path: String,

    #[serde(default)]
    pub concurrency: u32,

    #[serde(default = "default_clp_timeout")]
    pub clp_process_timeout_secs: u64,
}

fn default_clp_timeout() -> u64 {
    300
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            clp_binary_path: String::new(),
            concurrency: 0,
            clp_process_timeout_secs: default_clp_timeout(),
        }
    }
}

impl WorkerConfig {
    /// Resolves the CLP binary path: explicit config, or search $PATH.
    pub fn resolve_clp_binary(&self) -> Option<PathBuf> {
        if !self.clp_binary_path.is_empty() {
            return Some(PathBuf::from(&self.clp_binary_path));
        }
        which::which("clp-s").ok()
    }
}

/// HTTP health probe configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_health_port")]
    pub port: u16,
}

fn default_health_port() -> u16 {
    8081
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_health_port(),
        }
    }
}

/// Logging tuning.
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    /// Minimum interval between repeated failure log messages (seconds).
    #[serde(default = "default_failure_log_interval")]
    pub failure_log_interval_secs: u64,
}

fn default_failure_log_interval() -> u64 {
    60
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            failure_log_interval_secs: default_failure_log_interval(),
        }
    }
}

impl LoggingConfig {
    /// Returns the failure log interval as a `Duration`.
    pub fn failure_log_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.failure_log_interval_secs)
    }
}

/// Telemetry configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_exporter")]
    pub exporter: String,
}

fn default_exporter() -> String {
    "prometheus".into()
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exporter: default_exporter(),
        }
    }
}

/// Configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config file {0}: {1}")]
    Io(String, std::io::Error),

    #[error("config parse {0}: {1}")]
    Parse(String, serde_yaml::Error),

    #[error("config validation: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_yaml() -> &'static str {
        r#"
database:
  primary:
    database: metalog_test
"#
    }

    #[test]
    fn parse_minimal() {
        let cfg: NodeConfig = serde_yaml::from_str(minimal_yaml()).unwrap();
        assert_eq!(cfg.database.primary.database, "metalog_test");
        assert_eq!(cfg.database.primary.host, "localhost");
        assert_eq!(cfg.database.primary.port, 3306);
        assert_eq!(cfg.grpc.port, 9090);
        assert_eq!(cfg.health.port, 8081);
        assert!(!cfg.coordinator.enabled);
    }

    #[test]
    fn blocking_ingestion_default_true() {
        let cfg = GRPCConfig::default();
        assert!(cfg.is_blocking_ingestion());
    }

    #[test]
    fn blocking_ingestion_explicit_false() {
        let yaml = "blocking_ingestion: false\nport: 9090";
        let cfg: GRPCConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.is_blocking_ingestion());
    }

    #[test]
    fn ha_strategy_default() {
        let cfg = CoordinatorConfig::default();
        assert_eq!(cfg.ha_strategy, HAStrategy::Heartbeat);
    }

    #[test]
    fn ha_strategy_lease() {
        let yaml = "ha_strategy: lease\nenabled: true";
        let cfg: CoordinatorConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.ha_strategy, HAStrategy::Lease);
    }

    #[test]
    fn effective_replica_fallback() {
        let cfg: NodeConfig = serde_yaml::from_str(minimal_yaml()).unwrap();
        assert_eq!(
            cfg.effective_replica().database,
            cfg.database.primary.database
        );
    }

    #[test]
    fn has_any_service() {
        let mut cfg = GRPCConfig::default();
        assert!(!cfg.has_any_service());
        cfg.ingestion.enabled = true;
        assert!(cfg.has_any_service());
    }

    #[test]
    fn failure_log_interval() {
        let cfg = LoggingConfig::default();
        assert_eq!(
            cfg.failure_log_interval(),
            std::time::Duration::from_secs(60)
        );
    }

    #[test]
    fn coordinator_defaults() {
        let cfg = CoordinatorConfig::default();
        assert_eq!(cfg.reconciliation_interval_secs, 60);
        assert_eq!(cfg.heartbeat_interval_secs, 30);
        assert_eq!(cfg.dead_node_threshold_secs, 180);
        assert_eq!(cfg.lease_ttl_secs, 180);
        assert_eq!(cfg.lease_renewal_interval_secs, 30);
    }
}

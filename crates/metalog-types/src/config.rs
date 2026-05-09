use serde::{Deserialize, Serialize};

/// Per-table configuration stored as JSON in `_table_config.config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableConfig {
    #[serde(default)]
    pub consolidation: ConsolidationConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
}

/// Consolidation subsystem configuration (premium feature).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsolidationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub policies: Vec<ConsolidationPolicyConfig>,
    #[serde(default)]
    pub stale_buffering_mins: i64,
}

/// A single consolidation policy configuration entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsolidationPolicyConfig {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Retention subsystem configuration (premium feature).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default, rename = "type")]
    pub type_name: String,

    /// Grace period in seconds added to `expires_at` before a file is
    /// considered expired. Allows slight overshoot without premature deletion.
    #[serde(default)]
    pub grace_period_secs: u64,

    /// How often the retention scanner runs, in seconds.
    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,

    /// Maximum storage object deletions per second.
    #[serde(default = "default_delete_rate")]
    pub delete_rate: u32,
}

fn default_scan_interval() -> u64 {
    60
}

fn default_delete_rate() -> u32 {
    500
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            type_name: String::new(),
            grace_period_secs: 0,
            scan_interval_secs: default_scan_interval(),
            delete_rate: default_delete_rate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_table_config() {
        let cfg = TableConfig::default();
        assert!(!cfg.consolidation.enabled);
        assert!(!cfg.retention.enabled);
        assert!(cfg.consolidation.policies.is_empty());
    }

    #[test]
    fn deserialize_json() {
        let json = r#"{
            "consolidation": {
                "enabled": true,
                "policies": [{"type": "time_window"}]
            },
            "retention": {
                "enabled": true,
                "type": "default",
                "grace_period_secs": 300,
                "scan_interval_secs": 120,
                "delete_rate": 100
            }
        }"#;
        let cfg: TableConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.consolidation.enabled);
        assert_eq!(cfg.consolidation.policies.len(), 1);
        assert_eq!(cfg.consolidation.policies[0].type_name, "time_window");
        assert!(cfg.retention.enabled);
        assert_eq!(cfg.retention.type_name, "default");
        assert_eq!(cfg.retention.grace_period_secs, 300);
        assert_eq!(cfg.retention.scan_interval_secs, 120);
        assert_eq!(cfg.retention.delete_rate, 100);
    }

    #[test]
    fn deserialize_empty_json() {
        let cfg: TableConfig = serde_json::from_str("{}").unwrap();
        assert!(!cfg.consolidation.enabled);
        assert!(!cfg.retention.enabled);
    }

    #[test]
    fn roundtrip_serde() {
        let cfg = TableConfig {
            consolidation: ConsolidationConfig {
                enabled: true,
                policies: vec![ConsolidationPolicyConfig {
                    type_name: "time_window".into(),
                    config: serde_json::json!({"window_size": "1h"}),
                }],
                stale_buffering_mins: 60,
            },
            retention: RetentionConfig {
                enabled: true,
                type_name: "default".into(),
                grace_period_secs: 300,
                scan_interval_secs: 120,
                delete_rate: 100,
            },
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: TableConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.consolidation.stale_buffering_mins, 60);
    }
}

use serde::{Deserialize, Serialize};

/// Per-table configuration stored as JSON in `_table_config.config`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// Immutable creation-time pools. Missing on legacy tables means dynamic allocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_reserves: Option<ColumnReserves>,
    #[serde(default)]
    pub consolidation: ConsolidationConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Logical dimension fields to index. Empty removes only Metalog-managed indexes.
    #[serde(default)]
    pub indexes: Vec<String>,
}

/// Typed nullable columns created before a table is published to coordinators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnReserves {
    #[serde(default = "default_dimension_reserves")]
    pub dimensions: Vec<DimensionReserve>,
    #[serde(default = "default_aggregate_reserves")]
    pub aggregates: Vec<AggregateReserve>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DimensionReserve {
    #[serde(rename = "type")]
    pub base_type: String,
    pub count: u32,
    /// Character capacity for strings; omitted means 256. Invalid for numeric types.
    #[serde(default)]
    pub width: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateReserve {
    /// int (BIGINT) or float (DOUBLE).
    #[serde(rename = "type")]
    pub value_type: String,
    pub count: u32,
}

fn default_dimension_reserves() -> Vec<DimensionReserve> {
    [
        ("str", 16, Some(512)),
        ("str_utf8", 8, Some(512)),
        ("int", 8, None),
        ("float", 4, None),
        ("bool", 4, None),
    ]
    .into_iter()
    .map(|(base_type, count, width)| DimensionReserve {
        base_type: base_type.into(),
        count,
        width,
    })
    .collect()
}

fn default_aggregate_reserves() -> Vec<AggregateReserve> {
    [("int", 16), ("float", 8)]
        .into_iter()
        .map(|(value_type, count)| AggregateReserve {
            value_type: value_type.into(),
            count,
        })
        .collect()
}

impl Default for ColumnReserves {
    fn default() -> Self {
        Self {
            dimensions: default_dimension_reserves(),
            aggregates: default_aggregate_reserves(),
        }
    }
}

impl ColumnReserves {
    pub fn validate(&self) -> Result<(), String> {
        use crate::column::{MAX_AGG_SLOTS, MAX_DIM_SLOTS, MIN_VARCHAR_WIDTH};
        let mut dims = 0u64;
        // Leave headroom for the base metadata columns within the SQL row-size limit.
        let mut declared_bytes = 0u64;
        for group in &self.dimensions {
            if group.count == 0 {
                return Err("dimension reserve counts must be positive".into());
            }
            dims += u64::from(group.count);
            let bytes =
                match group.base_type.as_str() {
                    "str" | "str_utf8" => {
                        let width = group.width.unwrap_or(MIN_VARCHAR_WIDTH as u16);
                        if !(MIN_VARCHAR_WIDTH as u16..=16383).contains(&width) {
                            return Err("reserved string widths must be between 256 and 16383 \
                                        characters"
                                .into());
                        }
                        u64::from(width) * if group.base_type == "str_utf8" { 4 } else { 1 } + 2
                    }
                    "int" | "float" | "bool" if group.width.is_none() => 8,
                    _ => {
                        return Err("dimension reserves require str, str_utf8, int, float or \
                                    bool; only strings accept width"
                            .into())
                    }
                };
            declared_bytes += bytes * u64::from(group.count);
        }
        let mut aggs = 0u64;
        for group in &self.aggregates {
            if group.count == 0 || !matches!(group.value_type.as_str(), "int" | "float") {
                return Err(
                    "aggregate reserves require type int or float and a positive count".into(),
                );
            }
            aggs += u64::from(group.count);
        }
        if dims > u64::from(MAX_DIM_SLOTS) || aggs > u64::from(MAX_AGG_SLOTS) {
            return Err(
                "column reserves allow at most 99 dimensions and 99 aggregates per table".into(),
            );
        }
        if declared_bytes + aggs * 8 > 48_000 {
            return Err(
                "column reserves exceed the 48000-byte declared capacity budget; reduce string \
                 widths or counts"
                    .into(),
            );
        }
        Ok(())
    }
}

impl TableConfig {
    pub fn validate_indexes(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        if self.indexes.len() > 32 {
            return Err("at most 32 dimension indexes may be configured".into());
        }
        for key in &self.indexes {
            if key.is_empty()
                || key.chars().count() > 1024
                || key.contains('\0')
                || !seen.insert(key)
            {
                return Err(
                    "index fields must be unique, nonempty logical names of at most 1024 \
                     characters without NUL"
                        .into(),
                );
            }
        }
        Ok(())
    }
}

/// Consolidation subsystem configuration (premium feature).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
        // Legacy stored configs must not acquire creation-time defaults on read.
        assert!(cfg.column_reserves.is_none());
    }

    #[test]
    fn reserve_defaults_and_partial_overrides() {
        let defaults = ColumnReserves::default();
        defaults.validate().unwrap();
        assert_eq!(defaults.dimensions.iter().map(|g| g.count).sum::<u32>(), 40);
        assert_eq!(defaults.aggregates.iter().map(|g| g.count).sum::<u32>(), 24);
        let override_config: ColumnReserves = serde_json::from_str(r#"{"dimensions":[]}"#).unwrap();
        assert!(override_config.dimensions.is_empty());
        assert_eq!(override_config.aggregates, defaults.aggregates);
        let opt_out: TableConfig = serde_json::from_str(r#"{"column_reserves":null}"#).unwrap();
        assert!(opt_out.column_reserves.is_none());
    }

    #[test]
    fn roundtrip_serde() {
        let cfg = TableConfig {
            column_reserves: None,
            indexes: vec![],
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

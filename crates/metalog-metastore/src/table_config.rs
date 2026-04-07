use metalog_types::config::TableConfig;

/// Returns the default table config (all features disabled, empty policies).
pub fn default_table_config() -> TableConfig {
    TableConfig::default()
}

/// Decodes a table config from a JSON blob (stored in `_table_config.config`).
/// Returns the default config if the blob is `None` or empty.
pub fn decode_table_config(blob: Option<&str>) -> Result<TableConfig, serde_json::Error> {
    match blob {
        Some(s) if !s.is_empty() => serde_json::from_str(s),
        _ => Ok(default_table_config()),
    }
}

/// Encodes a table config to a JSON string for storage.
pub fn encode_table_config(cfg: &TableConfig) -> Result<String, serde_json::Error> {
    serde_json::to_string(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_none() {
        let cfg = decode_table_config(None).unwrap();
        assert!(!cfg.consolidation.enabled);
        assert!(!cfg.retention.enabled);
    }

    #[test]
    fn decode_empty() {
        let cfg = decode_table_config(Some("")).unwrap();
        assert!(!cfg.consolidation.enabled);
    }

    #[test]
    fn decode_json() {
        let json = r#"{"consolidation":{"enabled":true}}"#;
        let cfg = decode_table_config(Some(json)).unwrap();
        assert!(cfg.consolidation.enabled);
    }

    #[test]
    fn roundtrip() {
        let original = TableConfig::default();
        let json = encode_table_config(&original).unwrap();
        let decoded = decode_table_config(Some(&json)).unwrap();
        assert_eq!(
            original.consolidation.enabled,
            decoded.consolidation.enabled
        );
        assert_eq!(original.retention.enabled, decoded.retention.enabled);
    }
}

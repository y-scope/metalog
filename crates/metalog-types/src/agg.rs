use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Premium: aggregation data attached to a [`FileRecord`](crate::FileRecord).
///
/// Populated by the `metalog-aggs` crate during proto conversion. The base
/// crate defines this struct so that `FileRecord` compiles without the premium
/// dependency; processing logic lives entirely in the premium crate.
#[derive(Debug, Clone, Default)]
pub struct AggData {
    /// Logical agg key -> value (resolved to physical `agg_fNN` at flush time).
    pub entries: HashMap<String, Value>,
    /// Schema metadata for each aggregation entry.
    pub meta: Vec<AggMeta>,
}

/// Metadata for a single aggregation field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggMeta {
    pub key: String,
    pub value: String,
    pub agg_type: String,
    pub value_type: String,
    pub alias_col: String,
}

/// Composite cache key for aggregation lookups: `"agg_type\0agg_key\0agg_value"`.
pub fn agg_cache_key(agg_key: &str, agg_value: &str, agg_type: &str) -> String {
    format!("{agg_type}\0{agg_key}\0{agg_value}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_cache_key_format() {
        let key = agg_cache_key("level", "error", "EQ");
        assert_eq!(key, "EQ\0level\0error");
    }

    #[test]
    fn agg_data_default() {
        let data = AggData::default();
        assert!(data.entries.is_empty());
        assert!(data.meta.is_empty());
    }
}

use std::collections::HashMap;

use serde_json::Value;

use crate::{agg::AggData, file_state::FileState, sketch::SketchData};

/// Metadata for a single dimension field attached to a file record.
#[derive(Debug, Clone)]
pub struct DimMeta {
    pub key: String,
    pub base_type: String,
    pub width: i32,
}

/// A single file metadata row in the metastore.
///
/// Maps 1:1 to a row in a per-table metadata table (cloned from `_clp_template`).
/// Dimensions, aggregations, and sketches are stored as logical key-value pairs
/// here and resolved to physical column names (`dim_fNN`, `agg_fNN`) at flush time.
#[derive(Debug, Clone)]
pub struct FileRecord {
    pub id: i64,
    pub state: FileState,
    pub min_timestamp: i64,
    pub max_timestamp: i64,

    // Storage locations (nullable in DB).
    pub file_storage_backend: Option<String>,
    pub file_bucket: Option<String>,
    pub file_path: Option<String>,
    pub file_size_bytes: i64,

    pub archive_storage_backend: Option<String>,
    pub archive_bucket: Option<String>,
    pub archive_path: Option<String>,
    pub archive_size_bytes: i64,
    pub archive_created_at: i64,

    // Metrics.
    pub raw_size_bytes: i64,
    pub record_count: i64,

    // Retention (fields exist in base; scanning/enforcement is premium).
    pub retention_days: i32,
    pub expires_at: i64,

    // Dimensions (base feature).
    pub dims: HashMap<String, Value>,
    pub dim_meta: Vec<DimMeta>,

    // --- Premium field slots (None in community edition) ---
    pub aggs: Option<AggData>,
    pub sketches: Option<SketchData>,
}

impl Default for FileRecord {
    fn default() -> Self {
        Self {
            id: 0,
            state: FileState::IrBuffering,
            min_timestamp: 0,
            max_timestamp: 0,
            file_storage_backend: None,
            file_bucket: None,
            file_path: None,
            file_size_bytes: 0,
            archive_storage_backend: None,
            archive_bucket: None,
            archive_path: None,
            archive_size_bytes: 0,
            archive_created_at: 0,
            raw_size_bytes: 0,
            record_count: 0,
            retention_days: DEFAULT_RETENTION_DAYS,
            expires_at: 0,
            dims: HashMap::new(),
            dim_meta: Vec::new(),
            aggs: None,
            sketches: None,
        }
    }
}

/// Default retention period for files without explicit retention.
pub const DEFAULT_RETENTION_DAYS: i32 = 30;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_record() {
        let rec = FileRecord::default();
        assert_eq!(rec.state, FileState::IrBuffering);
        assert_eq!(rec.retention_days, DEFAULT_RETENTION_DAYS);
        assert!(rec.dims.is_empty());
        assert!(rec.aggs.is_none());
        assert!(rec.sketches.is_none());
    }
}

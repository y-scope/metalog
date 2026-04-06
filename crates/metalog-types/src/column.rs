/// Physical column name prefix for dimension columns.
pub const DIM_COLUMN_PREFIX: &str = "dim_f";

/// Physical column name prefix for aggregation columns.
pub const AGG_COLUMN_PREFIX: &str = "agg_f";

/// Maximum number of dimension column slots per table.
pub const MAX_DIM_SLOTS: u32 = 99;

/// Maximum number of aggregation column slots per table.
pub const MAX_AGG_SLOTS: u32 = 99;

/// Maximum number of sketch slots per table (MySQL SET limit).
pub const MAX_SKETCH_SLOTS: u32 = 64;

/// Minimum VARCHAR width for dimension columns. Set to 256 to avoid the InnoDB
/// 1-byte to 2-byte length prefix transition at the 255/256 boundary.
pub const MIN_VARCHAR_WIDTH: i32 = 256;

/// Default VARCHAR width for new string dimension columns.
pub const DEFAULT_VARCHAR_WIDTH: i32 = 256;

// --- Well-known column names ---

pub const COL_ID: &str = "id";
pub const COL_MIN_TIMESTAMP: &str = "min_timestamp";
pub const COL_MAX_TIMESTAMP: &str = "max_timestamp";
pub const COL_STATE: &str = "state";
pub const COL_RECORD_COUNT: &str = "record_count";
pub const COL_RAW_SIZE_BYTES: &str = "raw_size_bytes";
pub const COL_RETENTION_DAYS: &str = "retention_days";
pub const COL_EXPIRES_AT: &str = "expires_at";

pub const COL_CLP_IR_STORAGE_BACKEND: &str = "clp_ir_storage_backend";
pub const COL_CLP_IR_BUCKET: &str = "clp_ir_bucket";
pub const COL_CLP_IR_PATH: &str = "clp_ir_path";
pub const COL_CLP_IR_PATH_HASH: &str = "clp_ir_path_hash";
pub const COL_CLP_IR_SIZE_BYTES: &str = "clp_ir_size_bytes";

pub const COL_CLP_ARCHIVE_STORAGE_BACKEND: &str = "clp_archive_storage_backend";
pub const COL_CLP_ARCHIVE_BUCKET: &str = "clp_archive_bucket";
pub const COL_CLP_ARCHIVE_PATH: &str = "clp_archive_path";
pub const COL_CLP_ARCHIVE_PATH_HASH: &str = "clp_archive_path_hash";
pub const COL_CLP_ARCHIVE_SIZE_BYTES: &str = "clp_archive_size_bytes";
pub const COL_CLP_ARCHIVE_CREATED_AT: &str = "clp_archive_created_at";

pub const COL_SKETCHES: &str = "sketches";
pub const COL_EXT: &str = "ext";

// --- System table names ---

pub const TABLE_REGISTRY: &str = "_table";
pub const TABLE_REGISTRY_CONFIG: &str = "_table_config";
pub const TABLE_REGISTRY_ASSIGNMENT: &str = "_table_assignment";
pub const NODE_REGISTRY_TABLE: &str = "_node_registry";
pub const DIM_REGISTRY_TABLE: &str = "_dim_registry";
pub const AGG_REGISTRY_TABLE: &str = "_agg_registry";
pub const SKETCH_REGISTRY_TABLE: &str = "_sketch_registry";
pub const TEMPLATE_TABLE: &str = "_clp_template";
pub const KAFKA_SOURCE_TABLE: &str = "_kafka_source";
pub const KAFKA_ASSIGNMENT_TABLE: &str = "_kafka_assignment";
pub const TASK_QUEUE_TABLE: &str = "_task_queue";

/// Maps a logical dimension/aggregation key and its physical column name.
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    pub physical_col: String,
    pub logical_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dim_prefix() {
        assert!(DIM_COLUMN_PREFIX.starts_with("dim_"));
    }

    #[test]
    fn min_varchar_width_avoids_innodb_transition() {
        const { assert!(MIN_VARCHAR_WIDTH >= 256) };
    }
}

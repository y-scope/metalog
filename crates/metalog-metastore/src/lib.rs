mod advisory_lock;
mod env_match;
mod file_records;
mod metadata_reader;
mod table_config;
mod upsert_builder;
mod validate;

pub use advisory_lock::{AdvisoryLock, AdvisoryLockError};
pub use env_match::{matches_env, parse_required_env, validate_required_env};
pub use file_records::{DeletionResult, FileRecords, PendingFile, StoragePath};
pub use metadata_reader::{AggInfo, DimensionInfo, MetadataReader, SketchInfo};
// Re-export core types used throughout metastore consumers.
pub use metalog_types::{
    agg::{AggData, AggMeta},
    column::*,
    config::{ConsolidationConfig, ConsolidationPolicyConfig, RetentionConfig, TableConfig},
    file_record::{DimMeta, FileRecord, DEFAULT_RETENTION_DAYS},
    file_state::FileState,
    sketch::SketchData,
};
pub use table_config::{decode_table_config, default_table_config, encode_table_config};
pub use upsert_builder::{
    build_guard_expression,
    build_guarded_update,
    BASE_COLUMNS,
    GUARDED_UPDATE_COLUMNS,
    MAX_MULTI_ROW_INSERT,
};
pub use validate::{validate_file_record, ValidationError};

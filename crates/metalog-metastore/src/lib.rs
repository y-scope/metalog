mod env_match;
mod table_config;
mod validate;

pub use env_match::{matches_env, parse_required_env, validate_required_env};
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
pub use validate::validate_file_record;

use metalog_types::FileRecord;

/// Validates a [`FileRecord`] for ingestion.
///
/// Checks:
/// - `min_timestamp` is non-negative
/// - `max_timestamp >= min_timestamp`
/// - At least one storage path is set (IR or archive)
pub fn validate_file_record(rec: &FileRecord) -> Result<(), ValidationError> {
    if rec
        .aggs
        .as_ref()
        .is_some_and(|a| !a.entries.is_empty() || !a.meta.is_empty())
        || rec.sketches.as_ref().is_some_and(|s| {
            !s.sketches.is_empty() || s.set_value.is_some() || s.ext_blob.is_some()
        })
    {
        return Err(ValidationError(
            "aggregation and sketch ingestion are not supported".into(),
        ));
    }

    if rec.min_timestamp < 0 {
        return Err(ValidationError("min_timestamp must be non-negative".into()));
    }
    if rec.max_timestamp < rec.min_timestamp {
        return Err(ValidationError(
            "max_timestamp must be >= min_timestamp".into(),
        ));
    }
    let has_ir = rec.file_path.as_ref().is_some_and(|p| !p.is_empty());
    let has_archive = rec.archive_path.as_ref().is_some_and(|p| !p.is_empty());
    if !has_ir && !has_archive {
        return Err(ValidationError(
            "at least one of file_path or archive_path must be set".into(),
        ));
    }
    for (kind, backend, bucket, path) in [
        (
            "file",
            &rec.file_storage_backend,
            &rec.file_bucket,
            &rec.file_path,
        ),
        (
            "archive",
            &rec.archive_storage_backend,
            &rec.archive_bucket,
            &rec.archive_path,
        ),
    ] {
        if path.as_ref().is_some_and(|path| !path.is_empty())
            && (backend.as_ref().is_none_or(|value| value.is_empty())
                || bucket.as_ref().is_none_or(|value| value.is_empty()))
        {
            return Err(ValidationError(format!(
                "{kind} path requires an explicit storage backend and bucket"
            )));
        }
    }
    for (name, value) in [
        ("file_size_bytes", rec.file_size_bytes),
        ("archive_size_bytes", rec.archive_size_bytes),
        ("record_count", rec.record_count),
    ] {
        if !(0..=u32::MAX as i64).contains(&value) {
            return Err(ValidationError(format!(
                "{name} must fit an unsigned 32-bit integer"
            )));
        }
    }
    if rec.raw_size_bytes < 0 || rec.archive_created_at < 0 || rec.expires_at < 0 {
        return Err(ValidationError(
            "sizes and timestamps must be non-negative".into(),
        ));
    }
    if !(0..=u16::MAX as i32).contains(&rec.retention_days) {
        return Err(ValidationError(
            "retention_days must fit an unsigned 16-bit integer".into(),
        ));
    }
    for (name, value, max_chars, ascii) in [
        ("file_storage_backend", &rec.file_storage_backend, 32, true),
        (
            "archive_storage_backend",
            &rec.archive_storage_backend,
            32,
            true,
        ),
        ("file_bucket", &rec.file_bucket, 63, true),
        ("archive_bucket", &rec.archive_bucket, 63, true),
        ("file_path", &rec.file_path, 1024, false),
        ("archive_path", &rec.archive_path, 1024, false),
    ] {
        if let Some(value) = value {
            if value.chars().count() > max_chars || (ascii && !value.is_ascii()) {
                return Err(ValidationError(format!(
                    "invalid {name}: exceeds storage width or character set"
                )));
            }
        }
    }
    let mut keys = std::collections::HashSet::new();
    for meta in &rec.dim_meta {
        if meta.key.is_empty() || !keys.insert(meta.key.as_str()) {
            return Err(ValidationError(
                "dimension keys must be nonempty and unique".into(),
            ));
        }
        let value = rec
            .dims
            .get(&meta.key)
            .ok_or_else(|| ValidationError(format!("missing value for dimension {}", meta.key)))?;
        let valid = match meta.base_type.as_str() {
            "str" | "str_utf8" => value.as_str().is_some_and(|s| {
                let width = meta.width.max(metalog_types::column::MIN_VARCHAR_WIDTH);
                meta.width >= 0
                    && width <= u16::MAX as i32
                    && s.chars().count() <= width as usize
                    && (meta.base_type != "str" || s.is_ascii())
            }),
            "int" => value.as_i64().is_some(),
            "float" => value.as_f64().is_some_and(f64::is_finite),
            "bool" => value.is_boolean(),
            _ => false,
        };
        if !value.is_null() && !valid {
            return Err(ValidationError(format!(
                "invalid value or type for dimension {}",
                meta.key
            )));
        }
        if !["str", "str_utf8", "int", "float", "bool"].contains(&meta.base_type.as_str()) {
            return Err(ValidationError(format!(
                "unsupported dimension type {}",
                meta.base_type
            )));
        }
    }
    if keys.len() != rec.dims.len() {
        return Err(ValidationError(
            "every dimension value must have dimension metadata".into(),
        ));
    }
    Ok(())
}

/// Validation error for file record ingestion.
#[derive(Debug, thiserror::Error)]
#[error("validation: {0}")]
pub struct ValidationError(pub String);

#[cfg(test)]
mod tests {
    use metalog_types::file_state::FileState;

    use super::*;

    fn valid_record() -> FileRecord {
        FileRecord {
            min_timestamp: 1000,
            max_timestamp: 2000,
            file_path: Some("/data/test.ir".into()),
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
            state: FileState::IrBuffering,
            ..FileRecord::default()
        }
    }

    #[test]
    fn valid() {
        assert!(validate_file_record(&valid_record()).is_ok());
    }

    #[test]
    fn negative_min_timestamp() {
        let mut rec = valid_record();
        rec.min_timestamp = -1;
        assert!(validate_file_record(&rec).is_err());
    }

    #[test]
    fn max_less_than_min() {
        let mut rec = valid_record();
        rec.max_timestamp = 500;
        assert!(validate_file_record(&rec).is_err());
    }

    #[test]
    fn no_paths() {
        let mut rec = valid_record();
        rec.file_path = None;
        rec.archive_path = None;
        assert!(validate_file_record(&rec).is_err());
    }

    #[test]
    fn archive_only() {
        let mut rec = valid_record();
        rec.file_path = None;
        rec.archive_path = Some("/data/test.archive".into());
        rec.archive_storage_backend = Some("s3".into());
        rec.archive_bucket = Some("test".into());
        assert!(validate_file_record(&rec).is_ok());
    }

    #[test]
    fn empty_paths_treated_as_missing() {
        let mut rec = valid_record();
        rec.file_path = Some(String::new());
        rec.archive_path = Some(String::new());
        assert!(validate_file_record(&rec).is_err());
    }
    #[test]
    fn rejects_unsigned_overflow_before_it_can_poison_a_batch() {
        let mut rec = valid_record();
        rec.file_size_bytes = -1;
        assert!(validate_file_record(&rec).is_err());
        rec.file_size_bytes = u32::MAX as i64 + 1;
        assert!(validate_file_record(&rec).is_err());
        rec.file_size_bytes = 0;
        rec.record_count = -1;
        assert!(validate_file_record(&rec).is_err());
        rec.record_count = 0;
        rec.retention_days = u16::MAX as i32 + 1;
        assert!(validate_file_record(&rec).is_err());
    }

    #[test]
    fn rejects_dimensions_without_matching_schema_metadata() {
        let mut rec = valid_record();
        rec.dims.insert("host".into(), serde_json::json!("web"));
        assert!(validate_file_record(&rec).is_err());
        rec.dim_meta.push(metalog_types::file_record::DimMeta {
            key: "host".into(),
            base_type: "int".into(),
            width: 0,
        });
        assert!(validate_file_record(&rec).is_err());
    }
    #[test]
    fn storage_identity_requires_explicit_backend_and_bucket() {
        let mut rec = valid_record();
        rec.file_bucket = None;
        assert!(validate_file_record(&rec).is_err());
        rec.file_bucket = Some("test".into());
        rec.file_storage_backend = Some(String::new());
        assert!(validate_file_record(&rec).is_err());
    }
}

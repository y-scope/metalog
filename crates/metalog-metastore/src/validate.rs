use metalog_types::FileRecord;

/// Validates a [`FileRecord`] for ingestion.
///
/// Checks:
/// - `min_timestamp` is non-negative
/// - `max_timestamp >= min_timestamp`
/// - At least one storage path is set (IR or archive)
pub fn validate_file_record(rec: &FileRecord) -> Result<(), ValidationError> {
    if rec.min_timestamp < 0 {
        return Err(ValidationError("min_timestamp must be non-negative".into()));
    }
    if rec.max_timestamp < rec.min_timestamp {
        return Err(ValidationError(
            "max_timestamp must be >= min_timestamp".into(),
        ));
    }
    let has_ir = rec.clp_ir_path.as_ref().is_some_and(|p| !p.is_empty());
    let has_archive = rec.clp_archive_path.as_ref().is_some_and(|p| !p.is_empty());
    if !has_ir && !has_archive {
        return Err(ValidationError(
            "at least one of clp_ir_path or clp_archive_path must be set".into(),
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
            clp_ir_path: Some("/data/test.ir".into()),
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
        rec.clp_ir_path = None;
        rec.clp_archive_path = None;
        assert!(validate_file_record(&rec).is_err());
    }

    #[test]
    fn archive_only() {
        let mut rec = valid_record();
        rec.clp_ir_path = None;
        rec.clp_archive_path = Some("/data/test.archive".into());
        assert!(validate_file_record(&rec).is_ok());
    }

    #[test]
    fn empty_paths_treated_as_missing() {
        let mut rec = valid_record();
        rec.clp_ir_path = Some(String::new());
        rec.clp_archive_path = Some(String::new());
        assert!(validate_file_record(&rec).is_err());
    }
}

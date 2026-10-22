use metalog_proto::coordinator::{dimension_value, DimEntry, FileFields, MetadataRecord};
use metalog_types::{file_record::DimMeta, file_state::FileState, FileRecord};

/// Converts a protobuf [`MetadataRecord`] to an internal [`FileRecord`].
///
/// Extracts file metadata and dimensions. Unsupported extension fields are
/// rejected explicitly so callers never receive success for discarded metadata.
pub fn convert_record(record: &MetadataRecord) -> Result<FileRecord, ConvertError> {
    if !record.agg.is_empty() || !record.sketch.is_empty() || !record.self_describing_kv.is_empty()
    {
        return Err(ConvertError(
            "aggregation, sketch, and self-describing ingestion are not supported".into(),
        ));
    }
    let file = record
        .file
        .as_ref()
        .ok_or_else(|| ConvertError("missing file fields".into()))?;

    let mut rec = file_record_from_proto(file)?;

    // Dimensions (base feature).
    extract_dims(&record.dim, &mut rec)?;

    Ok(rec)
}

fn file_record_from_proto(file: &FileFields) -> Result<FileRecord, ConvertError> {
    let state = FileState::from_db_str(&file.state)
        .ok_or_else(|| ConvertError(format!("invalid state: {}", file.state)))?;

    let mut rec = FileRecord {
        state,
        min_timestamp: file.min_timestamp,
        max_timestamp: file.max_timestamp,
        raw_size_bytes: file.raw_size_bytes,
        record_count: file.record_count as i64,
        retention_days: file.retention_days,
        expires_at: file.expires_at,
        ..FileRecord::default()
    };

    if let Some(file_info) = &file.file {
        rec.file_storage_backend = Some(file_info.file_storage_backend.clone());
        rec.file_bucket = Some(file_info.file_bucket.clone());
        rec.file_path = Some(file_info.file_path.clone());
        rec.file_size_bytes = file_info.file_size_bytes;
    }

    if let Some(archive) = &file.archive {
        rec.archive_storage_backend = Some(archive.archive_storage_backend.clone());
        rec.archive_bucket = Some(archive.archive_bucket.clone());
        rec.archive_path = Some(archive.archive_path.clone());
        rec.archive_size_bytes = archive.archive_size_bytes;
        rec.archive_created_at = archive.archive_created_at;
    }

    Ok(rec)
}

fn extract_dims(dims: &[DimEntry], rec: &mut FileRecord) -> Result<(), ConvertError> {
    for dim in dims {
        let dv = dim
            .value
            .as_ref()
            .ok_or_else(|| ConvertError(format!("missing value for dimension {}", dim.key)))?;

        let (json_val, base_type, width) = match &dv.value {
            Some(dimension_value::Value::Str(s)) => (
                serde_json::Value::String(s.value.clone()),
                "str",
                s.max_length,
            ),
            Some(dimension_value::Value::StrUtf8(s)) => (
                serde_json::Value::String(s.value.clone()),
                "str_utf8",
                s.max_length,
            ),
            Some(dimension_value::Value::IntVal(v)) => {
                (serde_json::Value::Number((*v).into()), "int", 0)
            }
            Some(dimension_value::Value::BoolVal(v)) => (serde_json::Value::Bool(*v), "bool", 0),
            Some(dimension_value::Value::FloatVal(v)) => {
                if !v.is_finite() {
                    return Err(ConvertError(format!("non-finite dimension {}", dim.key)));
                }
                (serde_json::json!(*v), "float", 0)
            }
            None => {
                return Err(ConvertError(format!(
                    "missing value for dimension {}",
                    dim.key
                )))
            }
        };

        rec.dims.insert(dim.key.clone(), json_val);
        rec.dim_meta.push(DimMeta {
            key: dim.key.clone(),
            base_type: base_type.to_string(),
            width,
        });
    }
    Ok(())
}

/// Error during proto-to-domain conversion.
#[derive(Debug, thiserror::Error)]
#[error("convert: {0}")]
pub struct ConvertError(pub String);

#[cfg(test)]
mod tests {
    use metalog_proto::coordinator::{ArchiveInfo, DimensionValue, FileInfo, StringDimension};

    use super::*;

    fn test_proto_record() -> MetadataRecord {
        MetadataRecord {
            file: Some(FileFields {
                state: "IR_BUFFERING".into(),
                min_timestamp: 1000,
                max_timestamp: 2000,
                raw_size_bytes: 500,
                record_count: 10,
                retention_days: 30,
                expires_at: 0,
                file: Some(FileInfo {
                    file_storage_backend: "s3".into(),
                    file_bucket: "logs".into(),
                    file_path: "/data/test.ir".into(),
                    file_size_bytes: 100,
                }),
                archive: None,
            }),
            dim: vec![DimEntry {
                key: "hostname".into(),
                value: Some(DimensionValue {
                    value: Some(dimension_value::Value::Str(StringDimension {
                        value: "web-01".into(),
                        max_length: 256,
                    })),
                }),
            }],
            agg: vec![],
            self_describing_kv: vec![],
            sketch: vec![],
        }
    }

    #[test]
    fn convert_basic() {
        let rec = convert_record(&test_proto_record()).unwrap();
        assert_eq!(rec.state, FileState::IrBuffering);
        assert_eq!(rec.min_timestamp, 1000);
        assert_eq!(rec.max_timestamp, 2000);
        assert_eq!(rec.file_path, Some("/data/test.ir".into()));
        assert_eq!(rec.dims.len(), 1);
        assert_eq!(rec.dims["hostname"], "web-01");
        assert_eq!(rec.dim_meta.len(), 1);
        assert_eq!(rec.dim_meta[0].base_type, "str");
    }

    #[test]
    fn convert_missing_file_fields() {
        let rec = MetadataRecord::default();
        assert!(convert_record(&rec).is_err());
    }

    #[test]
    fn convert_invalid_state() {
        let mut proto = test_proto_record();
        proto.file.as_mut().unwrap().state = "INVALID".into();
        assert!(convert_record(&proto).is_err());
    }

    #[test]
    fn convert_with_archive() {
        let mut proto = test_proto_record();
        proto.file.as_mut().unwrap().archive = Some(ArchiveInfo {
            archive_storage_backend: "s3".into(),
            archive_bucket: "archives".into(),
            archive_path: "/archives/test.clp".into(),
            archive_size_bytes: 50,
            archive_created_at: 3000,
        });
        let rec = convert_record(&proto).unwrap();
        assert_eq!(rec.archive_path, Some("/archives/test.clp".into()));
        assert_eq!(rec.archive_created_at, 3000);
    }

    #[test]
    fn convert_no_premium() {
        let rec = convert_record(&test_proto_record()).unwrap();
        assert!(rec.aggs.is_none());
        assert!(rec.sketches.is_none());
    }
    #[test]
    fn unsupported_extensions_are_rejected_instead_of_discarded() {
        let mut rec = test_proto_record();
        rec.agg.push(Default::default());
        assert!(convert_record(&rec)
            .unwrap_err()
            .to_string()
            .contains("not supported"));
        rec.agg.clear();
        rec.sketch.push(Default::default());
        assert!(convert_record(&rec).is_err());
        rec.sketch.clear();
        rec.self_describing_kv.push(Default::default());
        assert!(convert_record(&rec).is_err());
    }

    #[test]
    fn malformed_dimension_values_are_rejected() {
        let mut rec = test_proto_record();
        rec.dim[0].value = None;
        assert!(convert_record(&rec).is_err());
        rec.dim[0].value = Some(DimensionValue {
            value: Some(dimension_value::Value::FloatVal(f64::NAN)),
        });
        assert!(convert_record(&rec).is_err());
    }
}

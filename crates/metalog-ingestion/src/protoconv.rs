use metalog_proto::coordinator::{
    dimension_value,
    ingest_agg_entry,
    DimEntry,
    FileFields,
    MetadataRecord,
};
use metalog_types::{
    agg::{AggData, AggMeta},
    file_record::DimMeta,
    file_state::FileState,
    sketch::SketchData,
    FileRecord,
};

/// Converts a protobuf [`MetadataRecord`] to an internal [`FileRecord`].
///
/// Extracts file metadata, dimensions, and (if present) aggregation and sketch
/// data into the premium extension slots.
pub fn convert_record(record: &MetadataRecord) -> Result<FileRecord, ConvertError> {
    let file = record
        .file
        .as_ref()
        .ok_or_else(|| ConvertError("missing file fields".into()))?;

    let mut rec = file_record_from_proto(file)?;

    // Dimensions (base feature).
    extract_dims(&record.dim, &mut rec);

    // Premium: aggregations (populated into Option slot).
    if !record.agg.is_empty() {
        let mut agg_data = AggData::default();
        for agg in &record.agg {
            let agg_type_str = agg.agg_type().as_str_name();
            let key = format!("{}\0{}\0{}", agg_type_str, agg.field, agg.qualifier);
            let value = match &agg.value {
                Some(ingest_agg_entry::Value::IntVal(v)) => serde_json::Value::Number((*v).into()),
                Some(ingest_agg_entry::Value::FloatVal(v)) => serde_json::json!(*v),
                None => serde_json::Value::Null,
            };
            let is_float = matches!(&agg.value, Some(ingest_agg_entry::Value::FloatVal(_)));
            agg_data.entries.insert(key, value);
            agg_data.meta.push(AggMeta {
                key: agg.field.clone(),
                value: agg.qualifier.clone(),
                agg_type: agg_type_str.to_string(),
                value_type: if is_float { "FLOAT" } else { "INT" }.to_string(),
                alias_col: agg.alias_column.clone(),
            });
        }
        rec.aggs = Some(agg_data);
    }

    // Premium: sketches (populated into Option slot).
    if !record.sketch.is_empty() {
        let mut sketch_data = SketchData::default();
        for sketch in &record.sketch {
            sketch_data
                .sketches
                .insert(sketch.sketch_key.clone(), sketch.data.clone());
        }
        rec.sketches = Some(sketch_data);
    }

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

    if let Some(ir) = &file.ir {
        rec.clp_ir_storage_backend = Some(ir.clp_ir_storage_backend.clone());
        rec.clp_ir_bucket = Some(ir.clp_ir_bucket.clone());
        rec.clp_ir_path = Some(ir.clp_ir_path.clone());
        rec.clp_ir_size_bytes = ir.clp_ir_size_bytes;
    }

    if let Some(archive) = &file.archive {
        rec.clp_archive_storage_backend = Some(archive.clp_archive_storage_backend.clone());
        rec.clp_archive_bucket = Some(archive.clp_archive_bucket.clone());
        rec.clp_archive_path = Some(archive.clp_archive_path.clone());
        rec.clp_archive_size_bytes = archive.clp_archive_size_bytes;
        rec.clp_archive_created_at = archive.clp_archive_created_at;
    }

    Ok(rec)
}

fn extract_dims(dims: &[DimEntry], rec: &mut FileRecord) {
    for dim in dims {
        let Some(ref dv) = dim.value else {
            continue;
        };

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
            Some(dimension_value::Value::FloatVal(v)) => (serde_json::json!(*v), "float", 0),
            None => continue,
        };

        rec.dims.insert(dim.key.clone(), json_val);
        rec.dim_meta.push(DimMeta {
            key: dim.key.clone(),
            base_type: base_type.to_string(),
            width,
        });
    }
}

/// Error during proto-to-domain conversion.
#[derive(Debug, thiserror::Error)]
#[error("convert: {0}")]
pub struct ConvertError(pub String);

#[cfg(test)]
mod tests {
    use metalog_proto::coordinator::{
        ArchiveFileInfo,
        DimensionValue,
        IrFileInfo,
        StringDimension,
    };

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
                ir: Some(IrFileInfo {
                    clp_ir_storage_backend: "s3".into(),
                    clp_ir_bucket: "logs".into(),
                    clp_ir_path: "/data/test.ir".into(),
                    clp_ir_size_bytes: 100,
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
        assert_eq!(rec.clp_ir_path, Some("/data/test.ir".into()));
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
        proto.file.as_mut().unwrap().archive = Some(ArchiveFileInfo {
            clp_archive_storage_backend: "s3".into(),
            clp_archive_bucket: "archives".into(),
            clp_archive_path: "/archives/test.clp".into(),
            clp_archive_size_bytes: 50,
            clp_archive_created_at: 3000,
        });
        let rec = convert_record(&proto).unwrap();
        assert_eq!(rec.clp_archive_path, Some("/archives/test.clp".into()));
        assert_eq!(rec.clp_archive_created_at, 3000);
    }

    #[test]
    fn convert_no_premium() {
        let rec = convert_record(&test_proto_record()).unwrap();
        assert!(rec.aggs.is_none());
        assert!(rec.sketches.is_none());
    }
}

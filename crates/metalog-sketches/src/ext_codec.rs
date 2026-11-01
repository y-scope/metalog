use std::collections::HashMap;

use metalog_encoding::{marshal_frame, unmarshal_frame};
use serde::{Deserialize, Serialize};

pub const SBBF_TYPE: &str = "parquet_sbbf_xxhash64";
/// Bound queued records and SQL statements, independently of gRPC's message limit.
pub const MAX_SKETCH_BYTES: usize = 1 << 20;
pub const MAX_EXT_BYTES: usize = 2 << 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SketchPayload {
    pub sketches: HashMap<String, Snapshot>,
}

pub fn decode_snapshots(sketches: &HashMap<String, Vec<u8>>) -> Result<SketchPayload, String> {
    if sketches.len() > 64 || sketches.values().map(Vec::len).sum::<usize>() > MAX_SKETCH_BYTES {
        return Err("sketches exceed 64 fields or 1 MiB per record".into());
    }
    let mut decoded = HashMap::new();
    for (key, bytes) in sketches {
        if key.is_empty() || key.chars().count() > 1024 || key.contains('\0') {
            return Err("sketch field names must contain 1..1024 characters without NUL".into());
        }
        let snapshot: Snapshot =
            rmp_serde::from_slice(bytes).map_err(|e| format!("invalid sketch {key}: {e}"))?;
        if snapshot.type_name != SBBF_TYPE
            || snapshot.data.is_empty()
            || !snapshot.data.len().is_multiple_of(32)
        {
            return Err(format!(
                "sketch {key} requires {SBBF_TYPE} with nonempty 32-byte blocks"
            ));
        }
        decoded.insert(key.clone(), snapshot);
    }
    Ok(SketchPayload { sketches: decoded })
}

/// Producer snapshots are nested unchanged under sketches.<logical field>.
pub fn encode_ext_blob(sketches: &HashMap<String, Vec<u8>>) -> Result<Vec<u8>, String> {
    marshal_frame(&decode_snapshots(sketches)?).map_err(|e| e.to_string())
}

pub fn decode_ext_blob(data: &[u8]) -> Result<SketchPayload, metalog_encoding::CodecError> {
    unmarshal_frame(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{may_match, parse_expression, sbbf_contains};
    const WIRE: &[u8] = include_bytes!("../../../tests/fixtures/bloom/snapshot.msgpack");
    const EXT: &[u8] = include_bytes!("../../../tests/fixtures/bloom/ext.lz4");
    const VALUES: &[&str] = &["", "abc", "user-12345", "a'b", "é🙂", "nul\0value"];

    #[test]
    fn independently_generated_go_filters_have_no_false_negatives() {
        for data in [
            &include_bytes!("../../../tests/fixtures/bloom/parquet_1.bin")[..],
            &include_bytes!("../../../tests/fixtures/bloom/parquet_3.bin")[..],
            &include_bytes!("../../../tests/fixtures/bloom/parquet_16.bin")[..],
            &include_bytes!("../../../tests/fixtures/bloom/sbbf_1.bin")[..],
            &include_bytes!("../../../tests/fixtures/bloom/sbbf_3.bin")[..],
            &include_bytes!("../../../tests/fixtures/bloom/sbbf_16.bin")[..],
        ] {
            for value in VALUES {
                assert!(
                    sbbf_contains(data, value.as_bytes()),
                    "{value:?} in {} blocks",
                    data.len() / 32
                );
            }
            assert!(!sbbf_contains(data, b"definitely-absent"));
        }
    }

    #[test]
    fn go_snapshot_and_lz4_frame_are_compatible_and_conservative() {
        let original = decode_ext_blob(EXT).unwrap();
        assert_eq!(original.sketches.len(), 2);
        let input = HashMap::from([("trace_id".into(), WIRE.to_vec())]);
        let rust = encode_ext_blob(&input).unwrap();
        assert_eq!(&rust[..4], &[4, 34, 77, 24]); // standard LZ4 frame magic
        assert_eq!(
            decode_ext_blob(&rust).unwrap().sketches["trace_id"].data,
            original.sketches["trace_id"].data
        );
        for ext in [EXT, &rust] {
            for value in VALUES {
                let predicates = vec![crate::SketchPredicate {
                    key: "trace_id".into(),
                    values: vec![(*value).into()],
                }];
                assert!(may_match("IR_CLOSED", Some(ext), &predicates));
            }
            let p = parse_expression("trace_id = 'definitely-absent'").unwrap();
            assert!(!may_match("IR_CLOSED", Some(ext), &p));
            assert!(may_match("IR_BUFFERING", Some(ext), &p));
            assert!(may_match("IR_ARCHIVE_BUFFERING", Some(ext), &p));
            assert!(may_match(
                "IR_CLOSED",
                Some(ext),
                &parse_expression("missing = 'x'").unwrap()
            ));
            assert!(may_match(
                "IR_CLOSED",
                Some(ext),
                &parse_expression("trace_id IN ('definitely-absent', 'abc')").unwrap()
            ));
        }
        let mut future = original;
        future.sketches.get_mut("trace_id").unwrap().type_name = "future_format".into();
        let p = parse_expression("trace_id = 'definitely-absent'").unwrap();
        assert!(may_match(
            "IR_CLOSED",
            Some(&marshal_frame(&future).unwrap()),
            &p
        ));
        future.sketches.get_mut("trace_id").unwrap().type_name = SBBF_TYPE.into();
        for size in [0, 1, 31, 33, 63] {
            future.sketches.get_mut("trace_id").unwrap().data = vec![0; size];
            assert!(may_match(
                "IR_CLOSED",
                Some(&marshal_frame(&future).unwrap()),
                &p
            ));
        }
    }

    #[test]
    fn reject_malformed_and_oversized_ingestion() {
        for data in [vec![], vec![0; 31], vec![0; 33]] {
            let wire = rmp_serde::to_vec_named(&Snapshot {
                type_name: SBBF_TYPE.into(),
                data,
            })
            .unwrap();
            assert!(decode_snapshots(&HashMap::from([("x".into(), wire)])).is_err());
        }
        assert!(decode_snapshots(&HashMap::from([(
            "x".into(),
            vec![0; MAX_SKETCH_BYTES + 1]
        )]))
        .is_err());
        let bomb = metalog_encoding::marshal_frame(&vec![
            0u8;
            metalog_encoding::MAX_DECOMPRESSED_SIZE
                + 1
        ]);
        assert!(bomb.is_err());
    }
}

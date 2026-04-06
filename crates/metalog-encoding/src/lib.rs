use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum decompressed size (16 MB). Prevents unbounded memory allocation from
/// crafted LZ4 streams.
pub const MAX_DECOMPRESSED_SIZE: usize = 16 << 20;

#[derive(Debug, Error)]
pub enum CodecError {
    #[error("msgpack encode error: {0}")]
    Encode(#[from] rmp_serde::encode::Error),

    #[error("msgpack decode error: {0}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("lz4 decompress error: {0}")]
    Decompress(String),

    #[error("decompressed size {0} exceeds limit {MAX_DECOMPRESSED_SIZE}")]
    DecompressedTooLarge(usize),
}

/// Serializes `value` to msgpack, then LZ4-compresses the result.
pub fn marshal<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let msgpack = rmp_serde::to_vec(value)?;
    let compressed = lz4_flex::compress_prepend_size(&msgpack);
    Ok(compressed)
}

/// LZ4-decompresses `data`, then deserializes from msgpack.
pub fn unmarshal<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T, CodecError> {
    let decompressed = lz4_flex::decompress_size_prepended(data)
        .map_err(|e| CodecError::Decompress(e.to_string()))?;
    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
        return Err(CodecError::DecompressedTooLarge(decompressed.len()));
    }
    let value = rmp_serde::from_slice(&decompressed)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestPayload {
        name: String,
        values: Vec<i64>,
    }

    #[test]
    fn roundtrip() {
        let payload = TestPayload {
            name: "test".into(),
            values: vec![1, 2, 3],
        };
        let encoded = marshal(&payload).unwrap();
        let decoded: TestPayload = unmarshal(&encoded).unwrap();
        assert_eq!(payload, decoded);
    }

    #[test]
    fn compressed_smaller_than_raw() {
        let payload = TestPayload {
            name: "x".repeat(1000),
            values: (0..100).collect(),
        };
        let raw = rmp_serde::to_vec(&payload).unwrap();
        let compressed = marshal(&payload).unwrap();
        assert!(compressed.len() < raw.len());
    }

    #[test]
    fn empty_struct() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Empty {}
        let encoded = marshal(&Empty {}).unwrap();
        let decoded: Empty = unmarshal(&encoded).unwrap();
        assert_eq!(Empty {}, decoded);
    }

    #[test]
    fn invalid_data() {
        let result: Result<TestPayload, _> = unmarshal(&[0xff, 0xfe, 0xfd]);
        assert!(result.is_err());
    }
}

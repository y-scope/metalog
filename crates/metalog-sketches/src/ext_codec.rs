use std::collections::HashMap;

use metalog_encoding::{marshal, unmarshal};

/// Encodes sketch data as LZ4-compressed msgpack for the `ext` MEDIUMBLOB column.
///
/// Input: map of sketch_key → raw SBBF data.
/// Output: compressed blob ready for DB storage.
pub fn encode_ext_blob(
    sketches: &HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>, metalog_encoding::CodecError> {
    marshal(sketches)
}

/// Decodes an ext blob back to sketch data.
pub fn decode_ext_blob(
    data: &[u8],
) -> Result<HashMap<String, Vec<u8>>, metalog_encoding::CodecError> {
    unmarshal(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut sketches = HashMap::new();
        sketches.insert("uuid".to_string(), vec![1, 2, 3, 4]);
        sketches.insert("trace_id".to_string(), vec![5, 6, 7, 8]);

        let encoded = encode_ext_blob(&sketches).unwrap();
        let decoded = decode_ext_blob(&encoded).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded["uuid"], vec![1, 2, 3, 4]);
        assert_eq!(decoded["trace_id"], vec![5, 6, 7, 8]);
    }

    #[test]
    fn empty_map() {
        let sketches = HashMap::new();
        let encoded = encode_ext_blob(&sketches).unwrap();
        let decoded: HashMap<String, Vec<u8>> = decode_ext_blob(&encoded).unwrap();
        assert!(decoded.is_empty());
    }
}

use std::collections::HashMap;

/// Premium: sketch (bloom filter) data attached to a [`FileRecord`](crate::FileRecord).
///
/// Populated by the `metalog-sketches` crate during proto conversion. The base
/// crate defines this struct so that `FileRecord` compiles without the premium
/// dependency; processing logic (SBBF evaluation, ext encoding) lives entirely
/// in the premium crate.
#[derive(Debug, Clone, Default)]
pub struct SketchData {
    /// Logical sketch key -> raw bloom filter data (msgpack-encoded SBBF snapshot).
    pub sketches: HashMap<String, Vec<u8>>,
    /// The MySQL SET value string (e.g., "s01,s03") indicating which sketch slots
    /// are populated.
    pub set_value: Option<String>,
    /// The LZ4+msgpack encoded ext blob containing all sketch data for this row.
    pub ext_blob: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_data_default() {
        let data = SketchData::default();
        assert!(data.sketches.is_empty());
        assert!(data.set_value.is_none());
        assert!(data.ext_blob.is_none());
    }
}

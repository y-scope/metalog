mod ext_codec;
mod sbbf;
mod sketch_registry;

pub use ext_codec::{decode_ext_blob, encode_ext_blob};
use metalog_types::processors::SketchProcessor;
pub use sbbf::sbbf_contains;
pub use sketch_registry::SketchRegistry;

/// Premium sketch/bloom filter processor.
///
/// Manages `_sketch_registry`, encodes/decodes ext blobs (LZ4+msgpack),
/// evaluates Split Block Bloom Filters (SBBF) for query pruning.
pub struct SketchExtension;

impl SketchExtension {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SketchExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl SketchProcessor for SketchExtension {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_extension_is_object_safe() {
        let ext: Box<dyn SketchProcessor> = Box::new(SketchExtension::new());
        assert_eq!(ext.name(), "sketches");
    }
}

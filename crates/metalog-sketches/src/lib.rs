use metalog_types::processors::SketchProcessor;

/// Premium sketch/bloom filter processor.
///
/// Manages the `_sketch_registry`, encodes/decodes ext blobs (LZ4+msgpack),
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
    fn sketch_extension_creates() {
        let ext = SketchExtension::new();
        assert_eq!(ext.name(), "sketches");
    }

    #[test]
    fn sketch_extension_is_object_safe() {
        let ext: Box<dyn SketchProcessor> = Box::new(SketchExtension::new());
        assert_eq!(ext.name(), "sketches");
    }
}

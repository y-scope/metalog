mod registry;

use std::sync::Arc;

use metalog_types::processors::AggProcessor;
pub use registry::AggRegistry;

/// Premium aggregation column processor.
///
/// Manages the `_agg_registry`, resolves logical agg keys to physical `agg_fNN`
/// columns, and generates UPSERT column clauses and __AGG.* query resolution.
pub struct AggExtension {
    registry: Option<Arc<AggRegistry>>,
}

impl AggExtension {
    pub fn new() -> Self {
        Self { registry: None }
    }

    /// Sets the agg registry (created per table after DB connection).
    pub fn with_registry(mut self, registry: Arc<AggRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Returns the registry if set.
    pub fn registry(&self) -> Option<&Arc<AggRegistry>> {
        self.registry.as_ref()
    }
}

impl Default for AggExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl AggProcessor for AggExtension {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_extension_creates() {
        let ext = AggExtension::new();
        assert_eq!(ext.name(), "aggs");
        assert!(ext.registry().is_none());
    }

    #[test]
    fn agg_extension_is_object_safe() {
        let ext: Box<dyn AggProcessor> = Box::new(AggExtension::new());
        assert_eq!(ext.name(), "aggs");
    }
}

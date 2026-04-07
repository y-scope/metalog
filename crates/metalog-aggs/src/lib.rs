use metalog_types::processors::AggProcessor;

/// Premium aggregation column processor.
///
/// Manages the `_agg_registry`, resolves logical agg keys to physical `agg_fNN`
/// columns, and generates UPSERT column clauses for aggregation data.
pub struct AggExtension {
    // Will hold: AggRegistry (similar to ColumnRegistry but for agg columns),
    // DDL templates, query resolution logic.
}

impl AggExtension {
    pub fn new() -> Self {
        Self {}
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
    }

    #[test]
    fn agg_extension_is_object_safe() {
        let ext: Box<dyn AggProcessor> = Box::new(AggExtension::new());
        assert_eq!(ext.name(), "aggs");
    }
}

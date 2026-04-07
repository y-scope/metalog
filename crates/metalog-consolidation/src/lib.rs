mod inflight;
mod policy;

pub use inflight::InFlightSet;
use metalog_types::processors::ConsolidationProvider;
pub use policy::{FileGroup, Policy, TimeWindowPolicy};

/// Premium consolidation pipeline module.
///
/// Provides: task queue (Queue, Task, SKIP LOCKED claiming), Planner (7-step
/// pipeline), Policy trait + TimeWindowPolicy + SparkJobPolicy + PolicyChain,
/// InFlightSet, Worker Core + Prefetcher, ArchiveCreator + Compressor trait,
/// two-phase worker shutdown.
pub struct ConsolidationModule;

impl ConsolidationModule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConsolidationModule {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolidationProvider for ConsolidationModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consolidation_module_creates() {
        let module = ConsolidationModule::new();
        assert_eq!(module.name(), "consolidation");
    }

    #[test]
    fn consolidation_module_is_object_safe() {
        let module: Box<dyn ConsolidationProvider> = Box::new(ConsolidationModule::new());
        assert_eq!(module.name(), "consolidation");
    }
}

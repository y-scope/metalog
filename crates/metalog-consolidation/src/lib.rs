mod inflight;
mod planner;
mod policy;
mod task_queue;
mod worker;

pub use inflight::InFlightSet;
use metalog_types::processors::ConsolidationProvider;
pub use planner::{Planner, PlannerConfig};
pub use policy::{FileGroup, Policy, TimeWindowPolicy};
pub use task_queue::{
    marshal_payload,
    marshal_result,
    unmarshal_payload,
    unmarshal_result,
    ConsolidationPayload,
    Queue,
    Task,
    TaskPayload,
    TaskResult,
    TaskState,
    TASK_PAYLOAD_VERSION,
};
pub use worker::{Prefetcher, WorkerUnit};

/// Premium consolidation pipeline module.
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
    fn consolidation_module_is_object_safe() {
        let module: Box<dyn ConsolidationProvider> = Box::new(ConsolidationModule::new());
        assert_eq!(module.name(), "consolidation");
    }
}

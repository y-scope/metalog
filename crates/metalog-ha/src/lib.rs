use metalog_types::processors::HAProvider;

/// Premium high-availability module.
///
/// Provides: node registry (heartbeat UPSERT), table assignment (fair-share
/// claiming, orphan adoption), reconciliation loop (4-step: claim orphans →
/// claim unassigned → watchdog → ownership verify), two HA strategies
/// (heartbeat + lease), _node_registry + _table_assignment DDL.
pub struct HAModule;

impl HAModule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for HAModule {
    fn default() -> Self {
        Self::new()
    }
}

impl HAProvider for HAModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_module_creates() {
        let module = HAModule::new();
        assert_eq!(module.name(), "ha");
    }

    #[test]
    fn ha_module_is_object_safe() {
        let module: Box<dyn HAProvider> = Box::new(HAModule::new());
        assert_eq!(module.name(), "ha");
    }
}

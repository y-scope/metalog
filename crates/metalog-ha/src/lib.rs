mod liveness;
mod reconcile;
mod registry;

use std::{sync::Arc, time::Duration};

pub use liveness::LivenessLoop;
use metalog_config::CoordinatorConfig;
use metalog_types::processors::HAProvider;
pub use reconcile::ReconciliationLoop;
pub use registry::NodeRegistry;
use sqlx::MySqlPool;
use tokio_util::sync::CancellationToken;

/// Premium high-availability module.
///
/// Manages multi-node coordination: heartbeat/lease liveness, fair-share
/// table claiming, orphan adoption, stalled coordinator watchdog.
pub struct HAModule {
    registry: Arc<NodeRegistry>,
    config: CoordinatorConfig,
}

impl HAModule {
    pub fn new(db: MySqlPool, node_id: &str, config: CoordinatorConfig) -> Self {
        Self {
            registry: Arc::new(NodeRegistry::new(db, node_id)),
            config,
        }
    }

    /// Returns the node registry.
    pub fn registry(&self) -> &Arc<NodeRegistry> {
        &self.registry
    }

    /// Starts the liveness loop (heartbeat or lease renewal).
    pub fn start_liveness(&self, token: CancellationToken) -> tokio::task::JoinHandle<()> {
        let registry = self.registry.clone();
        let strategy = self.config.ha_strategy;
        let interval = Duration::from_secs(self.config.heartbeat_interval_secs);
        let lease_ttl = Duration::from_secs(self.config.lease_ttl_secs);

        tokio::spawn(async move {
            LivenessLoop::new(registry, strategy, interval, lease_ttl)
                .run(token)
                .await;
        })
    }

    /// Starts the reconciliation loop.
    pub fn start_reconciliation(
        &self,
        token: CancellationToken,
        on_start: Arc<dyn Fn(&str) + Send + Sync>,
        on_stop: Arc<dyn Fn(&str) + Send + Sync>,
    ) -> tokio::task::JoinHandle<()> {
        let registry = self.registry.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            ReconciliationLoop::new(registry, config, on_start, on_stop)
                .run(token)
                .await;
        })
    }
}

impl HAProvider for HAModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ha_module_is_object_safe() {
        // Can't construct without DB, but verify trait is object-safe.
        fn _assert(_: &dyn HAProvider) {}
    }
}

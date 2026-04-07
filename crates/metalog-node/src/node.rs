use std::{collections::HashMap, sync::Arc};

use metalog_config::NodeConfig;
use tokio_util::sync::CancellationToken;

use crate::{CoordinatorUnit, Resources};

/// Top-level node orchestrator.
pub struct Node {
    _config: NodeConfig,
    shared: Arc<Resources>,
    coordinators: HashMap<String, CoordinatorUnit>,
    token: CancellationToken,
}

/// Builder for constructing a Node with optional premium providers.
pub struct NodeBuilder {
    config: NodeConfig,
    shared: Arc<Resources>,
}

impl NodeBuilder {
    pub fn new(config: NodeConfig, shared: Arc<Resources>) -> Self {
        Self { config, shared }
    }

    pub fn build(self) -> Node {
        Node {
            _config: self.config,
            shared: self.shared,
            coordinators: HashMap::new(),
            token: CancellationToken::new(),
        }
    }
}

impl Node {
    pub fn shared(&self) -> &Arc<Resources> {
        &self.shared
    }

    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    pub async fn stop(&mut self) {
        tracing::info!("stopping node");
        self.token.cancel();
        for (name, unit) in &self.coordinators {
            tracing::debug!(table = %name, "stopping coordinator");
            unit.stop();
        }
        self.coordinators.clear();
        tracing::info!("node stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_token_size() {
        // Smoke test that Node types compile correctly.
        assert!(std::mem::size_of::<CancellationToken>() > 0);
    }
}

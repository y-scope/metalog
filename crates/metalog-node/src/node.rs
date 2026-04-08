use std::{sync::Arc, time::Duration};

use metalog_config::{NodeConfig, DEFAULT_PROGRESS_STALL_TIMEOUT};
use metalog_consolidation::{InFlightSet, Planner, PlannerConfig, Queue, TimeWindowPolicy};
use metalog_ingestion::BatchingWriter;
use metalog_metastore::FileRecords;
use metalog_schema::{ensure_table, execute_ddl_statements, ColumnRegistry, SCHEMA_SQL};
use metalog_types::file_state::FileState;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    run_periodic,
    CoordinatorUnit,
    Resources,
    ALIAS_REFRESH_INTERVAL,
    PARTITION_MAINTENANCE_INTERVAL,
};

/// Default planner interval (10 seconds).
const DEFAULT_PLANNER_INTERVAL: Duration = Duration::from_secs(10);

/// Default stale buffering threshold (60 minutes).
const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(3600);

/// Top-level node orchestrator.
///
/// Community edition: starts coordinators for all configured tables directly.
/// Enterprise (with HAProvider): coordinators managed by reconciliation loop.
pub struct Node {
    #[allow(dead_code)]
    config: NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    token: CancellationToken,
    join_set: JoinSet<()>,
}

/// Builder for constructing a Node with optional premium providers.
pub struct NodeBuilder {
    config: NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
}

impl NodeBuilder {
    pub fn new(config: NodeConfig, shared: Arc<Resources>) -> Self {
        Self {
            config,
            shared,
            writer: None,
        }
    }

    /// Sets the BatchingWriter (required for ingestion).
    pub fn with_writer(mut self, writer: Arc<BatchingWriter>) -> Self {
        self.writer = Some(writer);
        self
    }

    pub fn build(self) -> Node {
        Node {
            config: self.config,
            shared: self.shared,
            writer: self.writer,
            token: CancellationToken::new(),
            join_set: JoinSet::new(),
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

    /// Starts the node: ensures schema, starts coordinators for configured tables.
    ///
    /// In community edition, all tables are started directly. In enterprise
    /// (with HAProvider), the reconciliation loop manages table ownership.
    pub async fn start(&mut self, tables: &[String]) -> Result<(), NodeError> {
        // Ensure system tables exist.
        execute_ddl_statements(&self.shared.db, SCHEMA_SQL).await?;
        tracing::info!("schema ready");

        // Start coordinators for each table.
        for table_name in tables {
            self.start_coordinator(table_name).await?;
        }

        tracing::info!(tables = tables.len(), "node started");
        Ok(())
    }

    /// Starts a coordinator for a single table.
    async fn start_coordinator(&mut self, table_name: &str) -> Result<(), NodeError> {
        // Ensure the table is provisioned.
        ensure_table(&self.shared.db, table_name, None).await?;

        // Create column registry.
        let registry = Arc::new(ColumnRegistry::new(self.shared.db.clone(), table_name).await?);

        // Cache registry in shared resources.
        self.shared
            .set_column_registry(table_name, registry.clone())
            .await;

        // Register with BatchingWriter if available.
        if let Some(writer) = &self.writer {
            writer.set_registry(table_name, registry.clone()).await;
        }

        // Create coordinator unit.
        let table_cfg = metalog_metastore::default_table_config();
        let consolidation_cfg = table_cfg.consolidation.clone();
        let unit = CoordinatorUnit::new(
            table_name,
            table_cfg,
            registry.clone(),
            DEFAULT_PROGRESS_STALL_TIMEOUT,
        );
        let unit_token = unit.token().clone();

        // Spawn partition maintenance task.
        let pm = metalog_schema::PartitionManager::new(self.shared.db.clone(), table_name, 7, 90);
        let pm_token = unit_token.clone();
        self.join_set.spawn(async move {
            run_periodic(pm_token, PARTITION_MAINTENANCE_INTERVAL, || async {
                if let Err(e) = pm.run_maintenance().await {
                    tracing::warn!(error = %e, "partition maintenance failed");
                }
            })
            .await;
        });

        // Spawn alias refresh task.
        let reg = registry.clone();
        let ar_token = unit_token.clone();
        self.join_set.spawn(async move {
            run_periodic(ar_token, ALIAS_REFRESH_INTERVAL, || async {
                if let Err(e) = reg.refresh_aliases().await {
                    tracing::warn!(error = %e, "alias refresh failed");
                }
            })
            .await;
        });

        // Spawn consolidation planner if enabled in table config.
        if consolidation_cfg.enabled {
            let archive_backend = self.config.storage.default_backend.clone();
            let archive_bucket = self
                .config
                .storage
                .backends
                .get(&archive_backend)
                .map(|b| b.bucket.clone())
                .unwrap_or_default();

            let stale_threshold = if consolidation_cfg.stale_buffering_mins > 0 {
                Duration::from_secs(consolidation_cfg.stale_buffering_mins as u64 * 60)
            } else if consolidation_cfg.stale_buffering_mins < 0 {
                Duration::ZERO // disabled
            } else {
                DEFAULT_STALE_THRESHOLD
            };

            let file_recs = Arc::new(FileRecords::new(self.shared.db.clone(), table_name));
            let queue = Arc::new(Queue::new(self.shared.db.clone()));
            let in_flight = Arc::new(InFlightSet::new());
            let policy: Arc<dyn metalog_consolidation::Policy> = Arc::new(
                TimeWindowPolicy::new(Duration::from_secs(3600), 2, 100),
            );

            let planner = Planner::new(PlannerConfig {
                file_recs,
                queue,
                policy,
                in_flight,
                table_name: table_name.to_string(),
                archive_backend,
                archive_bucket,
                interval: DEFAULT_PLANNER_INTERVAL,
                stale_threshold,
                buffering_state: FileState::IrArchiveBuffering,
                pending_state: FileState::IrArchiveConsolidationPending,
            });

            let planner_token = unit_token.clone();
            self.join_set.spawn(async move {
                planner.run(planner_token).await;
            });

            tracing::info!(table = table_name, "consolidation planner started");
        }

        tracing::info!(table = table_name, "coordinator started");
        Ok(())
    }

    /// Stops the node: cancels all coordinators and waits for cleanup.
    pub async fn stop(&mut self) {
        tracing::info!("stopping node");
        self.token.cancel();

        // Stop BatchingWriter (drains remaining records).
        if let Some(writer) = &self.writer {
            writer.stop().await;
        }

        // Wait for all background tasks.
        while self.join_set.join_next().await.is_some() {}

        tracing::info!("node stopped");
    }
}

/// Errors from node lifecycle operations.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),

    #[error("schema: {0}")]
    Schema(#[from] metalog_schema::EnsureTableError),

    #[error("registry: {0}")]
    Registry(#[from] metalog_schema::RegistryError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_builder_compiles() {
        assert!(std::mem::size_of::<NodeBuilder>() > 0);
    }

    #[tokio::test]
    async fn cancellation_token_works() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }
}

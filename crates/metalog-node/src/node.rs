use std::{sync::Arc, time::Duration};

use metalog_config::{NodeConfig, DEFAULT_PROGRESS_STALL_TIMEOUT};
use metalog_consolidation::{InFlightSet, Planner, PlannerConfig, Queue, TimeWindowPolicy};
use metalog_ingestion::BatchingWriter;
use metalog_metastore::FileRecords;
use metalog_schema::{ensure_table, execute_ddl_statements, ColumnRegistry, SCHEMA_SQL};
use metalog_types::file_state::FileState;
use tokio_util::sync::CancellationToken;

use crate::{
    run_periodic,
    CoordinatorUnit,
    Resources,
    ALIAS_REFRESH_INTERVAL,
};

/// Default planner interval (10 seconds).
const DEFAULT_PLANNER_INTERVAL: Duration = Duration::from_secs(10);

/// Default stale buffering threshold (60 minutes).
const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(3600);

/// Top-level node orchestrator.
///
/// Community edition: starts coordinators for all configured tables directly.
/// Enterprise (with HAProvider): coordinators managed by reconciliation loop via
/// [`Node::ha_callbacks`].
pub struct Node {
    config: NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    token: CancellationToken,
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
    /// In community edition, call with the full table list. In enterprise (HA) mode,
    /// call with an empty slice — the reconciliation loop drives table assignment via
    /// [`Node::ha_callbacks`].
    pub async fn start(&mut self, tables: &[String]) -> Result<(), NodeError> {
        // Ensure system tables exist.
        execute_ddl_statements(&self.shared.db, SCHEMA_SQL).await?;
        tracing::info!("schema ready");

        // Start coordinators for each table.
        for table_name in tables {
            start_coordinator(
                table_name,
                &self.config,
                self.shared.clone(),
                self.writer.clone(),
                &self.token,
            )
            .await?;
        }

        tracing::info!(tables = tables.len(), "node started");
        Ok(())
    }

    /// Returns the three closures needed to wire HA reconciliation.
    ///
    /// - `on_start(table)` — spawns a coordinator for the table (fire-and-forget)
    /// - `on_stop(table)` — cancels and removes the coordinator
    /// - `stall_checker(table)` — returns true if the coordinator's [`ProgressTracker`] has
    ///   exceeded its stall timeout
    ///
    /// Pass these directly to [`HAModule::start_reconciliation`].
    ///
    /// [`ProgressTracker`]: metalog_coordinator::ProgressTracker
    pub fn ha_callbacks(
        &self,
    ) -> (
        Arc<dyn Fn(&str) + Send + Sync>,
        Arc<dyn Fn(&str) + Send + Sync>,
        Arc<dyn Fn(&str) -> bool + Send + Sync>,
    ) {
        let config = Arc::new(self.config.clone());
        let shared = self.shared.clone();
        let writer = self.writer.clone();
        let parent_token = self.token.clone();

        let on_start = {
            let config = config.clone();
            let shared = shared.clone();
            let writer = writer.clone();
            let token = parent_token.clone();
            Arc::new(move |table: &str| {
                let table = table.to_string();
                let config = config.clone();
                let shared = shared.clone();
                let writer = writer.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        start_coordinator(&table, &config, shared, writer, &token).await
                    {
                        tracing::error!(error = %e, table = %table, "coordinator start failed");
                    }
                });
            }) as Arc<dyn Fn(&str) + Send + Sync>
        };

        let on_stop = {
            let shared = shared.clone();
            Arc::new(move |table: &str| {
                shared.stop_coordinator(table);
            }) as Arc<dyn Fn(&str) + Send + Sync>
        };

        let stall_checker = {
            Arc::new(move |table: &str| -> bool { shared.is_coordinator_stalled(table) })
                as Arc<dyn Fn(&str) -> bool + Send + Sync>
        };

        (on_start, on_stop, stall_checker)
    }

    /// Stops the node: cancels all coordinator tokens, drains the writer.
    pub async fn stop(&mut self) {
        tracing::info!("stopping node");
        // Cancels all coordinator child tokens (partition maintenance, alias refresh, planner).
        self.token.cancel();

        // Drain the BatchingWriter before exit.
        if let Some(writer) = &self.writer {
            writer.stop().await;
        }

        tracing::info!("node stopped");
    }
}

/// Ensures a table is provisioned, creates its column registry, and spawns its background tasks.
///
/// The coordinator unit's cancellation token is created as a child of `parent_token` so that
/// cancelling the node token (on shutdown) propagates to all coordinator tasks.
async fn start_coordinator(
    table_name: &str,
    config: &NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    parent_token: &CancellationToken,
) -> Result<(), NodeError> {
    // Ensure the table is provisioned.
    ensure_table(&shared.db, table_name, None).await?;

    // Create column registry.
    let registry = Arc::new(ColumnRegistry::new(shared.db.clone(), table_name).await?);

    // Cache registry in shared resources (synchronous with parking_lot).
    shared.set_column_registry(table_name, registry.clone());

    // Register with BatchingWriter if available.
    if let Some(writer) = &writer {
        writer.set_registry(table_name, registry.clone()).await;
    }

    // Create coordinator unit with a child token so node shutdown cascades.
    let table_cfg = metalog_metastore::default_table_config();
    let consolidation_cfg = table_cfg.consolidation.clone();
    let unit = Arc::new(CoordinatorUnit::new_with_token(
        table_name,
        table_cfg,
        registry.clone(),
        shared.db.clone(),
        None, // TODO: pass storage registry when wired
        DEFAULT_PROGRESS_STALL_TIMEOUT,
        parent_token,
    ));
    let unit_token = unit.token().clone();

    // Store unit so HA watchdog can inspect stall status and stop it.
    shared.store_coordinator_unit(table_name, unit.clone());

    // Blocking: ensure daily partitions exist before proceeding.
    unit.ensure_partitions_ready().await;

    // Spawn partition maintenance + retention scanner (records progress for
    // HA stall detection).
    unit.spawn_background_tasks();

    // Spawn alias refresh task.
    let reg = registry.clone();
    let ar_token = unit_token.clone();
    tokio::spawn(async move {
        run_periodic(ar_token, ALIAS_REFRESH_INTERVAL, || async {
            if let Err(e) = reg.refresh_aliases().await {
                tracing::warn!(error = %e, "alias refresh failed");
            }
        })
        .await;
    });

    // Spawn consolidation planner if enabled in table config.
    if consolidation_cfg.enabled {
        let archive_backend = config.storage.default_backend.clone();
        let archive_bucket = config
            .storage
            .backends
            .get(&archive_backend)
            .map(|b| b.bucket.clone())
            .unwrap_or_default();

        let stale_threshold = if consolidation_cfg.stale_buffering_mins > 0 {
            Duration::from_secs(consolidation_cfg.stale_buffering_mins as u64 * 60)
        } else if consolidation_cfg.stale_buffering_mins < 0 {
            Duration::ZERO
        } else {
            DEFAULT_STALE_THRESHOLD
        };

        let file_recs = Arc::new(FileRecords::new(shared.db.clone(), table_name));
        let queue = Arc::new(Queue::new(shared.db.clone()));
        let in_flight = Arc::new(InFlightSet::new());
        let policy: Arc<dyn metalog_consolidation::Policy> =
            Arc::new(TimeWindowPolicy::new(Duration::from_secs(3600), 2, 100));

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
        tokio::spawn(async move {
            planner.run(planner_token).await;
        });

        tracing::info!(table = table_name, "consolidation planner started");
    }

    tracing::info!(table = table_name, "coordinator started");
    Ok(())
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

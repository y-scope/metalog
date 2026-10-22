use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use metalog_config::{NodeConfig, DEFAULT_PROGRESS_STALL_TIMEOUT};
use metalog_ingestion::BatchingWriter;
use metalog_schema::{ensure_table, execute_ddl_statements, ColumnRegistry, SCHEMA_SQL};
use metalog_storage::Registry as StorageRegistry;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{run_periodic, CoordinatorUnit, Resources, ALIAS_REFRESH_INTERVAL};

pub type CoordinatorFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type CoordinatorCallback = Arc<dyn Fn(String) -> CoordinatorFuture + Send + Sync>;
pub type StallChecker = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Owns coordinator tasks and shared ingestion resources for one process.
pub struct Node {
    config: NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    storage: Option<Arc<StorageRegistry>>,
    token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

pub struct NodeBuilder {
    config: NodeConfig,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    storage: Option<Arc<StorageRegistry>>,
}

impl NodeBuilder {
    pub fn new(config: NodeConfig, shared: Arc<Resources>) -> Self {
        Self {
            config,
            shared,
            writer: None,
            storage: None,
        }
    }

    pub fn with_writer(mut self, writer: Arc<BatchingWriter>) -> Self {
        self.writer = Some(writer);
        self
    }

    pub fn with_storage(mut self, storage: Arc<StorageRegistry>) -> Self {
        self.storage = Some(storage);
        self
    }

    pub fn build(self) -> Node {
        Node {
            config: self.config,
            shared: self.shared,
            writer: self.writer,
            storage: self.storage,
            token: CancellationToken::new(),
            tasks: Vec::new(),
        }
    }
}

impl Node {
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn shared(&self) -> &Arc<Resources> {
        &self.shared
    }

    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Retains node-level tasks, including HA, for graceful shutdown.
    pub fn track_background_task(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    /// Ensures schema and starts the supplied table coordinators. HA callers pass
    /// an empty list and let reconciliation drive the awaitable callbacks.
    pub async fn start(&mut self, tables: &[String]) -> Result<(), NodeError> {
        execute_ddl_statements(&self.shared.db, SCHEMA_SQL).await?;
        for table_name in tables {
            if let Err(error) = start_coordinator(
                table_name,
                self.shared.clone(),
                self.writer.clone(),
                self.storage.clone(),
                &self.token,
            )
            .await
            {
                self.stop().await;
                return Err(error);
            }
        }
        tracing::info!(tables = tables.len(), "node started");
        Ok(())
    }

    /// Startup/stop completes before HA changes its local running set. Startup
    /// errors are returned to reconciliation so the claim can be released/retried.
    pub fn ha_callbacks(&self) -> (CoordinatorCallback, CoordinatorCallback, StallChecker) {
        let shared = self.shared.clone();
        let writer = self.writer.clone();
        let storage = self.storage.clone();
        let token = self.token.clone();
        let on_start: CoordinatorCallback = Arc::new(move |table: String| {
            let shared = shared.clone();
            let writer = writer.clone();
            let storage = storage.clone();
            let token = token.clone();
            Box::pin(async move {
                start_coordinator(&table, shared, writer, storage, &token)
                    .await
                    .map_err(|e| e.to_string())
            })
        });
        let shared = self.shared.clone();
        let on_stop: CoordinatorCallback = Arc::new(move |table: String| {
            let shared = shared.clone();
            Box::pin(async move {
                shared.stop_coordinator(&table).await;
                Ok(())
            })
        });
        let shared = self.shared.clone();
        let stalled: StallChecker = Arc::new(move |table| shared.is_coordinator_stalled(table));
        (on_start, on_stop, stalled)
    }

    /// Cancels and joins background work before draining ingestion.
    pub async fn stop(&mut self) {
        self.token.cancel();
        for mut handle in self.tasks.drain(..) {
            if tokio::time::timeout(Duration::from_secs(10), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.shared.stop_coordinators().await;
        if let Some(writer) = &self.writer {
            writer.stop().await;
        }
        tracing::info!("node stopped");
    }
}

async fn start_coordinator(
    table_name: &str,
    shared: Arc<Resources>,
    writer: Option<Arc<BatchingWriter>>,
    storage: Option<Arc<StorageRegistry>>,
    parent_token: &CancellationToken,
) -> Result<(), NodeError> {
    // Serialize startup with stop so duplicate callbacks cannot leak tasks or
    // replace the registered unit while an earlier instance is still running.
    let _guard = shared.coordinator_lifecycle.lock().await;
    if parent_token.is_cancelled() {
        return Err(NodeError::ShuttingDown);
    }
    let existing = shared.coordinator(table_name);
    if existing.is_none() {
        ensure_table(&shared.db, table_name, None).await?;
    }

    let (config_json,): (Option<String>,) =
        sqlx::query_as("SELECT CAST(config AS CHAR) FROM _table_config WHERE table_name = ?")
            .bind(table_name)
            .fetch_one(&shared.db)
            .await?;
    let table_cfg =
        metalog_metastore::decode_table_config(config_json.as_deref()).map_err(|e| {
            NodeError::TableConfig {
                table: table_name.to_string(),
                message: e.to_string(),
            }
        })?;
    // Do not accept a configured lifecycle until its archive/storage path is
    // available. Silently replacing these values with defaults loses user intent.
    if table_cfg.consolidation.enabled {
        return Err(NodeError::UnsupportedFeature {
            table: table_name.to_string(),
            feature: "consolidation requires a configured archive executor",
        });
    }
    if table_cfg.retention.enabled && storage.is_none() {
        return Err(NodeError::UnsupportedFeature {
            table: table_name.to_string(),
            feature: "retention requires configured storage backends",
        });
    }
    if table_cfg.retention.enabled
        && (!matches!(table_cfg.retention.type_name.as_str(), "" | "default")
            || table_cfg.retention.scan_interval_secs == 0
            || table_cfg.retention.scan_interval_secs > i64::MAX as u64 / 1_000_000_000
            || table_cfg.retention.delete_rate == 0
            || table_cfg.retention.grace_period_secs > i64::MAX as u64 / 1_000_000_000)
    {
        return Err(NodeError::TableConfig {
            table: table_name.to_string(),
            message: "retention requires the default strategy, positive scan interval/delete \
                      rate, and intervals representable in nanoseconds"
                .into(),
        });
    }
    if let Some(existing) = existing {
        let encoded = |config: &metalog_types::config::TableConfig| {
            metalog_metastore::encode_table_config(config).map_err(|error| NodeError::TableConfig {
                table: table_name.to_string(),
                message: error.to_string(),
            })
        };
        if encoded(existing.table_config())? == encoded(&table_cfg)? {
            return Ok(());
        }
        tracing::info!(
            table = table_name,
            "table configuration changed, restarting coordinator"
        );
        shared.remove_coordinator(table_name);
        existing.stop().await;
    }

    let registry = Arc::new(ColumnRegistry::new(shared.db.clone(), table_name).await?);
    let unit = Arc::new(CoordinatorUnit::new_with_token(
        table_name,
        table_cfg,
        registry.clone(),
        shared.db.clone(),
        storage,
        DEFAULT_PROGRESS_STALL_TIMEOUT,
        parent_token,
    ));
    unit.ensure_partitions_ready().await?;
    if parent_token.is_cancelled() {
        return Err(NodeError::ShuttingDown);
    }
    shared.set_column_registry(table_name, registry.clone());
    if let Some(writer) = &writer {
        writer.set_registry(table_name, registry.clone()).await;
    }
    shared.store_coordinator_unit(table_name, unit.clone());
    unit.spawn_background_tasks();

    let alias_token = unit.token().clone();
    let progress_unit = unit.clone();
    unit.spawn_task(async move {
        run_periodic(alias_token, ALIAS_REFRESH_INTERVAL, || async {
            if let Err(error) = registry.refresh_aliases().await {
                tracing::warn!(error = %error, "alias refresh failed");
            }
            // This loop runs every minute. Hourly partition maintenance has its
            // own progress deadline, so idle tables do not trip the watchdog.
            progress_unit.record_progress();
        })
        .await;
    });
    tracing::info!(table = table_name, "coordinator started");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("sql: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("schema: {0}")]
    Schema(#[from] metalog_schema::EnsureTableError),
    #[error("registry: {0}")]
    Registry(#[from] metalog_schema::RegistryError),
    #[error("partition: {0}")]
    Partition(#[from] metalog_schema::PartitionError),
    #[error("invalid config for table {table}: {message}")]
    TableConfig { table: String, message: String },
    #[error("table {table}: {feature}")]
    UnsupportedFeature {
        table: String,
        feature: &'static str,
    },
    #[error("node is shutting down")]
    ShuttingDown,
}

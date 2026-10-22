use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc, time::Duration};

use metalog_config::{CoordinatorConfig, HAStrategy};
use metalog_logutil::FailureLogger;
use tokio_util::sync::CancellationToken;

use crate::NodeRegistry;

/// A coordinator transition completes before reconciliation updates local ownership.
pub type CoordinatorFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type CoordinatorCallback = Arc<dyn Fn(String) -> CoordinatorFuture + Send + Sync>;
pub type StallChecker = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Reconciles ownership and coordinator lifecycle. Node membership is heartbeated in
/// both strategies; leases additionally fence ownership of individual tables.
pub struct ReconciliationLoop {
    registry: Arc<NodeRegistry>,
    config: CoordinatorConfig,
    on_start: CoordinatorCallback,
    on_stop: CoordinatorCallback,
    stall_checker: StallChecker,
}

impl ReconciliationLoop {
    pub fn new(
        registry: Arc<NodeRegistry>,
        config: CoordinatorConfig,
        on_start: CoordinatorCallback,
        on_stop: CoordinatorCallback,
        stall_checker: StallChecker,
    ) -> Self {
        Self {
            registry,
            config,
            on_start,
            on_stop,
            stall_checker,
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let fl = FailureLogger::new(Duration::from_secs(60));
        let interval = Duration::from_secs(self.config.reconciliation_interval_secs.max(1));
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut running = HashSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = ticker.tick() => {
                    let result = tokio::select! {
                        _ = token.cancelled() => break,
                        result = self.reconcile_once(&mut running) => result,
                    };
                    match result {
                        Ok(()) => fl.ok(),
                        Err(e) => {
                            fl.fail(&format!("reconciliation failed: {e}"));
                            // Without an authoritative ownership check, stop local work.
                            // The next successful cycle can restart still-owned tables.
                            self.stop_running(&mut running).await;
                        }
                    }
                }
            }
        }

        self.stop_running(&mut running).await;
        // Also release a claim whose startup was cancelled before it entered `running`.
        if let Ok(tables) = self.registry.get_my_tables().await {
            for table in tables {
                if let Err(e) = self.registry.release_table(&table).await {
                    tracing::warn!(table, error = %e, "failed to release table during shutdown");
                }
            }
        }
        // A stopped node must no longer dilute its replacements' fair share.
        // Liveness uses UPDATE, so an in-flight heartbeat cannot resurrect it.
        if let Err(e) = self.registry.unregister_node().await {
            tracing::warn!(error = %e, "failed to deregister node during shutdown");
        }
    }

    async fn stop_running(&self, running: &mut HashSet<String>) {
        for table in running.drain() {
            if let Err(e) = (self.on_stop)(table.clone()).await {
                tracing::error!(table, error = %e, "coordinator stop failed");
            }
        }
    }

    /// Performs one reconciliation cycle; public for deterministic cluster tests.
    pub async fn reconcile_once(&self, running: &mut HashSet<String>) -> Result<(), sqlx::Error> {
        self.registry.release_inactive_tables().await?;
        let dead_threshold = Duration::from_secs(self.config.dead_node_threshold_secs)
            .as_nanos()
            .min(i64::MAX as u128) as i64;
        let lease_ttl = match self.config.ha_strategy {
            HAStrategy::Heartbeat => {
                self.registry
                    .claim_orphans_heartbeat(dead_threshold)
                    .await?;
                None
            }
            HAStrategy::Lease => {
                self.registry.claim_orphans_lease().await?;
                Some(
                    Duration::from_secs(self.config.lease_ttl_secs)
                        .as_nanos()
                        .min(i64::MAX as u128) as i64,
                )
            }
        };

        // Stop lost ownership before starting or restarting any local work.
        let mut owned: HashSet<String> = match self.config.ha_strategy {
            HAStrategy::Heartbeat => self.registry.get_my_tables().await?,
            HAStrategy::Lease => self.registry.get_my_leased_tables().await?,
        }
        .into_iter()
        .collect();
        let lost: Vec<String> = running.difference(&owned).cloned().collect();
        for table in lost {
            match (self.on_stop)(table.clone()).await {
                Ok(()) => {
                    running.remove(&table);
                }
                Err(e) => tracing::error!(table, error = %e, "coordinator stop failed"),
            }
        }

        let unassigned = self.registry.get_unassigned_tables().await?;
        let active_nodes = self
            .registry
            .count_active_nodes_heartbeat(dead_threshold)
            .await?
            .max(1);
        let total = self.registry.count_assigned_tables().await? + unassigned.len() as i64;
        let fair_share = (total + active_nodes - 1) / active_nodes;
        let mut my_count = owned.len() as i64;
        for table in unassigned {
            if my_count >= fair_share {
                break;
            }
            if self
                .registry
                .claim_table_with_lease(&table, lease_ttl)
                .await?
            {
                my_count += 1;
                owned.insert(table);
            }
        }

        for table in owned {
            if running.contains(&table) && (self.stall_checker)(&table) {
                tracing::warn!(table, "coordinator stalled, restarting");
                if let Err(e) = (self.on_stop)(table.clone()).await {
                    tracing::error!(table, error = %e, "coordinator stop failed");
                    continue;
                }
                running.remove(&table);
            }
            // An idempotent ensure-start callback also refreshes stored table
            // configuration for an already running coordinator.
            match (self.on_start)(table.clone()).await {
                Ok(()) => {
                    running.insert(table);
                }
                Err(e) => {
                    tracing::error!(
                        table, error = %e, "coordinator start failed; releasing assignment"
                    );
                    if let Err(stop_error) = (self.on_stop)(table.clone()).await {
                        tracing::error!(table, error = %stop_error, "coordinator cleanup failed");
                        continue;
                    }
                    running.remove(&table);
                    // A failed startup must not strand the table behind a live owner.
                    self.registry.release_table(&table).await?;
                }
            }
        }
        Ok(())
    }
}

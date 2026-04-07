use std::{collections::HashSet, sync::Arc, time::Duration};

use metalog_config::{CoordinatorConfig, HAStrategy};
use metalog_logutil::FailureLogger;
use tokio_util::sync::CancellationToken;

use crate::NodeRegistry;

/// Reconciliation loop: periodically claims tables and verifies ownership.
///
/// 4 steps per cycle (each tolerates failure independently):
/// 1. Claim orphans — adopt tables from dead nodes
/// 2. Claim unassigned — pick up newly registered tables (fair-share)
/// 3. Watchdog — (future: detect stalled coordinators)
/// 4. Ownership verification — stop coordinators for lost assignments
pub struct ReconciliationLoop {
    registry: Arc<NodeRegistry>,
    config: CoordinatorConfig,
    on_start: Arc<dyn Fn(&str) + Send + Sync>,
    on_stop: Arc<dyn Fn(&str) + Send + Sync>,
}

impl ReconciliationLoop {
    pub fn new(
        registry: Arc<NodeRegistry>,
        config: CoordinatorConfig,
        on_start: Arc<dyn Fn(&str) + Send + Sync>,
        on_stop: Arc<dyn Fn(&str) + Send + Sync>,
    ) -> Self {
        Self {
            registry,
            config,
            on_start,
            on_stop,
        }
    }

    /// Runs until cancelled.
    pub async fn run(&self, token: CancellationToken) {
        let fl = FailureLogger::new(Duration::from_secs(60));
        let interval = Duration::from_secs(self.config.reconciliation_interval_secs);
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(
            interval_secs = interval.as_secs(),
            "reconciliation loop started"
        );

        // Track locally running tables.
        let mut running: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("reconciliation loop stopped");
                    return;
                }
                _ = ticker.tick() => {
                    match self.reconcile_once(&mut running).await {
                        Ok(()) => fl.ok(),
                        Err(e) => fl.fail(&format!("reconciliation failed: {e}")),
                    }
                }
            }
        }
    }

    async fn reconcile_once(&self, running: &mut HashSet<String>) -> Result<(), sqlx::Error> {
        let dead_threshold =
            Duration::from_secs(self.config.dead_node_threshold_secs).as_nanos() as i64;

        // Step 1: Claim orphans.
        let orphaned = match self.config.ha_strategy {
            HAStrategy::Heartbeat => {
                self.registry
                    .claim_orphans_heartbeat(dead_threshold)
                    .await?
            }
            HAStrategy::Lease => self.registry.claim_orphans_lease().await?,
        };
        if orphaned > 0 {
            tracing::info!(orphaned, "claimed orphaned tables");
        }

        // Step 2: Claim unassigned (fair-share).
        let active_nodes = self
            .registry
            .count_active_nodes_heartbeat(dead_threshold)
            .await?
            .max(1);
        let assigned = self.registry.count_assigned_tables().await?;
        let my_count = self.registry.count_my_tables().await?;
        let fair_share = (assigned + active_nodes - 1) / active_nodes; // ceil division

        if my_count < fair_share {
            let unassigned = self.registry.get_unassigned_tables().await?;
            for table in unassigned {
                if my_count >= fair_share {
                    break;
                }
                if self.registry.claim_table(&table).await? {
                    tracing::info!(table = %table, "claimed table");
                    (self.on_start)(&table);
                    running.insert(table);
                }
            }
        }

        // Step 3: Watchdog (TODO: detect stalled coordinators via ProgressTracker).

        // Step 4: Ownership verification — stop coordinators for lost assignments.
        let my_tables: HashSet<String> = self.registry.get_my_tables().await?.into_iter().collect();

        // Stop tables we're running but no longer own.
        let to_stop: Vec<String> = running
            .iter()
            .filter(|t| !my_tables.contains(*t))
            .cloned()
            .collect();
        for table in &to_stop {
            tracing::warn!(table = %table, "lost table assignment, stopping");
            (self.on_stop)(table);
            running.remove(table);
        }

        // Start tables we own but aren't running.
        for table in &my_tables {
            if !running.contains(table) {
                tracing::info!(table = %table, "starting coordinator for assigned table");
                (self.on_start)(table);
                running.insert(table.clone());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn fair_share_calculation() {
        // 10 tables, 3 nodes → ceil(10/3) = 4
        let assigned = 10i64;
        let active = 3i64;
        let fair = (assigned + active - 1) / active;
        assert_eq!(fair, 4);
    }

    #[test]
    fn fair_share_single_node() {
        let assigned = 5i64;
        let active = 1i64;
        let fair = (assigned + active - 1) / active;
        assert_eq!(fair, 5);
    }
}

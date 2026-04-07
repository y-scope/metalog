use std::{sync::Arc, time::Duration};

use metalog_config::HAStrategy;
use metalog_logutil::FailureLogger;
use tokio_util::sync::CancellationToken;

use crate::NodeRegistry;

/// Periodic liveness loop: heartbeat or lease renewal.
pub struct LivenessLoop {
    registry: Arc<NodeRegistry>,
    strategy: HAStrategy,
    interval: Duration,
    lease_ttl: Duration,
}

impl LivenessLoop {
    pub fn new(
        registry: Arc<NodeRegistry>,
        strategy: HAStrategy,
        interval: Duration,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            registry,
            strategy,
            interval,
            lease_ttl,
        }
    }

    /// Runs until cancelled. Sends heartbeats or renews leases at each tick.
    pub async fn run(&self, token: CancellationToken) {
        let fl = FailureLogger::new(Duration::from_secs(60));
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tracing::info!(
            strategy = ?self.strategy,
            interval_secs = self.interval.as_secs(),
            "liveness loop started"
        );

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    tracing::info!("liveness loop stopped");
                    return;
                }
                _ = ticker.tick() => {
                    let result = match self.strategy {
                        HAStrategy::Heartbeat => self.registry.send_heartbeat().await,
                        HAStrategy::Lease => {
                            self.registry.renew_leases(self.lease_ttl.as_nanos() as i64).await
                        }
                    };
                    match result {
                        Ok(()) => fl.ok(),
                        Err(e) => fl.fail(&format!("liveness failed: {e}")),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn liveness_cancels() {
        let token = CancellationToken::new();
        let t = token.clone();
        let handle = tokio::spawn(async move {
            // Can't create real LivenessLoop without DB, just test cancellation.
            tokio::select! {
                _ = t.cancelled() => {}
            }
        });
        token.cancel();
        handle.await.unwrap();
    }
}

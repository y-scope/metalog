use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use axum::{extract::State, http::StatusCode, routing::get, Router};

/// HTTP health check server with liveness and readiness probes.
pub struct HealthServer {
    ready: Arc<AtomicBool>,
    port: u16,
}

impl HealthServer {
    pub fn new(port: u16) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            port,
        }
    }

    /// Sets the readiness state.
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// Returns the readiness state.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Starts the HTTP server. Returns when the server shuts down.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        let ready = self.ready.clone();
        let app = Router::new()
            .route("/health", get(liveness))
            .route("/health/live", get(liveness))
            .route("/ready", get(readiness))
            .route("/health/ready", get(readiness))
            .with_state(ready);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        tracing::info!(%addr, "health server started");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await
    }
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(ready): State<Arc<AtomicBool>>) -> StatusCode {
    if ready.load(Ordering::Relaxed) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_state() {
        let server = HealthServer::new(8081);
        assert!(!server.is_ready());
        server.set_ready(true);
        assert!(server.is_ready());
        server.set_ready(false);
        assert!(!server.is_ready());
    }
}

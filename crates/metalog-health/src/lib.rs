use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{extract::State, http::StatusCode, routing::get, Router};
use tokio::net::TcpListener;

type Probe = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// HTTP liveness and readiness, with a fresh dependency check on every probe.
#[derive(Clone)]
pub struct HealthServer {
    ready: Arc<AtomicBool>,
    port: u16,
    probe: Option<Probe>,
}

impl HealthServer {
    pub fn new(port: u16) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(false)),
            port,
            probe: None,
        }
    }

    pub fn with_readiness_probe<F, Fut>(mut self, probe: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = bool> + Send + 'static, {
        self.probe = Some(Arc::new(move || Box::pin(probe())));
        self
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub async fn check_ready(&self) -> bool {
        if !self.is_ready() {
            return false;
        }
        let dependencies_ready = match &self.probe {
            Some(probe) => tokio::time::timeout(Duration::from_secs(2), probe())
                .await
                .unwrap_or(false),
            None => true,
        };
        // Shutdown may have started while the dependency check was in flight.
        dependencies_ready && self.is_ready()
    }

    /// Bind separately so the caller can fail startup before advertising readiness.
    pub async fn bind(&self) -> Result<TcpListener, std::io::Error> {
        TcpListener::bind(SocketAddr::from(([0, 0, 0, 0], self.port))).await
    }

    pub async fn serve_with_shutdown<F>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), std::io::Error>
    where
        F: Future<Output = ()> + Send + 'static, {
        let app = Router::new()
            .route("/health", get(liveness))
            .route("/health/live", get(liveness))
            .route("/ready", get(readiness))
            .route("/health/ready", get(readiness))
            .with_state(self);
        tracing::info!(addr = %listener.local_addr()?, "health server listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        self.clone()
            .serve_with_shutdown(self.bind().await?, std::future::pending())
            .await
    }
}

async fn liveness() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}
async fn readiness(State(server): State<HealthServer>) -> (StatusCode, &'static str) {
    if server.check_ready().await {
        (StatusCode::OK, "READY")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "NOT READY")
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn readiness_tracks_dependency_failure_recovery_and_shutdown() {
        let dependency = Arc::new(AtomicBool::new(true));
        let check = dependency.clone();
        let server = HealthServer::new(0).with_readiness_probe(move || {
            let dependency = check.clone();
            async move { dependency.load(Ordering::Relaxed) }
        });
        let listener = server.bind().await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(server.clone().serve_with_shutdown(listener, async {
            let _ = stopped.await;
        }));
        async fn get(address: SocketAddr, path: &str) -> String {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket
                .write_all(
                    format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = String::new();
            socket.read_to_string(&mut response).await.unwrap();
            response
        }
        assert!(get(address, "/ready").await.starts_with("HTTP/1.1 503"));
        server.set_ready(true);
        assert!(get(address, "/ready").await.starts_with("HTTP/1.1 200"));
        dependency.store(false, Ordering::Relaxed);
        assert!(get(address, "/ready").await.starts_with("HTTP/1.1 503"));
        assert!(get(address, "/health").await.starts_with("HTTP/1.1 200"));
        dependency.store(true, Ordering::Relaxed);
        assert!(get(address, "/health/ready")
            .await
            .starts_with("HTTP/1.1 200"));
        server.set_ready(false);
        assert!(get(address, "/ready").await.starts_with("HTTP/1.1 503"));
        stop.send(()).unwrap();
        handle.await.unwrap().unwrap();
    }
}

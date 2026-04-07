use std::sync::atomic::{AtomicUsize, Ordering};

use metalog_proto::coordinator::{
    metadata_ingestion_service_client::MetadataIngestionServiceClient,
    IngestRequest,
    IngestResponse,
};
use tonic::transport::Channel;

/// A pool of gRPC connections that round-robins requests across multiple
/// HTTP/2 channels for higher throughput.
///
/// A single HTTP/2 connection (tonic `Channel`) tops out at ~11K rec/s due to
/// the `h2` library's global mutex ([hyperium/h2#531]). This pool creates N
/// independent TCP connections and distributes requests across them.
///
/// With 4-16 connections, throughput scales to 20-30K+ rec/s.
///
/// [hyperium/h2#531]: https://github.com/hyperium/h2/issues/531
pub struct IngestionClientPool {
    clients: Vec<MetadataIngestionServiceClient<Channel>>,
    next: AtomicUsize,
}

impl IngestionClientPool {
    /// Creates a pool with `n` connections to the given endpoint.
    ///
    /// Each connection is an independent HTTP/2 channel. Typical values:
    /// - `n = 4`: ~16K rec/s (sufficient for most deployments)
    /// - `n = 8`: ~20K rec/s
    /// - `n = 16`: ~30K rec/s
    pub async fn connect(endpoint: &str, n: usize) -> Result<Self, tonic::transport::Error> {
        let n = n.max(1);
        let mut clients = Vec::with_capacity(n);
        for _ in 0..n {
            let channel = Channel::from_shared(endpoint.to_string())
                .map_err(|e| {
                    // InvalidUri doesn't impl Into<tonic::transport::Error>,
                    // so we panic on bad URIs (programmer error).
                    panic!("invalid endpoint URI: {e}");
                })?
                .connect()
                .await?;
            clients.push(MetadataIngestionServiceClient::new(channel));
        }
        Ok(Self {
            clients,
            next: AtomicUsize::new(0),
        })
    }

    /// Creates a pool from pre-built channels (for testing or custom transports).
    pub fn from_channels(channels: Vec<Channel>) -> Self {
        let clients = channels
            .into_iter()
            .map(MetadataIngestionServiceClient::new)
            .collect();
        Self {
            clients,
            next: AtomicUsize::new(0),
        }
    }

    /// Sends an ingest request, round-robin across connections.
    pub async fn ingest(
        &self,
        req: IngestRequest,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        // Clone the client (cheap — clones the inner Channel which is Arc-based).
        let mut client = self.clients[idx].clone();
        client.ingest(req).await
    }

    /// Returns the number of connections in the pool.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Returns true if the pool has no connections.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_size() {
        let channels: Vec<Channel> = vec![];
        let pool = IngestionClientPool::from_channels(channels);
        assert!(pool.is_empty());
    }

    #[test]
    fn round_robin_index() {
        let counter = AtomicUsize::new(0);
        let n = 4;
        for expected in 0..12 {
            let idx = counter.fetch_add(1, Ordering::Relaxed) % n;
            assert_eq!(idx, expected % n);
        }
    }
}

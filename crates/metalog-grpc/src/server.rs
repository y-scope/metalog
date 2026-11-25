use std::{io, net::SocketAddr};

use tokio::net::{TcpListener, TcpStream};
use tokio_stream::{wrappers::TcpListenerStream, Stream, StreamExt};

/// gRPC server wrapper around tonic.
pub struct GrpcServer {
    port: u16,
}

impl GrpcServer {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.port))
    }

    /// Accept connections with low-latency delivery of gRPC response frames.
    ///
    /// Tonic does not configure sockets supplied through `serve_with_incoming`.
    /// Without TCP_NODELAY, a small response header followed by a data frame can
    /// stall behind the peer's delayed ACK, even on an established connection.
    pub fn incoming(listener: TcpListener) -> impl Stream<Item = io::Result<TcpStream>> {
        TcpListenerStream::new(listener).map(|accepted| {
            let stream = accepted?;
            stream.set_nodelay(true)?;
            Ok(stream)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_addr() {
        let server = GrpcServer::new(9090);
        assert_eq!(server.port(), 9090);
        assert_eq!(server.addr().port(), 9090);
    }

    #[tokio::test]
    async fn every_accepted_connection_has_nodelay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut incoming = GrpcServer::incoming(listener);
        for _ in 0..3 {
            let _client = TcpStream::connect(address).await.unwrap();
            let stream = tokio::time::timeout(std::time::Duration::from_secs(5), incoming.next())
                .await
                .expect("listener did not accept connection")
                .expect("listener ended")
                .expect("accept/configure connection failed");
            assert!(stream.nodelay().unwrap());
        }
    }
}

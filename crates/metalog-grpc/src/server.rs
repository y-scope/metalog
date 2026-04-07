use std::net::SocketAddr;

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
}

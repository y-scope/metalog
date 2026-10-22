// Keep tonic's concrete Status throughout the RPC boundary and its validators.
#[allow(clippy::result_large_err)]
mod admin_handler;
#[allow(clippy::result_large_err)]
mod client_pool;
mod ingestion_handler;
mod metadata_handler;
#[allow(clippy::result_large_err)]
mod query_handler;
mod server;

pub use admin_handler::AdminHandler;
pub use client_pool::IngestionClientPool;
pub use ingestion_handler::IngestionHandler;
pub use metadata_handler::MetadataHandler;
pub use query_handler::QueryHandler;
pub use server::GrpcServer;

/// Generated protobuf types for the coordinator gRPC services
/// (MetadataIngestionService, AdminService).
pub mod coordinator {
    tonic::include_proto!("com.yscope.metalog.coordinator.grpc");
}

/// Generated protobuf types for the query gRPC services
/// (SplitQueryService, MetadataService).
pub mod query {
    tonic::include_proto!("com.yscope.metalog.query.api.proto.grpc");
}

// Re-export commonly used types at the crate root for convenience.
pub use coordinator::{
    metadata_ingestion_service_client::MetadataIngestionServiceClient,
    metadata_ingestion_service_server::{MetadataIngestionService, MetadataIngestionServiceServer},
    BatchIngestRequest,
    BatchIngestResponse,
    FailedRecord,
    IngestRequest,
    IngestResponse,
    MetadataRecord,
};
pub use query::{
    split_query_service_client::SplitQueryServiceClient,
    split_query_service_server::{SplitQueryService, SplitQueryServiceServer},
    StreamSplitsRequest,
    StreamSplitsResponse,
};

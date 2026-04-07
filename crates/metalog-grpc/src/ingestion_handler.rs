use std::sync::Arc;

use metalog_ingestion::{convert_record, IngestionService};
use metalog_proto::coordinator::{
    metadata_ingestion_service_server::MetadataIngestionService,
    IngestRequest,
    IngestResponse,
};
use tonic::{Request, Response, Status};

/// gRPC handler for the MetadataIngestionService.
pub struct IngestionHandler {
    service: Arc<IngestionService>,
}

impl IngestionHandler {
    pub fn new(service: Arc<IngestionService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl MetadataIngestionService for IngestionHandler {
    async fn ingest(
        &self,
        request: Request<IngestRequest>,
    ) -> Result<Response<IngestResponse>, Status> {
        let req = request.into_inner();
        let table_name = &req.table_name;

        if table_name.is_empty() {
            return Err(Status::invalid_argument("table_name is required"));
        }

        let record = req
            .record
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("record is required"))?;

        let file_record =
            convert_record(record).map_err(|e| Status::invalid_argument(e.to_string()))?;

        let result = self.service.ingest(table_name, file_record).await;

        Ok(Response::new(IngestResponse {
            accepted: result.accepted,
            error: result.error.unwrap_or_default(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_creation() {
        // Verify the handler struct compiles with the right types.
        // Full gRPC testing requires a running server (integration test).
        assert_eq!(
            std::mem::size_of::<IngestionHandler>(),
            std::mem::size_of::<Arc<IngestionService>>()
        );
    }
}

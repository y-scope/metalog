use std::sync::Arc;

use metalog_ingestion::{convert_record, IngestionService};
use metalog_proto::coordinator::{
    metadata_ingestion_service_server::MetadataIngestionService,
    BatchIngestRequest,
    BatchIngestResponse,
    FailedRecord,
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

    async fn batch_ingest(
        &self,
        request: Request<BatchIngestRequest>,
    ) -> Result<Response<BatchIngestResponse>, Status> {
        let req = request.into_inner();
        let table_name = &req.table_name;

        if table_name.is_empty() {
            return Err(Status::invalid_argument("table_name is required"));
        }

        if req.records.is_empty() {
            return Ok(Response::new(BatchIngestResponse {
                accepted_count: 0,
                rejected_count: 0,
                failures: vec![],
            }));
        }

        // Convert proto records to domain records, tracking conversion failures
        // and preserving the original index for each valid record.
        let mut valid_records = Vec::with_capacity(req.records.len());
        let mut original_indices = Vec::with_capacity(req.records.len());
        let mut failures = Vec::new();

        for (i, proto_record) in req.records.iter().enumerate() {
            match convert_record(proto_record) {
                Ok(fr) => {
                    original_indices.push(i as i32);
                    valid_records.push(fr);
                }
                Err(e) => failures.push(FailedRecord {
                    index: i as i32,
                    error: e.to_string(),
                }),
            }
        }

        // Submit valid records via the service (uses submit_wait for backpressure).
        let batch_result = self.service.batch_ingest(table_name, valid_records).await;

        // Remap ingestion failure indices back to original input positions.
        for f in &batch_result.failures {
            failures.push(FailedRecord {
                index: original_indices[f.index as usize],
                error: f.error.clone(),
            });
        }

        let total = req.records.len() as i32;
        let rejected_count = failures.len() as i32;

        Ok(Response::new(BatchIngestResponse {
            accepted_count: total - rejected_count,
            rejected_count,
            failures,
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

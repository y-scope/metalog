use std::sync::Arc;

use metalog_coordinator::TableRegistration;
use metalog_proto::coordinator::{
    admin_service_server::AdminService,
    DeleteKafkaSourceRequest,
    DeleteKafkaSourceResponse,
    InvalidateColumnRequest,
    InvalidateColumnResponse,
    RegisterKafkaSourceRequest,
    RegisterKafkaSourceResponse,
    RegisterTableRequest,
    RegisterTableResponse,
    SetColumnAliasRequest,
    SetColumnAliasResponse,
};
use metalog_types::processors::KafkaProvider;
use tonic::{Request, Response, Status};

/// gRPC handler for the AdminService.
pub struct AdminHandler {
    registration: Arc<TableRegistration>,
    kafka: Option<Arc<dyn KafkaProvider>>,
}

impl AdminHandler {
    pub fn new(
        registration: Arc<TableRegistration>,
        kafka: Option<Arc<dyn KafkaProvider>>,
    ) -> Self {
        Self {
            registration,
            kafka,
        }
    }
}

#[tonic::async_trait]
impl AdminService for AdminHandler {
    async fn register_table(
        &self,
        request: Request<RegisterTableRequest>,
    ) -> Result<Response<RegisterTableResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() {
            return Err(Status::invalid_argument("table_name is required"));
        }

        let display_name = if req.display_name.is_empty() {
            &req.table_name
        } else {
            &req.display_name
        };

        let opts = metalog_coordinator::RegisterTableOpts {
            config_json: req.config_json,
        };

        let created = self
            .registration
            .register_table(&req.table_name, display_name, opts)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(RegisterTableResponse {
            table_name: req.table_name,
            created,
        }))
    }

    async fn register_kafka_source(
        &self,
        request: Request<RegisterKafkaSourceRequest>,
    ) -> Result<Response<RegisterKafkaSourceResponse>, Status> {
        let kafka = self
            .kafka
            .as_ref()
            .ok_or_else(|| Status::unimplemented("Kafka ingestion requires premium edition"))?;

        let req = request.into_inner();
        if req.table_name.is_empty() || req.source_name.is_empty() {
            return Err(Status::invalid_argument(
                "table_name and source_name are required",
            ));
        }
        if req.topic.is_empty() || req.bootstrap_servers.is_empty() {
            return Err(Status::invalid_argument(
                "topic and bootstrap_servers are required",
            ));
        }
        if req.consumer_group_id.is_empty() {
            return Err(Status::invalid_argument("consumer_group_id is required"));
        }

        // Delegate to premium KafkaProvider.
        // TODO: call kafka.register_source() when trait method is implemented
        let _ = kafka;

        Ok(Response::new(RegisterKafkaSourceResponse {
            table_name: req.table_name,
            source_name: req.source_name,
            created: true,
        }))
    }

    async fn delete_kafka_source(
        &self,
        request: Request<DeleteKafkaSourceRequest>,
    ) -> Result<Response<DeleteKafkaSourceResponse>, Status> {
        let _kafka = self
            .kafka
            .as_ref()
            .ok_or_else(|| Status::unimplemented("Kafka ingestion requires premium edition"))?;

        let req = request.into_inner();
        if req.table_name.is_empty() || req.source_name.is_empty() {
            return Err(Status::invalid_argument(
                "table_name and source_name are required",
            ));
        }

        // TODO: call kafka.delete_source() when trait method is implemented

        Ok(Response::new(DeleteKafkaSourceResponse {}))
    }

    async fn set_column_alias(
        &self,
        request: Request<SetColumnAliasRequest>,
    ) -> Result<Response<SetColumnAliasResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.column_name.is_empty() {
            return Err(Status::invalid_argument(
                "table_name and column_name are required",
            ));
        }

        let alias = self
            .registration
            .set_column_alias(&req.table_name, &req.column_name, &req.alias_column)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetColumnAliasResponse {
            column_name: req.column_name,
            alias_column: alias,
        }))
    }

    async fn invalidate_column(
        &self,
        request: Request<InvalidateColumnRequest>,
    ) -> Result<Response<InvalidateColumnResponse>, Status> {
        let req = request.into_inner();
        if req.table_name.is_empty() || req.column_name.is_empty() {
            return Err(Status::invalid_argument(
                "table_name and column_name are required",
            ));
        }

        let previous_key = self
            .registration
            .invalidate_column(&req.table_name, &req.column_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(InvalidateColumnResponse {
            column_name: req.column_name,
            previous_key,
        }))
    }
}

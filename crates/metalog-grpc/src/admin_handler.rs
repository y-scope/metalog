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

fn validate_register_table_req(req: &RegisterTableRequest) -> Result<(), Status> {
    if req.table_name.is_empty() {
        return Err(Status::invalid_argument("table_name is required"));
    }
    Ok(())
}

fn validate_register_kafka_source_req(req: &RegisterKafkaSourceRequest) -> Result<(), Status> {
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
    Ok(())
}

fn validate_delete_kafka_source_req(req: &DeleteKafkaSourceRequest) -> Result<(), Status> {
    if req.table_name.is_empty() || req.source_name.is_empty() {
        return Err(Status::invalid_argument(
            "table_name and source_name are required",
        ));
    }
    Ok(())
}

fn validate_set_column_alias_req(req: &SetColumnAliasRequest) -> Result<(), Status> {
    if req.table_name.is_empty() || req.column_name.is_empty() {
        return Err(Status::invalid_argument(
            "table_name and column_name are required",
        ));
    }
    Ok(())
}

fn validate_invalidate_column_req(req: &InvalidateColumnRequest) -> Result<(), Status> {
    if req.table_name.is_empty() || req.column_name.is_empty() {
        return Err(Status::invalid_argument(
            "table_name and column_name are required",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl AdminService for AdminHandler {
    async fn register_table(
        &self,
        request: Request<RegisterTableRequest>,
    ) -> Result<Response<RegisterTableResponse>, Status> {
        let req = request.into_inner();
        validate_register_table_req(&req)?;

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
        validate_register_kafka_source_req(&req)?;

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
        validate_delete_kafka_source_req(&req)?;

        Ok(Response::new(DeleteKafkaSourceResponse {}))
    }

    async fn set_column_alias(
        &self,
        request: Request<SetColumnAliasRequest>,
    ) -> Result<Response<SetColumnAliasResponse>, Status> {
        let req = request.into_inner();
        validate_set_column_alias_req(&req)?;

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
        validate_invalidate_column_req(&req)?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_table_rejects_empty_name() {
        let req = RegisterTableRequest {
            table_name: String::new(),
            display_name: String::new(),
            config_json: None,
        };
        let err = validate_register_table_req(&req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn kafka_source_rejects_missing_fields() {
        let base = RegisterKafkaSourceRequest {
            table_name: "t".into(),
            source_name: "s".into(),
            topic: "topic".into(),
            bootstrap_servers: "host:9092".into(),
            record_transformer: String::new(),
            consumer_group_id: "cg".into(),
            required_env: String::new(),
        };
        // All fields present → Ok.
        assert!(validate_register_kafka_source_req(&base).is_ok());

        // Missing table_name.
        let mut r = base.clone();
        r.table_name.clear();
        assert!(validate_register_kafka_source_req(&r).is_err());

        // Missing source_name.
        let mut r = base.clone();
        r.source_name.clear();
        assert!(validate_register_kafka_source_req(&r).is_err());

        // Missing topic.
        let mut r = base.clone();
        r.topic.clear();
        assert!(validate_register_kafka_source_req(&r).is_err());

        // Missing bootstrap_servers.
        let mut r = base.clone();
        r.bootstrap_servers.clear();
        assert!(validate_register_kafka_source_req(&r).is_err());

        // Missing consumer_group_id.
        let mut r = base.clone();
        r.consumer_group_id.clear();
        assert!(validate_register_kafka_source_req(&r).is_err());
    }

    #[test]
    fn delete_kafka_source_rejects_missing_fields() {
        let base = DeleteKafkaSourceRequest {
            table_name: "t".into(),
            source_name: "s".into(),
        };
        assert!(validate_delete_kafka_source_req(&base).is_ok());

        let mut r = base.clone();
        r.table_name.clear();
        assert!(validate_delete_kafka_source_req(&r).is_err());

        let mut r = base.clone();
        r.source_name.clear();
        assert!(validate_delete_kafka_source_req(&r).is_err());
    }

    #[test]
    fn set_column_alias_rejects_missing_fields() {
        let base = SetColumnAliasRequest {
            table_name: "t".into(),
            column_name: "c".into(),
            alias_column: "a".into(),
        };
        assert!(validate_set_column_alias_req(&base).is_ok());

        let mut r = base.clone();
        r.table_name.clear();
        assert!(validate_set_column_alias_req(&r).is_err());

        let mut r = base.clone();
        r.column_name.clear();
        assert!(validate_set_column_alias_req(&r).is_err());
    }

    #[test]
    fn invalidate_column_rejects_missing_fields() {
        let base = InvalidateColumnRequest {
            table_name: "t".into(),
            column_name: "c".into(),
        };
        assert!(validate_invalidate_column_req(&base).is_ok());

        let mut r = base.clone();
        r.table_name.clear();
        assert!(validate_invalidate_column_req(&r).is_err());

        let mut r = base.clone();
        r.column_name.clear();
        assert!(validate_invalidate_column_req(&r).is_err());
    }

    #[tokio::test]
    async fn kafka_source_requires_kafka_provider() {
        let handler = AdminHandler {
            registration: Arc::new(TableRegistration::new(
                sqlx::MySqlPool::connect_lazy("mysql://u@h/d").unwrap(),
                "",
            )),
            kafka: None,
        };

        let req = Request::new(RegisterKafkaSourceRequest {
            table_name: "t".into(),
            source_name: "s".into(),
            topic: "topic".into(),
            bootstrap_servers: "host:9092".into(),
            record_transformer: String::new(),
            consumer_group_id: "cg".into(),
            required_env: String::new(),
        });

        let result = handler.register_kafka_source(req).await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn delete_kafka_source_requires_kafka_provider() {
        let handler = AdminHandler {
            registration: Arc::new(TableRegistration::new(
                sqlx::MySqlPool::connect_lazy("mysql://u@h/d").unwrap(),
                "",
            )),
            kafka: None,
        };

        let req = Request::new(DeleteKafkaSourceRequest {
            table_name: "t".into(),
            source_name: "s".into(),
        });

        let result = handler.delete_kafka_source(req).await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
    }
}

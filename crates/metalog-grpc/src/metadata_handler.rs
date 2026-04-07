use std::sync::Arc;

use metalog_metastore::MetadataReader;
use metalog_proto::query::{
    metadata_service_server::MetadataService,
    AggInfo as ProtoAggInfo,
    AggValueType,
    DimensionInfo as ProtoDimensionInfo,
    ListAggsRequest,
    ListAggsResponse,
    ListDimensionsRequest,
    ListDimensionsResponse,
    ListSketchesRequest,
    ListSketchesResponse,
    ListTablesRequest,
    ListTablesResponse,
    SketchInfo as ProtoSketchInfo,
};
use tonic::{Request, Response, Status};

/// gRPC handler for the MetadataService (schema introspection).
pub struct MetadataHandler {
    reader: Arc<MetadataReader>,
}

impl MetadataHandler {
    pub fn new(reader: Arc<MetadataReader>) -> Self {
        Self { reader }
    }
}

#[tonic::async_trait]
impl MetadataService for MetadataHandler {
    async fn list_tables(
        &self,
        _request: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        let tables = self
            .reader
            .list_tables()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ListTablesResponse { tables }))
    }

    async fn list_dimensions(
        &self,
        request: Request<ListDimensionsRequest>,
    ) -> Result<Response<ListDimensionsResponse>, Status> {
        let table = &request.get_ref().table;
        if table.is_empty() {
            return Err(Status::invalid_argument("table is required"));
        }

        let dims = self
            .reader
            .list_dimensions(table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let dimensions = dims
            .into_iter()
            .map(|d| ProtoDimensionInfo {
                name: d.name,
                r#type: d.base_type,
                width: d.width.unwrap_or(0),
                alias_column: d.alias_column.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(ListDimensionsResponse { dimensions }))
    }

    async fn list_aggs(
        &self,
        request: Request<ListAggsRequest>,
    ) -> Result<Response<ListAggsResponse>, Status> {
        let table = &request.get_ref().table;
        if table.is_empty() {
            return Err(Status::invalid_argument("table is required"));
        }

        let aggs = self
            .reader
            .list_aggs(table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let aggs = aggs
            .into_iter()
            .map(|a| ProtoAggInfo {
                name: a.name,
                qualifier: a.qualifier,
                aggregation_type: parse_agg_type(&a.aggregation_type) as i32,
                value_type: if a.value_type == "FLOAT" {
                    AggValueType::Float as i32
                } else {
                    AggValueType::Int as i32
                },
                alias_column: a.alias_column.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(ListAggsResponse { aggs }))
    }

    async fn list_sketches(
        &self,
        request: Request<ListSketchesRequest>,
    ) -> Result<Response<ListSketchesResponse>, Status> {
        let table = &request.get_ref().table;
        if table.is_empty() {
            return Err(Status::invalid_argument("table is required"));
        }

        let sketches = self
            .reader
            .list_sketches(table)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let sketches = sketches
            .into_iter()
            .map(|s| ProtoSketchInfo { name: s.name })
            .collect();

        Ok(Response::new(ListSketchesResponse { sketches }))
    }
}

fn parse_agg_type(s: &str) -> metalog_proto::query::AggregationType {
    use metalog_proto::query::AggregationType;
    match s {
        "EQ" => AggregationType::Eq,
        "GTE" => AggregationType::Gte,
        "GT" => AggregationType::Gt,
        "LTE" => AggregationType::Lte,
        "LT" => AggregationType::Lt,
        "SUM" => AggregationType::Sum,
        "AVG" => AggregationType::Avg,
        "MIN" => AggregationType::Min,
        "MAX" => AggregationType::Max,
        _ => AggregationType::Unspecified,
    }
}

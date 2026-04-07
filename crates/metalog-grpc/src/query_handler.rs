use std::sync::Arc;

use metalog_proto::query::{
    split_query_service_server::SplitQueryService,
    FileInfo,
    QueryStats,
    Split,
    StreamSplitsRequest,
    StreamSplitsResponse,
};
use metalog_query::{validate_filter_expression, OrderBySpec, SplitQueryEngine};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// gRPC handler for the SplitQueryService (StreamSplits).
pub struct QueryHandler {
    engine: Arc<SplitQueryEngine>,
}

impl QueryHandler {
    pub fn new(engine: Arc<SplitQueryEngine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl SplitQueryService for QueryHandler {
    type StreamSplitsStream = ReceiverStream<Result<StreamSplitsResponse, Status>>;

    async fn stream_splits(
        &self,
        request: Request<StreamSplitsRequest>,
    ) -> Result<Response<Self::StreamSplitsStream>, Status> {
        let req = request.into_inner();

        // Validate required fields.
        if req.table.is_empty() {
            return Err(Status::invalid_argument("table is required"));
        }
        if req.order_by.is_empty() {
            return Err(Status::invalid_argument("order_by is required"));
        }

        // Validate filter expression.
        if !req.filter_expression.is_empty() {
            validate_filter_expression(&req.filter_expression)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        // Build ORDER BY specs.
        let order_by: Vec<OrderBySpec> = req
            .order_by
            .iter()
            .map(|ob| OrderBySpec {
                column: ob.column.clone(),
                desc: ob.order == metalog_proto::query::Order::Desc as i32,
            })
            .collect();

        // Validate sort columns.
        SplitQueryEngine::validate_sort_columns(&order_by, req.allow_unindexed_sort)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let order_clauses = SplitQueryEngine::build_order_clauses(&order_by);

        // Build keyset WHERE from cursor.
        let keyset_where = if let Some(ref cursor) = req.cursor {
            let cursor_values: Vec<String> = cursor
                .values
                .iter()
                .map(|cv| match &cv.value {
                    Some(metalog_proto::query::cursor_value::Value::IntVal(v)) => v.to_string(),
                    Some(metalog_proto::query::cursor_value::Value::FloatVal(v)) => v.to_string(),
                    Some(metalog_proto::query::cursor_value::Value::StrVal(v)) => v.clone(),
                    None => String::new(),
                })
                .collect();
            SplitQueryEngine::build_keyset_where(&order_by, &cursor_values, cursor.id)
        } else {
            String::new()
        };

        // Resolve projection columns.
        let columns = req.projection.clone();
        let filter = req.filter_expression.clone();
        let table = req.table.clone();
        let limit = req.limit;

        let engine = self.engine.clone();

        // Stream results via channel.
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            let mut sequence = 0i32;
            let splits_scanned;

            match engine
                .execute_page(
                    &table,
                    &columns,
                    &filter,
                    &order_clauses,
                    &keyset_where,
                    limit,
                )
                .await
            {
                Ok(rows) => {
                    let truncated = limit > 0 && rows.len() as i32 > limit;
                    let result_rows = if truncated {
                        &rows[..limit as usize]
                    } else {
                        &rows
                    };

                    splits_scanned = rows.len() as i64;

                    for row in result_rows {
                        sequence += 1;
                        let split = row_to_split(row);
                        let resp = StreamSplitsResponse {
                            split: Some(split),
                            sequence,
                            stats: None,
                            done: false,
                            cursor: None,
                        };
                        if tx.send(Ok(resp)).await.is_err() {
                            return; // Client disconnected.
                        }
                    }

                    // Final response with stats.
                    let _ = tx
                        .send(Ok(StreamSplitsResponse {
                            split: None,
                            sequence: sequence + 1,
                            stats: Some(QueryStats {
                                splits_scanned,
                                splits_matched: sequence as i64,
                                truncated,
                            }),
                            done: true,
                            cursor: None,
                        }))
                        .await;
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Converts a SplitRow (HashMap) to a proto Split message.
fn row_to_split(row: &metalog_query::SplitRow) -> Split {
    let file = FileInfo {
        min_timestamp: row
            .get("min_timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        max_timestamp: row
            .get("max_timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        state: row
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        record_count: row
            .get("record_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        raw_size_bytes: row
            .get("raw_size_bytes")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        clp_ir_path: row
            .get("clp_ir_path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_ir_storage_backend: row
            .get("clp_ir_storage_backend")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_ir_bucket: row
            .get("clp_ir_bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_ir_size_bytes: row
            .get("clp_ir_size_bytes")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        clp_archive_path: row
            .get("clp_archive_path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_archive_storage_backend: row
            .get("clp_archive_storage_backend")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_archive_bucket: row
            .get("clp_archive_bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        clp_archive_size_bytes: row
            .get("clp_archive_size_bytes")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        clp_archive_created_at: row
            .get("clp_archive_created_at")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        retention_days: row
            .get("retention_days")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32,
        expires_at: row.get("expires_at").and_then(|v| v.as_i64()).unwrap_or(0),
    };

    // Extract dimension columns (dim_fNN → logical key mapping not available
    // at this layer; return physical column names as-is for now).
    let mut dimensions = std::collections::HashMap::new();
    for (key, value) in row {
        if key.starts_with("dim_f") {
            if let Some(s) = value.as_str() {
                dimensions.insert(key.clone(), s.to_string());
            }
        }
    }

    Split {
        file: Some(file),
        dimensions,
        aggs: vec![], // Premium feature.
    }
}

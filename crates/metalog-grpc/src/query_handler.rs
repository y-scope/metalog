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
        file_path: row
            .get("file_path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        file_storage_backend: row
            .get("file_storage_backend")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        file_bucket: row
            .get("file_bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        file_size_bytes: row
            .get("file_size_bytes")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        archive_path: row
            .get("archive_path")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        archive_storage_backend: row
            .get("archive_storage_backend")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        archive_bucket: row
            .get("archive_bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        archive_size_bytes: row
            .get("archive_size_bytes")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        archive_created_at: row
            .get("archive_created_at")
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
        aggs: vec! [], // Premium feature.
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::row_to_split;

    fn make_row(pairs: &[(&str, serde_json::Value)]) -> metalog_query::SplitRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn row_to_split_maps_all_file_fields() {
        let row = make_row(&[
            ("min_timestamp", serde_json::json!(1000)),
            ("max_timestamp", serde_json::json!(2000)),
            ("state", serde_json::json!("IR_CLOSED")),
            ("record_count", serde_json::json!(500)),
            ("raw_size_bytes", serde_json::json!(1024)),
            ("file_path", serde_json::json!("/logs/a.ir")),
            ("file_storage_backend", serde_json::json!("s3")),
            ("file_bucket", serde_json::json!("my-bucket")),
            ("file_size_bytes", serde_json::json!(2048)),
            ("archive_path", serde_json::json!("/archives/a.clp")),
            ("archive_storage_backend", serde_json::json!("s3")),
            ("archive_bucket", serde_json::json!("arch-bucket")),
            ("archive_size_bytes", serde_json::json!(4096)),
            ("archive_created_at", serde_json::json!(3000)),
            ("retention_days", serde_json::json!(30)),
            ("expires_at", serde_json::json!(4000)),
        ]);

        let split = row_to_split(&row);
        let file = split.file.unwrap();
        assert_eq!(file.min_timestamp, 1000);
        assert_eq!(file.max_timestamp, 2000);
        assert_eq!(file.state, "IR_CLOSED");
        assert_eq!(file.record_count, 500);
        assert_eq!(file.raw_size_bytes, 1024);
        assert_eq!(file.file_path, "/logs/a.ir");
        assert_eq!(file.file_storage_backend, "s3");
        assert_eq!(file.file_bucket, "my-bucket");
        assert_eq!(file.file_size_bytes, 2048);
        assert_eq!(file.archive_path, "/archives/a.clp");
        assert_eq!(file.archive_storage_backend, "s3");
        assert_eq!(file.archive_bucket, "arch-bucket");
        assert_eq!(file.archive_size_bytes, 4096);
        assert_eq!(file.archive_created_at, 3000);
        assert_eq!(file.retention_days, 30);
        assert_eq!(file.expires_at, 4000);
        assert!(split.dimensions.is_empty());
        assert!(split.aggs.is_empty());
    }

    #[test]
    fn row_to_split_extracts_dimensions() {
        let row = make_row(&[
            ("min_timestamp", serde_json::json!(0)),
            ("dim_f01", serde_json::json!("host-1")),
            ("dim_f42", serde_json::json!("svc-abc")),
            ("other_col", serde_json::json!("ignored")),
        ]);

        let split = row_to_split(&row);
        assert_eq!(split.dimensions.len(), 2);
        assert_eq!(split.dimensions.get("dim_f01").unwrap(), "host-1");
        assert_eq!(split.dimensions.get("dim_f42").unwrap(), "svc-abc");
        assert!(!split.dimensions.contains_key("other_col"));
    }

    #[test]
    fn row_to_split_defaults_missing_fields() {
        let row = HashMap::new();
        let split = row_to_split(&row);
        let file = split.file.unwrap();
        assert_eq!(file.min_timestamp, 0);
        assert_eq!(file.max_timestamp, 0);
        assert_eq!(file.state, "");
        assert_eq!(file.record_count, 0);
        assert_eq!(file.file_path, "");
        assert_eq!(file.archive_path, "");
        assert_eq!(file.retention_days, 0);
        assert_eq!(file.expires_at, 0);
        assert!(split.dimensions.is_empty());
    }
}

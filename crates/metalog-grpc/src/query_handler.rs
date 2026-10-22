use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_db::validate_sql_identifier;
use metalog_proto::query::{
    cursor_value,
    split_query_service_server::SplitQueryService,
    CursorValue,
    FileInfo,
    KeysetCursor,
    QueryStats,
    Split,
    StreamSplitsRequest,
    StreamSplitsResponse,
};
use metalog_query::{
    resolve_column_ref,
    resolve_projection_columns,
    rewrite_filter_columns,
    OrderBySpec,
    SplitQueryEngine,
    SplitRow,
    QUERY_PAGE_SIZE,
};
use metalog_schema::ColumnRegistry;
use serde_json::Value;
use sqlx::MySqlPool;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct QueryHandler {
    engine: Arc<SplitQueryEngine>,
    db: MySqlPool,
}

impl QueryHandler {
    pub fn new(engine: Arc<SplitQueryEngine>, db: MySqlPool) -> Self {
        Self { engine, db }
    }
}

fn invalid(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}

#[tonic::async_trait]
impl SplitQueryService for QueryHandler {
    type StreamSplitsStream = ReceiverStream<Result<StreamSplitsResponse, Status>>;

    async fn stream_splits(
        &self,
        request: Request<StreamSplitsRequest>,
    ) -> Result<Response<Self::StreamSplitsStream>, Status> {
        let req = request.into_inner();
        validate_sql_identifier(&req.table).map_err(invalid)?;
        if req.order_by.is_empty() {
            return Err(invalid("order_by is required"));
        }
        if req.limit < 0 || req.stream_idle_timeout_ms < 0 {
            return Err(invalid(
                "limit and stream_idle_timeout_ms must be nonnegative",
            ));
        }
        if !req.sketch_expression.is_empty() {
            return Err(Status::unimplemented("sketch filtering is not implemented"));
        }
        if !req.group_by.is_empty() && req.cursor.is_some() {
            return Err(invalid("group_by and cursor are mutually exclusive"));
        }

        // Read a consistent registry per request. An independently cached registry
        // would miss dimensions and aliases created by another node indefinitely.
        let registry = ColumnRegistry::new(self.db.clone(), &req.table)
            .await
            .map_err(|e| Status::internal(format!("load column registry: {e}")))?;
        let filter =
            rewrite_filter_columns(&req.filter_expression, Some(&registry)).map_err(invalid)?;
        let mut columns =
            resolve_projection_columns(&req.projection, Some(&registry)).map_err(invalid)?;
        let group_by = req
            .group_by
            .iter()
            .map(|c| resolve_column_ref(c, Some(&registry)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid)?;
        let order_by = req
            .order_by
            .iter()
            .map(|order| {
                if !matches!(order.order, 0..=2) {
                    return Err(invalid("unknown sort direction"));
                }
                Ok(OrderBySpec {
                    column: resolve_column_ref(&order.column, Some(&registry)).map_err(invalid)?,
                    desc: order.order == 2,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        SplitQueryEngine::validate_sort_columns(
            &order_by,
            req.allow_unindexed_sort || !group_by.is_empty(),
        )
        .map_err(invalid)?;
        let mut seen_orders = std::collections::HashSet::new();
        if order_by.iter().any(|o| !seen_orders.insert(&o.column)) {
            return Err(invalid("duplicate sort column"));
        }

        let order_clauses = if group_by.is_empty() {
            // System fields preserve the Go API contract, including when clients
            // project only dimensions. Sort keys and id are needed for paging.
            if !columns.is_empty() {
                for col in ["id", "min_timestamp", "max_timestamp", "state"]
                    .into_iter()
                    .chain(order_by.iter().map(|o| o.column.as_str()))
                {
                    if !columns.iter().any(|c| c == col) {
                        columns.push(col.into());
                    }
                }
            }
            SplitQueryEngine::build_order_clauses(&order_by)
        } else {
            let projected: Vec<_> = columns
                .iter()
                .map(|c| {
                    if c == "COUNT(*)" {
                        "record_count"
                    } else {
                        SplitQueryEngine::extract_column_name(c)
                    }
                })
                .collect();
            if order_by
                .iter()
                .any(|o| !projected.contains(&o.column.as_str()) && !group_by.contains(&o.column))
            {
                return Err(invalid(
                    "grouped sort columns must be projected aggregates or group keys",
                ));
            }
            let mut clauses: Vec<_> = order_by
                .iter()
                .map(|o| format!("`{}` {}", o.column, if o.desc { "DESC" } else { "ASC" }))
                .collect();
            for col in &group_by {
                if !order_by.iter().any(|o| &o.column == col) {
                    clauses.push(format!("`{col}` ASC"));
                }
            }
            clauses
        };
        SplitQueryEngine::projection_sql(&columns, &group_by).map_err(invalid)?;
        let mut keyset_where = match &req.cursor {
            Some(cursor) => {
                let values = cursor
                    .values
                    .iter()
                    .map(proto_cursor_value)
                    .collect::<Result<Vec<_>, _>>()?;
                SplitQueryEngine::build_typed_keyset_where(&order_by, &values, cursor.id)
                    .map_err(invalid)?
            }
            None => String::new(),
        };
        let dimension_names: HashMap<_, _> = registry
            .all_dim_entries()
            .into_iter()
            .map(|entry| (entry.column_name, entry.dim_key))
            .collect();
        let timeout = Duration::from_millis(if req.stream_idle_timeout_ms == 0 {
            60_000
        } else {
            req.stream_idle_timeout_ms as u64
        });
        let engine = self.engine.clone();
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            let result: Result<(), Status> = async {
                let mut matched = 0i64;
                let mut scanned = 0i64;
                let mut last_cursor = None;
                let truncated;
                let mut offset = 0u64;
                loop {
                    let remaining = if req.limit == 0 {
                        i64::MAX
                    } else {
                        i64::from(req.limit) - matched
                    };
                    // One lookahead row identifies truncation without losing it: the
                    // next page always starts after the last row actually emitted.
                    let fetch_limit =
                        i64::from(QUERY_PAGE_SIZE).min(remaining.saturating_add(1)) as i32;
                    let query = engine.execute_page_offset(
                        &req.table,
                        &columns,
                        &filter,
                        &order_clauses,
                        &keyset_where,
                        fetch_limit,
                        &group_by,
                        offset,
                    );
                    let rows = tokio::select! {
                        _ = tx.closed() => return Ok(()),
                        result = tokio::time::timeout(timeout, query) => result
                            .map_err(|_| Status::deadline_exceeded("query page timeout"))?
                            .map_err(|e| Status::internal(e.to_string()))?,
                    };
                    scanned += rows.len() as i64;
                    let emit = rows.len().min(remaining as usize);
                    let page_truncated = rows.len() > emit;
                    for row in &rows[..emit] {
                        matched += 1;
                        if matched > i64::from(i32::MAX) {
                            return Err(Status::out_of_range(
                                "stream sequence exceeds protocol range",
                            ));
                        }
                        if group_by.is_empty() {
                            last_cursor = Some(row_cursor(row, &order_by)?);
                        }
                        let mut split = row_to_split(row);
                        split.dimensions = split
                            .dimensions
                            .into_iter()
                            .map(|(physical, value)| {
                                (
                                    dimension_names.get(&physical).cloned().unwrap_or(physical),
                                    value,
                                )
                            })
                            .collect();
                        let response = StreamSplitsResponse {
                            split: Some(split),
                            sequence: matched as i32,
                            stats: None,
                            done: false,
                            cursor: if req.include_cursor {
                                last_cursor.clone()
                            } else {
                                None
                            },
                        };
                        send_response(&tx, response, timeout).await?;
                    }
                    if page_truncated || rows.len() < fetch_limit as usize {
                        truncated = page_truncated;
                        break;
                    }
                    if group_by.is_empty() {
                        let cursor = last_cursor
                            .as_ref()
                            .ok_or_else(|| Status::internal("missing page cursor"))?;
                        let values = cursor
                            .values
                            .iter()
                            .map(proto_cursor_value)
                            .collect::<Result<Vec<_>, _>>()?;
                        keyset_where = SplitQueryEngine::build_typed_keyset_where(
                            &order_by, &values, cursor.id,
                        )
                        .map_err(invalid)?;
                    } else {
                        offset += emit as u64;
                    }
                }
                send_response(
                    &tx,
                    StreamSplitsResponse {
                        split: None,
                        sequence: (matched + 1).min(i64::from(i32::MAX)) as i32,
                        stats: Some(QueryStats {
                            splits_scanned: scanned,
                            splits_matched: matched,
                            truncated,
                        }),
                        done: true,
                        cursor: if req.include_cursor {
                            last_cursor
                        } else {
                            None
                        },
                    },
                    timeout,
                )
                .await
            }
            .await;
            if let Err(status) = result {
                let _ = tokio::time::timeout(timeout, tx.send(Err(status))).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn send_response(
    tx: &mpsc::Sender<Result<StreamSplitsResponse, Status>>,
    response: StreamSplitsResponse,
    timeout: Duration,
) -> Result<(), Status> {
    tokio::time::timeout(timeout, tx.send(Ok(response)))
        .await
        .map_err(|_| Status::deadline_exceeded("stream idle timeout"))?
        .map_err(|_| Status::cancelled("client closed stream"))
}

fn proto_cursor_value(value: &CursorValue) -> Result<Value, Status> {
    match &value.value {
        Some(cursor_value::Value::IntVal(value)) => Ok(Value::from(*value)),
        Some(cursor_value::Value::FloatVal(value)) if value.is_finite() => Ok(Value::from(*value)),
        Some(cursor_value::Value::FloatVal(_)) => Err(invalid("cursor floats must be finite")),
        Some(cursor_value::Value::StrVal(value)) => Ok(Value::String(value.clone())),
        None => Ok(Value::Null),
    }
}

fn row_cursor(row: &SplitRow, order_by: &[OrderBySpec]) -> Result<KeysetCursor, Status> {
    let id = row
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(|| Status::out_of_range("row id cannot be represented in cursor"))?;
    let values = order_by
        .iter()
        .map(|order| {
            let value = row
                .get(&order.column)
                .ok_or_else(|| Status::internal("sort column missing from result"))?;
            let value = match value {
                Value::Null => None,
                Value::String(value) => Some(cursor_value::Value::StrVal(value.clone())),
                Value::Bool(value) => Some(cursor_value::Value::IntVal(i64::from(*value))),
                Value::Number(value) => {
                    if let Some(value) = value.as_i64() {
                        Some(cursor_value::Value::IntVal(value))
                    } else if value.is_f64() {
                        Some(cursor_value::Value::FloatVal(value.as_f64().unwrap()))
                    } else {
                        return Err(Status::out_of_range(
                            "sort value exceeds cursor integer range",
                        ));
                    }
                }
                _ => return Err(Status::internal("unsupported cursor value")),
            };
            Ok(CursorValue { value })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(KeysetCursor { values, id })
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
        if key.starts_with("dim_f") && !value.is_null() {
            dimensions.insert(
                key.clone(),
                value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            );
        }
    }

    Split {
        file: Some(file),
        dimensions,
        aggs: vec![], // Premium feature.
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

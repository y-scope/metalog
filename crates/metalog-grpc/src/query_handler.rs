use std::{collections::HashMap, sync::Arc};

use metalog_proto::query::{
    split_query_service_server::SplitQueryService,
    FileInfo,
    QueryStats,
    Split,
    StreamSplitsRequest,
    StreamSplitsResponse,
};
use metalog_db::validate_sql_identifier;
use metalog_query::{
    rewrite_filter_columns, validate_filter_expression, OrderBySpec, SplitQueryEngine, SplitRow,
};
use metalog_schema::ColumnRegistry;
use sqlx::MySqlPool;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// gRPC handler for the SplitQueryService (StreamSplits).
///
/// Holds a lazy-loaded registry cache keyed by table name so that
/// `__DIM.<key>` references in filter expressions can be resolved to
/// physical column names (e.g. `dim_f01`) before the query reaches the DB.
pub struct QueryHandler {
    engine: Arc<SplitQueryEngine>,
    db: MySqlPool,
    registries: Arc<RwLock<HashMap<String, Arc<ColumnRegistry>>>>,
}

impl QueryHandler {
    pub fn new(engine: Arc<SplitQueryEngine>, db: MySqlPool) -> Self {
        Self {
            engine,
            db,
            registries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Returns the `ColumnRegistry` for `table_name`, loading it from the DB
    /// on the first call and caching it for subsequent ones.
    async fn get_or_load_registry(
        &self,
        table_name: &str,
    ) -> Result<Arc<ColumnRegistry>, Status> {
        // Fast path: registry already cached.
        {
            let cache = self.registries.read().await;
            if let Some(reg) = cache.get(table_name) {
                return Ok(Arc::clone(reg));
            }
        }

        // Slow path: load from DB and cache.
        let registry = ColumnRegistry::new(self.db.clone(), table_name)
            .await
            .map_err(|e| Status::internal(format!("load registry for {table_name}: {e}")))?;
        let registry = Arc::new(registry);
        self.registries
            .write()
            .await
            .insert(table_name.to_string(), Arc::clone(&registry));
        Ok(registry)
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

        // Validate filter expression (raw, before __DIM rewrite).
        if !req.filter_expression.is_empty() {
            validate_filter_expression(&req.filter_expression)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }

        // Rewrite __DIM.<key> references to physical column names.
        // Load the registry for this table only when the filter contains __DIM references.
        let filter = if req.filter_expression.contains("__DIM.") {
            let registry = self.get_or_load_registry(&req.table).await?;
            rewrite_filter_columns(&req.filter_expression, Some(&registry))
                .map_err(|e| Status::invalid_argument(format!("filter column resolution: {e}")))?
        } else {
            req.filter_expression.clone()
        };

        // Extract and validate group_by columns.
        let group_by = req.group_by.clone();
        for col in &group_by {
            validate_sql_identifier(col)
                .map_err(|e| Status::invalid_argument(format!("invalid group_by column: {e}")))?;
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
        SplitQueryEngine::validate_sort_columns(
            &order_by,
            req.allow_unindexed_sort || !group_by.is_empty(),
        )
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Build ORDER BY clauses. When group_by is set, prepend group columns so
        // rows with the same group key are consecutive (streaming dedup).
        let order_clauses = if group_by.is_empty() {
            SplitQueryEngine::build_order_clauses(&order_by)
        } else {
            let mut clauses: Vec<String> = group_by
                .iter()
                .map(|c| format!("{} ASC", metalog_db::quote_identifier(c)))
                .collect();
            for ob in &order_by {
                let dir = if ob.desc { "DESC" } else { "ASC" };
                let clause = format!("{} {dir}", metalog_db::quote_identifier(&ob.column));
                if !clauses.contains(&clause) {
                    clauses.push(clause);
                }
            }
            // Add id tiebreaker for keyset pagination.
            clauses.push("`id` ASC".to_string());
            clauses
        };

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

        let columns = req.projection.clone();
        let table = req.table.clone();
        let limit = req.limit;

        let engine = self.engine.clone();

        // Parse aggregation specs from projection when group_by is active.
        let agg_specs: Vec<(String, Option<String>)> = columns
            .iter()
            .map(|c| {
                let col_name = SplitQueryEngine::extract_column_name(c).to_string();
                let func = SplitQueryEngine::parse_aggregate_func(c);
                (col_name, func)
            })
            .collect();

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
                    &group_by,
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

                    if group_by.is_empty() {
                        // No grouping — emit each row directly.
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
                                return;
                            }
                        }
                    } else {
                        // Streaming merge: consecutive rows with the same group key are merged.
                        let mut current_group: Option<SplitRow> = None;
                        let mut current_key = String::new();

                        for row in result_rows {
                            let row_key = group_by
                                .iter()
                                .map(|g| {
                                    row.get(g.as_str())
                                        .map(|v| v.to_string())
                                        .unwrap_or_default()
                                })
                                .collect::<Vec<_>>()
                                .join("|");

                            if row_key != current_key {
                                // Emit previous group.
                                if let Some(group) = current_group.take() {
                                    sequence += 1;
                                    let split = row_to_split(&group);
                                    if tx
                                        .send(Ok(StreamSplitsResponse {
                                            split: Some(split),
                                            sequence,
                                            stats: None,
                                            done: false,
                                            cursor: None,
                                        }))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                current_key = row_key;
                                current_group = Some(row.clone());
                            } else {
                                // Merge into current group.
                                if let Some(ref mut group) = current_group {
                                    merge_row_into_group(group, row, &agg_specs);
                                }
                            }
                        }

                        // Emit last group.
                        if let Some(group) = current_group {
                            sequence += 1;
                            let split = row_to_split(&group);
                            let _ = tx
                                .send(Ok(StreamSplitsResponse {
                                    split: Some(split),
                                    sequence,
                                    stats: None,
                                    done: false,
                                    cursor: None,
                                }))
                                .await;
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

/// Merges a new row into an existing group, applying aggregation functions.
fn merge_row_into_group(
    group: &mut SplitRow,
    row: &SplitRow,
    agg_specs: &[(String, Option<String>)],
) {
    for (col_name, func) in agg_specs {
        let Some(new_val) = row.get(col_name.as_str()) else {
            continue;
        };
        let Some(func) = func else {
            // No aggregate (group_by column or ANY_VALUE) — keep first value.
            continue;
        };

        let cur_val = group.get(col_name.as_str()).cloned();
        let merged = match func.as_str() {
            "MIN" => match (&cur_val, new_val) {
                (Some(c), n) if n.as_i64() < c.as_i64() => new_val.clone(),
                (None, _) => new_val.clone(),
                _ => cur_val.unwrap_or_else(|| new_val.clone()),
            },
            "MAX" => match (&cur_val, new_val) {
                (Some(c), n) if n.as_i64() > c.as_i64() => new_val.clone(),
                (None, _) => new_val.clone(),
                _ => cur_val.unwrap_or_else(|| new_val.clone()),
            },
            "SUM" | "COUNT" => {
                let cur = cur_val.and_then(|v| v.as_i64()).unwrap_or(0);
                let add = new_val.as_i64().unwrap_or(0);
                serde_json::json!(cur + add)
            }
            "AVG" => {
                // For streaming AVG we'd need count tracking. Fall back to last value.
                new_val.clone()
            }
            "ANY_VALUE" => {
                // Keep first value.
                cur_val.unwrap_or_else(|| new_val.clone())
            }
            _ => new_val.clone(),
        };
        group.insert(col_name.clone(), merged);
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
        aggs: vec![], // Premium feature.
    }
}

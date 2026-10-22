use std::sync::Arc;

use metalog_grpc::QueryHandler;
use metalog_it::helpers::setup_db_with_table;
use metalog_proto::query::{
    cursor_value,
    split_query_service_server::SplitQueryService,
    CursorValue,
    KeysetCursor,
    OrderBy,
    QueryStats,
    Split,
    StreamSplitsRequest,
};
use metalog_query::SplitQueryEngine;
use metalog_schema::ColumnRegistry;
use sqlx::MySqlPool;
use tokio_stream::StreamExt;
use tonic::{Code, Request};

fn handler(pool: &MySqlPool) -> QueryHandler {
    QueryHandler::new(Arc::new(SplitQueryEngine::new(pool.clone())), pool.clone())
}

fn request(table: &str) -> StreamSplitsRequest {
    StreamSplitsRequest {
        table: table.into(),
        projection: vec!["__FILE.*".into()],
        order_by: vec![OrderBy {
            column: "__FILE.min_timestamp".into(),
            order: 1,
        }],
        include_cursor: true,
        ..Default::default()
    }
}

async fn collect(
    handler: &QueryHandler,
    req: StreamSplitsRequest,
) -> (Vec<Split>, QueryStats, Option<KeysetCursor>) {
    let mut stream = handler
        .stream_splits(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let mut splits = Vec::new();
    let mut stats = None;
    let mut cursor = None;
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        if let Some(split) = response.split {
            splits.push(split);
        }
        if response.done {
            stats = response.stats;
            cursor = response.cursor;
        }
    }
    (splits, stats.expect("stream must end with stats"), cursor)
}

#[tokio::test]
async fn handler_paginates_and_resumes_without_gaps() {
    let (pool, _container) = setup_db_with_table("test_grpc_pages").await;
    sqlx::query(
        "INSERT INTO test_grpc_pages (min_timestamp,max_timestamp,state,record_count,expires_at) \
         SELECT seq,seq+1,'IR_CLOSED',7,0 FROM seq_1_to_1505",
    )
    .execute(&pool)
    .await
    .unwrap();
    let handler = handler(&pool);
    let mut req = request("test_grpc_pages");
    req.limit = 1002;
    // The requested projection omits the ordering column: internal cursor keys
    // and mandatory file fields must still be present.
    req.projection = vec!["__FILE.record_count".into()];
    let (first, stats, cursor) = collect(&handler, req.clone()).await;
    assert_eq!(first.len(), 1002);
    assert!(stats.truncated);
    assert_eq!(stats.splits_matched, 1002);
    assert_eq!(first[0].file.as_ref().unwrap().record_count, 7);
    assert_eq!(first[1001].file.as_ref().unwrap().min_timestamp, 1002);
    let cursor = cursor.expect("include_cursor must return a continuation");
    assert_eq!(cursor.values.len(), 1);
    req.cursor = Some(cursor);
    req.limit = 0;
    let (rest, stats, _) = collect(&handler, req).await;
    assert_eq!(rest.len(), 503);
    assert!(!stats.truncated);
    assert_eq!(rest[0].file.as_ref().unwrap().min_timestamp, 1003);
    assert_eq!(
        rest.last().unwrap().file.as_ref().unwrap().min_timestamp,
        1505
    );
}

#[tokio::test]
async fn unlimited_handler_stream_exceeds_previous_100k_cap() {
    let (pool, _container) = setup_db_with_table("test_grpc_unlimited").await;
    sqlx::query(
        "INSERT INTO test_grpc_unlimited (min_timestamp,max_timestamp,state,expires_at) SELECT \
         seq,seq+1,'IR_CLOSED',0 FROM seq_1_to_100005",
    )
    .execute(&pool)
    .await
    .unwrap();
    let handler = handler(&pool);
    let mut req = request("test_grpc_unlimited");
    req.projection = vec!["min_timestamp".into()];
    let mut stream = handler
        .stream_splits(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let mut count = 0i64;
    let mut final_stats = None;
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        if let Some(split) = response.split {
            count += 1;
            assert_eq!(split.file.unwrap().min_timestamp, count);
            assert!(response.cursor.is_some());
        }
        if response.done {
            final_stats = response.stats;
        }
    }
    assert_eq!(count, 100_005);
    let stats = final_stats.unwrap();
    assert_eq!(stats.splits_matched, count);
    assert!(!stats.truncated);
}

#[tokio::test]
async fn handler_groups_before_sorting_and_limiting() {
    let (pool, _container) = setup_db_with_table("test_grpc_groups").await;
    sqlx::query(
        "INSERT INTO test_grpc_groups \
         (min_timestamp,max_timestamp,state,archive_path,record_count,expires_at) VALUES \
         (1,9,'ARCHIVE_CLOSED','a',100,0),(2,10,'ARCHIVE_CLOSED','a',200,0),(3,20,'ARCHIVE_CLOSED'\
         ,'z',7,0),(4,21,'ARCHIVE_CLOSED','z',8,0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let handler = handler(&pool);
    let mut req = request("test_grpc_groups");
    req.projection = [
        "__FILE.archive_path",
        "MIN(__FILE.min_timestamp)",
        "MAX(__FILE.max_timestamp)",
        "SUM(__FILE.record_count)",
    ]
    .map(str::to_owned)
    .to_vec();
    req.group_by = vec!["__FILE.archive_path".into()];
    req.order_by = vec![OrderBy {
        column: "__FILE.max_timestamp".into(),
        order: 2,
    }];
    req.limit = 1;
    let (splits, stats, cursor) = collect(&handler, req.clone()).await;
    assert_eq!(splits.len(), 1);
    let file = splits[0].file.as_ref().unwrap();
    assert_eq!(file.archive_path, "z");
    assert_eq!(file.record_count, 15);
    assert_eq!(file.min_timestamp, 3);
    assert_eq!(file.max_timestamp, 21);
    assert!(stats.truncated);
    assert!(cursor.is_none());
    req.cursor = Some(KeysetCursor {
        values: vec![],
        id: 0,
    });
    assert_eq!(
        handler
            .stream_splits(Request::new(req))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn handler_resolves_dimensions_and_preserves_literal_text() {
    let (pool, _container) = setup_db_with_table("test_grpc_names").await;
    let handler = handler(&pool);
    // Query once before allocating dimensions: no stale registry may survive.
    collect(&handler, request("test_grpc_names")).await;
    let registry = ColumnRegistry::new(pool.clone(), "test_grpc_names")
        .await
        .unwrap();
    let column = registry
        .resolve_or_allocate_dim("service.name", "str_utf8", 128)
        .await
        .unwrap();
    let numeric = registry
        .resolve_or_allocate_dim("attempt", "int", 0)
        .await
        .unwrap();
    let literal = "日志/__DIM.fake/__FILE.min_timestamp";
    sqlx::query(&format!(
        "INSERT INTO test_grpc_names \
         (min_timestamp,max_timestamp,state,file_path,{column},{numeric},expires_at) VALUES \
         (1,2,'IR_CLOSED',?,'prod',42,0)"
    ))
    .bind(literal)
    .execute(&pool)
    .await
    .unwrap();
    let mut req = request("test_grpc_names");
    req.projection = vec!["__FILE.file_path".into(), "__DIM.*".into()];
    req.filter_expression =
        format!("__FILE.file_path = '{literal}' AND __DIM.service.name = 'prod'");
    req.order_by = vec![OrderBy {
        column: "__DIM.service.name".into(),
        order: 1,
    }];
    req.allow_unindexed_sort = true;
    let (splits, _, cursor) = collect(&handler, req).await;
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].file.as_ref().unwrap().file_path, literal);
    assert_eq!(
        splits[0].dimensions.get("service.name").map(String::as_str),
        Some("prod")
    );
    assert_eq!(
        splits[0].dimensions.get("attempt").map(String::as_str),
        Some("42")
    );
    assert!(cursor.is_some());

    sqlx::query(
        "UPDATE _dim_registry SET alias_column = 'service_alias' WHERE table_name = \
         'test_grpc_names' AND dim_key = 'service.name'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut alias_request = request("test_grpc_names");
    alias_request.projection = vec!["__DIM.service_alias".into()];
    alias_request.filter_expression = "__DIM.service_alias = 'prod'".into();
    let (aliased, ..) = collect(&handler, alias_request).await;
    assert_eq!(aliased.len(), 1);
    assert_eq!(
        aliased[0]
            .dimensions
            .get("service.name")
            .map(String::as_str),
        Some("prod")
    );
}

#[tokio::test]
async fn handler_rejects_malformed_cursors_and_query_syntax() {
    let (pool, _container) = setup_db_with_table("test_grpc_validation").await;
    let handler = handler(&pool);
    for values in [
        vec![],
        vec![CursorValue {
            value: Some(cursor_value::Value::FloatVal(f64::NAN)),
        }],
        vec![CursorValue { value: None }, CursorValue { value: None }],
    ] {
        let mut req = request("test_grpc_validation");
        req.cursor = Some(KeysetCursor { values, id: 1 });
        assert_eq!(
            handler
                .stream_splits(Request::new(req))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    for filter in [
        "1=1; DROP TABLE test_grpc_validation",
        "1=1 ORDER BY id",
        "file_path = ?",
    ] {
        let mut req = request("test_grpc_validation");
        req.filter_expression = filter.into();
        assert_eq!(
            handler
                .stream_splits(Request::new(req))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    let mut req = request("test_grpc_validation");
    req.projection = vec!["bad); DROP TABLE x;--".into()];
    assert_eq!(
        handler
            .stream_splits(Request::new(req))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn handler_cursors_follow_database_null_ordering_in_both_directions() {
    let (pool, _container) = setup_db_with_table("test_grpc_null_order").await;
    sqlx::query(
        "INSERT INTO test_grpc_null_order \
         (min_timestamp,max_timestamp,state,archive_path,expires_at) VALUES \
         (1,2,'IR_CLOSED',NULL,0),(2,3,'IR_CLOSED','a',0),(3,4,'IR_CLOSED','b',0),(4,5,'IR_CLOSED'\
         ,NULL,0)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let handler = handler(&pool);
    for (direction, expected) in [(1, vec![1, 4, 2, 3]), (2, vec![3, 2, 4, 1])] {
        let mut req = request("test_grpc_null_order");
        req.order_by = vec![OrderBy {
            column: "archive_path".into(),
            order: direction,
        }];
        req.allow_unindexed_sort = true;
        req.limit = 1;
        let mut actual = Vec::new();
        loop {
            let (splits, stats, cursor) = collect(&handler, req.clone()).await;
            actual.extend(splits.into_iter().map(|s| s.file.unwrap().min_timestamp));
            if !stats.truncated {
                break;
            }
            req.cursor = cursor;
            assert!(actual.len() <= 4, "cursor repeated a row");
        }
        assert_eq!(actual, expected);
    }
}

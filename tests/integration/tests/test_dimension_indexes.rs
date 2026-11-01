use std::{sync::Arc, time::Duration};

use metalog_coordinator::{RegisterTableOpts, TableRegistration};
use metalog_it::helpers::setup_db_with_table;
use metalog_schema::{ColumnRegistry, IndexManager};
use sqlx::{Executor, MySqlPool, Row};

async fn indexes(pool: &MySqlPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = \
         DATABASE() AND TABLE_NAME = 'indexed_fields'",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}
async fn configure(pool: &MySqlPool, fields: &[&str]) {
    TableRegistration::new(pool.clone(), "")
        .register_table(
            "indexed_fields",
            "",
            RegisterTableOpts {
                config_json: Some(serde_json::json!({"indexes":fields}).to_string()),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn configured_indexes_reconcile_on_start_and_use_real_query_plans() {
    let (pool, db) = setup_db_with_table("indexed_fields").await;
    configure(&pool, &["region", "latency", "future"]).await;
    let manager = IndexManager::new(pool.clone());
    manager.reconcile("indexed_fields").await.unwrap(); // fields can arrive later
    let registry = ColumnRegistry::new(pool.clone(), "indexed_fields")
        .await
        .unwrap();
    let region = registry
        .resolve_or_allocate_dim("region", "str_utf8", 512)
        .await
        .unwrap();
    let latency = registry
        .resolve_or_allocate_dim("latency", "int", 0)
        .await
        .unwrap();
    let sql = format!(
        "INSERT INTO indexed_fields \
         (min_timestamp,max_timestamp,expires_at,state,`{region}`,`{latency}`) SELECT \
         seq,seq,0,'IR_CLOSED',CONCAT('region-',seq),seq FROM seq_1_to_5000"
    );
    pool.execute(sql.as_str()).await.unwrap();
    // Coordinator startup must apply configured indexes without an external DDL call.
    let resources = Arc::new(metalog_node::Resources::new(
        pool.clone(),
        true,
        Duration::from_secs(60),
    ));
    let mut node = metalog_node::NodeBuilder::new(
        serde_json::from_value(serde_json::json!({"database":{"primary":{"database":"test"}}}))
            .unwrap(),
        resources.clone(),
    )
    .build();
    let mut lock =
        metalog_metastore::AdvisoryLock::acquire_scoped(&pool, "dim_alloc", "indexed_fields", 10)
            .await
            .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        node.start(&["indexed_fields".into()]),
    )
    .await
    .expect("coordinator startup must not block on index construction")
    .unwrap();
    lock.release().await.unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if indexes(&pool)
                .await
                .contains(&format!("idx_metalog_{latency}"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("startup index reconciliation");
    let unit = resources
        .coordinator("indexed_fields")
        .expect("coordinator startup succeeded");
    for column in [&region, &latency] {
        assert!(indexes(&pool)
            .await
            .contains(&format!("idx_metalog_{column}")));
    }
    pool.execute("ANALYZE TABLE indexed_fields").await.unwrap();
    for (column, literal) in [(&region, "'region-1234'"), (&latency, "1234")] {
        let sql = format!("EXPLAIN SELECT id FROM indexed_fields WHERE `{column}` = {literal}");
        let plan = sqlx::query(&sql).fetch_one(&pool).await.unwrap();
        let key: Option<String> = plan.try_get("key").unwrap();
        assert_eq!(
            key.as_deref(),
            Some(format!("idx_metalog_{column}").as_str()),
            "{plan:?}"
        );
    }
    let readers = metalog_db::new_pool(&db.reader_config()).await.unwrap();
    let engine = Arc::new(metalog_query::SplitQueryEngine::new(readers.clone()));
    assert!(engine
        .has_full_column_index("indexed_fields", &latency)
        .await
        .unwrap());
    assert!(!engine
        .has_full_column_index("indexed_fields", &region)
        .await
        .unwrap());
    // The gRPC sort validator recognizes a configured full-column index.
    use metalog_proto::query::{
        split_query_service_server::SplitQueryService,
        OrderBy,
        StreamSplitsRequest,
    };
    use tokio_stream::StreamExt;
    let handler = metalog_grpc::QueryHandler::new(engine, readers);
    let request = StreamSplitsRequest {
        table: "indexed_fields".into(),
        order_by: vec![OrderBy {
            column: "__DIM.latency".into(),
            order: 1,
        }],
        limit: 1,
        ..Default::default()
    };
    let mut stream = handler
        .stream_splits(tonic::Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert!(stream.next().await.unwrap().unwrap().split.is_some());
    drop(stream);
    let mut request = request;
    request.order_by[0].column = "__DIM.region".into();
    assert!(handler
        .stream_splits(tonic::Request::new(request))
        .await
        .is_err());
    let prefix: Option<i64> = sqlx::query_scalar(
        "SELECT SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND \
         TABLE_NAME='indexed_fields' AND INDEX_NAME=?",
    )
    .bind(format!("idx_metalog_{region}"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prefix, Some(191));
    // Prefix collisions retain exact SQL equality semantics.
    let common = "é".repeat(191);
    let sql = format!(
        "INSERT INTO indexed_fields (min_timestamp,max_timestamp,expires_at,state,`{region}`) \
         VALUES (6001,6001,0,'IR_CLOSED',?),(6002,6002,0,'IR_CLOSED',?)"
    );
    sqlx::query(&sql)
        .bind(format!("{common}a"))
        .bind(format!("{common}b"))
        .execute(&pool)
        .await
        .unwrap();
    let sql = format!("SELECT COUNT(*) FROM indexed_fields WHERE `{region}`=?");
    let count: i64 = sqlx::query_scalar(&sql)
        .bind(format!("{common}a"))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let late = registry
        .resolve_or_allocate_dim("future", "bool", 0)
        .await
        .unwrap();
    unit.reconcile_indexes().await.unwrap(); // same path as the periodic task
    assert!(indexes(&pool)
        .await
        .contains(&format!("idx_metalog_{late}")));
    pool.execute("ALTER TABLE indexed_fields ADD INDEX idx_manual_state(state)")
        .await
        .unwrap();
    let before = indexes(&pool).await;
    configure(&pool, &[]).await;
    // Simultaneous coordinators serialize DDL and read current config under the lock.
    let (a, b) = tokio::join!(
        manager.reconcile("indexed_fields"),
        unit.reconcile_indexes()
    );
    a.unwrap();
    b.unwrap();
    let after = indexes(&pool).await;
    assert!(!after.iter().any(|n| n.starts_with("idx_metalog_")));
    for name in before.iter().filter(|n| !n.starts_with("idx_metalog_")) {
        assert!(after.contains(name), "removed unmanaged {name}");
    }
    assert!(after.contains(&"PRIMARY".into()));
    node.stop().await;
    // Invalid configuration is rejected before modifying the existing config.
    for fields in [vec!["region", "region"], vec![""]] {
        let result = TableRegistration::new(pool.clone(), "")
            .register_table(
                "indexed_fields",
                "",
                RegisterTableOpts {
                    config_json: Some(serde_json::json!({"indexes":fields}).to_string()),
                },
            )
            .await;
        assert!(result.is_err());
    }
}

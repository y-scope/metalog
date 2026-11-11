use std::collections::HashSet;

use metalog_aggs::AggRegistry;
use metalog_coordinator::{RegisterTableOpts, TableRegistration};
use metalog_it::helpers::setup_db;
use metalog_schema::{ensure_table, ColumnRegistry};
use sqlx::MySqlPool;

async fn storage_ids(db: &MySqlPool, table: &str) -> Vec<(u64, u64)> {
    sqlx::query_as(
        "SELECT CAST(TABLE_ID AS UNSIGNED), CAST(SPACE AS UNSIGNED) FROM \
         information_schema.INNODB_SYS_TABLES WHERE NAME LIKE CONCAT(DATABASE(), '/', ?, '#%') \
         ORDER BY NAME",
    )
    .bind(table)
    .fetch_all(db)
    .await
    .unwrap()
}

#[tokio::test]
async fn default_reserves_claim_all_types_without_rebuilding_populated_table() {
    let (db, container) = setup_db().await;
    // Inspect storage with an independent admin witness (requires PROCESS).
    // Provisioning, allocation and data access still use the application pool.
    let witness = container.direct_pool().await;
    ensure_table(&db, "reserved", None).await.unwrap();
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM _dim_registry WHERE table_name='reserved' AND \
         state='AVAILABLE'), (SELECT COUNT(*) FROM _agg_registry WHERE table_name='reserved' AND \
         state='AVAILABLE')",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(counts, (40, 24));
    sqlx::query(
        "INSERT INTO reserved (min_timestamp, file_path, expires_at) VALUES (1, 'before', 0)",
    )
    .execute(&db)
    .await
    .unwrap();
    let before = storage_ids(&witness, "reserved").await;
    assert!(!before.is_empty());
    let dims = ColumnRegistry::new(db.clone(), "reserved").await.unwrap();
    let aggs = AggRegistry::new(db.clone(), "reserved").await.unwrap();
    assert_eq!(dims.entry_count(), 0);
    assert!(aggs.all_entries().await.is_empty());
    for kind in ["str", "str_utf8", "int", "float", "bool"] {
        dims.resolve_or_allocate_dim(kind, kind, 64).await.unwrap();
    }
    let count = aggs
        .resolve_or_allocate_agg("count", "", "SUM", "INT")
        .await
        .unwrap();
    let sum = aggs
        .resolve_or_allocate_agg("sum", "", "SUM", "FLOAT")
        .await
        .unwrap();
    assert_eq!(dims.entry_count(), 5);
    assert_eq!(aggs.all_entries().await.len(), 2);
    sqlx::query(&format!(
        "INSERT INTO reserved (min_timestamp, file_path, expires_at, `{count}`, `{sum}`) VALUES \
         (2, 'after', 0, 42, 1.5)"
    ))
    .execute(&db)
    .await
    .unwrap();
    let values: (i64, f64) = sqlx::query_as(&format!(
        "SELECT `{count}`, `{sum}` FROM reserved WHERE file_path_hash=UNHEX(MD5('after'))"
    ))
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(values, (42, 1.5));
    assert_eq!(before, storage_ids(&witness, "reserved").await);
    assert_eq!(
        ColumnRegistry::new(db.clone(), "reserved")
            .await
            .unwrap()
            .entry_count(),
        5
    );
    assert_eq!(
        AggRegistry::new(db, "reserved")
            .await
            .unwrap()
            .all_entries()
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn best_fit_width_exhaustion_and_invalidated_slots_never_trigger_ddl() {
    let (db, container) = setup_db().await;
    let witness = container.direct_pool().await;
    let config = r#"{"column_reserves":{"dimensions":[
        {"type":"str","count":1,"width":1024},
        {"type":"str","count":1,"width":256},
        {"type":"int","count":1}
    ],"aggregates":[{"type":"float","count":1}]}}"#;
    ensure_table(&db, "bounded", Some(config)).await.unwrap();
    sqlx::query("INSERT INTO bounded (min_timestamp, file_path, expires_at) VALUES (1, 'old', 0)")
        .execute(&db)
        .await
        .unwrap();
    let before = storage_ids(&witness, "bounded").await;
    let dims = ColumnRegistry::new(db.clone(), "bounded").await.unwrap();
    assert_eq!(
        dims.resolve_or_allocate_dim("small", "str", 32)
            .await
            .unwrap(),
        "dim_f02"
    );
    assert!(dims
        .resolve_or_allocate_dim("small", "str", 300)
        .await
        .unwrap_err()
        .to_string()
        .contains("reserved width"));
    assert_eq!(
        dims.resolve_or_allocate_dim("large", "str", 900)
            .await
            .unwrap(),
        "dim_f01"
    );
    assert!(dims
        .resolve_or_allocate_dim("extra", "str", 32)
        .await
        .unwrap_err()
        .to_string()
        .contains("no reserved"));
    assert!(dims
        .resolve_or_allocate_dim("utf8", "str_utf8", 32)
        .await
        .is_err());
    dims.resolve_or_allocate_dim("number", "int", 0)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE _dim_registry SET state='INVALIDATED' WHERE table_name='bounded' AND \
         dim_key='number'",
    )
    .execute(&db)
    .await
    .unwrap();
    let restarted = ColumnRegistry::new(db.clone(), "bounded").await.unwrap();
    assert!(restarted
        .resolve_or_allocate_dim("new_number", "int", 0)
        .await
        .is_err());
    let aggs = AggRegistry::new(db.clone(), "bounded").await.unwrap();
    assert!(aggs
        .resolve_or_allocate_agg("x", "", "SUM", "INT")
        .await
        .is_err());
    aggs.resolve_or_allocate_agg("x", "", "SUM", "FLOAT")
        .await
        .unwrap();
    assert!(aggs
        .resolve_or_allocate_agg("y", "", "SUM", "FLOAT")
        .await
        .is_err());
    assert!(aggs
        .resolve_or_allocate_agg("x", "", "SUM", "INT")
        .await
        .is_err());
    assert!(!ensure_table(&db, "bounded", Some(config)).await.unwrap());
    assert!(ColumnRegistry::new(db.clone(), "bounded")
        .await
        .unwrap()
        .resolve_or_allocate_dim("new_number", "int", 0)
        .await
        .is_err());
    assert_eq!(before, storage_ids(&witness, "bounded").await);
}

#[tokio::test]
async fn independent_registries_claim_each_slot_and_key_once() {
    let (db, _container) = setup_db().await;
    let config = r#"{"column_reserves":{
        "dimensions":[{"type":"int","count":8}],
        "aggregates":[{"type":"int","count":8}]
    }}"#;
    ensure_table(&db, "concurrent", Some(config)).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..16 {
        let dims = ColumnRegistry::new(db.clone(), "concurrent").await.unwrap();
        let aggs = AggRegistry::new(db.clone(), "concurrent").await.unwrap();
        tasks.spawn(async move {
            let key = format!("key{}", i % 8);
            (
                key.clone(),
                dims.resolve_or_allocate_dim(&key, "int", 0).await.unwrap(),
                aggs.resolve_or_allocate_agg(&key, "", "SUM", "INT")
                    .await
                    .unwrap(),
            )
        });
    }
    let mut mappings = std::collections::HashMap::new();
    while let Some(result) = tasks.join_next().await {
        let (key, dim, agg) = result.unwrap();
        if let Some(previous) = mappings.insert(key, (dim.clone(), agg.clone())) {
            assert_eq!(previous, (dim, agg));
        }
    }
    assert_eq!(
        mappings
            .values()
            .map(|x| &x.0)
            .collect::<HashSet<_>>()
            .len(),
        8
    );
    assert_eq!(
        mappings
            .values()
            .map(|x| &x.1)
            .collect::<HashSet<_>>()
            .len(),
        8
    );
}

#[tokio::test]
async fn configuration_is_creation_only_but_other_settings_remain_mutable() {
    let (db, _container) = setup_db().await;
    let registration = TableRegistration::new(db.clone(), "");
    registration
        .register_table("immutable", "", RegisterTableOpts::default())
        .await
        .unwrap();
    let dims = ColumnRegistry::new(db.clone(), "immutable").await.unwrap();
    dims.resolve_or_allocate_dim("host", "str", 64)
        .await
        .unwrap();
    registration
        .register_table(
            "immutable",
            "",
            RegisterTableOpts {
                config_json: Some(r#"{"indexes":["host"]}"#.into()),
            },
        )
        .await
        .unwrap();
    for patch in [
        r#"{"column_reserves":null}"#,
        r#"{"column_reserves":{"dimensions":[],"aggregates":[]}}"#,
    ] {
        assert!(ensure_table(&db, "immutable", Some(patch)).await.is_err());
        assert!(registration
            .register_table(
                "immutable",
                "",
                RegisterTableOpts {
                    config_json: Some(patch.into())
                }
            )
            .await
            .is_err());
    }
    let json: String =
        sqlx::query_scalar("SELECT config FROM _table_config WHERE table_name='immutable'")
            .fetch_one(&db)
            .await
            .unwrap();
    let config = metalog_metastore::decode_table_config(Some(&json)).unwrap();
    assert_eq!(config.column_reserves, Some(Default::default()));
    assert_eq!(config.indexes, ["host"]);
    assert_eq!(
        ColumnRegistry::new(db, "immutable")
            .await
            .unwrap()
            .entry_count(),
        1
    );
}

#[tokio::test]
async fn explicit_opt_out_and_legacy_configs_preserve_dynamic_allocation() {
    let (db, _container) = setup_db().await;
    TableRegistration::new(db.clone(), "")
        .register_table(
            "dynamic",
            "",
            RegisterTableOpts {
                config_json: Some(r#"{"column_reserves":null}"#.into()),
            },
        )
        .await
        .unwrap();
    let dims = ColumnRegistry::new(db.clone(), "dynamic").await.unwrap();
    assert_eq!(
        dims.resolve_or_allocate_dim("number", "int", 0)
            .await
            .unwrap(),
        "dim_f01"
    );
    // A pre-feature table has no creation-time setting at all.
    sqlx::query("UPDATE _table_config SET config='{}' WHERE table_name='dynamic'")
        .execute(&db)
        .await
        .unwrap();
    ensure_table(&db, "dynamic", None).await.unwrap();
    let dims = ColumnRegistry::new(db.clone(), "dynamic").await.unwrap();
    assert_eq!(
        dims.resolve_or_allocate_dim("string", "str", 64)
            .await
            .unwrap(),
        "dim_f02"
    );
    assert!(
        ensure_table(&db, "dynamic", Some(r#"{"column_reserves":{}}"#))
            .await
            .is_err()
    );
    let aggs = AggRegistry::new(db.clone(), "dynamic").await.unwrap();
    let stale_aggs = AggRegistry::new(db.clone(), "dynamic").await.unwrap();
    assert_eq!(
        aggs.resolve_or_allocate_agg("first", "", "SUM", "INT")
            .await
            .unwrap(),
        "agg_f01"
    );
    // Dynamic allocation must also avoid DDL crash orphans and retired slots.
    sqlx::query("ALTER TABLE dynamic ADD COLUMN agg_f02 BIGINT NULL")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("UPDATE _agg_registry SET state='INVALIDATED' WHERE table_name='dynamic'")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        stale_aggs
            .resolve_or_allocate_agg("second", "", "SUM", "FLOAT")
            .await
            .unwrap(),
        "agg_f03"
    );
}

#[tokio::test]
async fn incomplete_provisioning_recovers_with_a_single_connection() {
    let (db, container) = setup_db().await;
    let witness = container.direct_pool().await;
    ensure_table(&db, "recover", None).await.unwrap();
    let before = storage_ids(&witness, "recover").await;
    // Simulate a crash after CREATE TABLE but before publishing its registries.
    for statement in [
        "UPDATE _table SET active=FALSE WHERE table_name='recover'",
        "DELETE FROM _table_assignment WHERE table_name='recover'",
        "DELETE FROM _dim_registry WHERE table_name='recover'",
        "DELETE FROM _agg_registry WHERE table_name='recover'",
    ] {
        sqlx::query(statement).execute(&db).await.unwrap();
    }
    let single = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(db.connect_options().as_ref().clone())
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        ensure_table(&single, "recover", None).await.unwrap();
        ColumnRegistry::new(single.clone(), "recover")
            .await
            .unwrap()
            .resolve_or_allocate_dim("host", "str", 64)
            .await
            .unwrap();
        AggRegistry::new(single.clone(), "recover")
            .await
            .unwrap()
            .resolve_or_allocate_agg("count", "", "SUM", "INT")
            .await
            .unwrap();
    })
    .await
    .unwrap();
    assert_eq!(before, storage_ids(&witness, "recover").await);
    let active: bool = sqlx::query_scalar("SELECT active FROM _table WHERE table_name='recover'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert!(active);
    // Idempotent registration must not re-enable an intentionally disabled table.
    sqlx::query("UPDATE _table SET active=FALSE WHERE table_name='recover'")
        .execute(&db)
        .await
        .unwrap();
    ensure_table(&db, "recover", None).await.unwrap();
    let active: bool = sqlx::query_scalar("SELECT active FROM _table WHERE table_name='recover'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert!(!active);
}

#[tokio::test]
async fn invalid_reserves_are_rejected_before_registration() {
    let (db, _container) = setup_db().await;
    for group in [
        r#"{"type":"int","count":100}"#,
        r#"{"type":"int","count":0}"#,
        r#"{"type":"str","count":1,"width":255}"#,
        r#"{"type":"int","count":1,"width":256}"#,
        r#"{"type":"str_utf8","count":99,"width":16383}"#,
        r#"{"type":"unknown","count":1}"#,
    ] {
        let config = format!(r#"{{"column_reserves":{{"dimensions":[{group}]}}}}"#);
        assert!(ensure_table(&db, "invalid", Some(&config)).await.is_err());
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _table WHERE table_name='invalid'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use metalog_it::helpers::setup_db;
    use metalog_schema::{ensure_table, ColumnRegistry};

    // ── ensure_table ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ensure_table_returns_true_on_first_call() {
        let (pool, _c) = setup_db().await;

        let created = ensure_table(&pool, "test_schema_new", None).await.unwrap();
        assert!(created, "first ensure_table should return true");
    }

    #[tokio::test]
    async fn ensure_table_returns_false_on_second_call() {
        let (pool, _c) = setup_db().await;

        ensure_table(&pool, "test_schema_idem", None).await.unwrap();
        let second = ensure_table(&pool, "test_schema_idem", None).await.unwrap();
        assert!(!second, "repeated ensure_table must return false");
    }

    #[tokio::test]
    async fn ensure_table_registers_in_system_tables() {
        let (pool, _c) = setup_db().await;
        ensure_table(&pool, "test_schema_reg", None).await.unwrap();

        let in_registry: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM _table WHERE table_name = 'test_schema_reg'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(in_registry, 1);

        let has_config: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM _table_config WHERE table_name = 'test_schema_reg'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(has_config, 1);

        let has_assignment: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM _table_assignment WHERE table_name = 'test_schema_reg'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(has_assignment, 1);
    }

    #[tokio::test]
    async fn ensure_table_creates_physical_table() {
        let (pool, _c) = setup_db().await;
        ensure_table(&pool, "test_schema_phys", None).await.unwrap();

        // If the table exists we can INSERT into it.
        sqlx::query(
            "INSERT INTO `test_schema_phys` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES (1, 2, 'IR_BUFFERING', 1, 30, 0)",
        )
        .execute(&pool)
        .await
        .expect("physical table must exist and accept rows");
    }

    #[tokio::test]
    async fn ensure_table_rejects_invalid_name() {
        let (pool, _c) = setup_db().await;

        let result = ensure_table(&pool, "'; DROP TABLE _table; --", None).await;
        assert!(result.is_err(), "SQL-injection name must be rejected");
    }

    #[tokio::test]
    async fn ensure_table_stores_config_json() {
        let (pool, _c) = setup_db().await;
        let cfg = r#"{"consolidation":{"enabled":true}}"#;
        ensure_table(&pool, "test_schema_cfg", Some(cfg))
            .await
            .unwrap();

        let stored: String = sqlx::query_scalar(
            "SELECT config FROM _table_config WHERE table_name = 'test_schema_cfg'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(stored, cfg);
    }

    // ── ColumnRegistry concurrent allocation ─────────────────────────────────

    /// Spawns N concurrent tasks that each allocate a *distinct* dim key via the
    /// same registry instance. Every task must get a unique physical column slot.
    #[tokio::test]
    async fn concurrent_dim_allocation_no_duplicate_slots() {
        let (pool, _c) = setup_db().await;
        ensure_table(&pool, "test_schema_concurrent_dim", None)
            .await
            .unwrap();

        let registry = Arc::new(
            ColumnRegistry::new(pool.clone(), "test_schema_concurrent_dim")
                .await
                .unwrap(),
        );

        const N: usize = 10;
        let mut set = tokio::task::JoinSet::new();

        for i in 0..N {
            let reg = Arc::clone(&registry);
            set.spawn(async move {
                let key = format!("dim_key_{i}");
                reg.resolve_or_allocate_dim(&key, "str", 64).await.unwrap()
            });
        }

        let mut slots: HashSet<String> = HashSet::new();
        while let Some(res) = set.join_next().await {
            let col = res.expect("task must not panic");
            assert!(slots.insert(col.clone()), "duplicate slot assigned: {col}");
        }

        assert_eq!(slots.len(), N, "each dim key must get its own slot");
    }

    /// Same key allocated concurrently many times should resolve to the same slot.
    #[tokio::test]
    async fn concurrent_same_key_returns_same_slot() {
        let (pool, _c) = setup_db().await;
        ensure_table(&pool, "test_schema_same_key", None)
            .await
            .unwrap();

        let registry = Arc::new(
            ColumnRegistry::new(pool.clone(), "test_schema_same_key")
                .await
                .unwrap(),
        );

        const N: usize = 8;
        let mut set = tokio::task::JoinSet::new();

        for _ in 0..N {
            let reg = Arc::clone(&registry);
            set.spawn(async move {
                reg.resolve_or_allocate_dim("shared_key", "str", 64)
                    .await
                    .unwrap()
            });
        }

        let mut slots: HashSet<String> = HashSet::new();
        while let Some(res) = set.join_next().await {
            slots.insert(res.expect("task must not panic"));
        }

        assert_eq!(slots.len(), 1, "all tasks must resolve to the same slot");
        assert_eq!(slots.iter().next().unwrap(), "dim_f01");
    }

    /// Allocating keys sequentially should assign slots in order dim_f01, dim_f02, …
    #[tokio::test]
    async fn sequential_allocation_assigns_ordered_slots() {
        let (pool, _c) = setup_db().await;
        ensure_table(&pool, "test_schema_seq", None).await.unwrap();

        let registry = ColumnRegistry::new(pool.clone(), "test_schema_seq")
            .await
            .unwrap();

        for i in 1..=5usize {
            let col = registry
                .resolve_or_allocate_dim(&format!("key_{i}"), "str", 64)
                .await
                .unwrap();
            assert_eq!(col, format!("dim_f{i:02}"), "slot {i} must be dim_f{i:02}");
        }
    }
}

#[tokio::test]
async fn provisioning_repairs_a_partial_registration_with_one_connection() {
    let (pool, _container) = metalog_it::helpers::setup_db().await;
    sqlx::query(
        "INSERT INTO _table(table_name, display_name, active) VALUES('partial_table', 'partial', \
         TRUE)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let single = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        metalog_schema::ensure_table(&single, "partial_table", None),
    )
    .await
    .unwrap()
    .unwrap();
    let configs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _table_config WHERE table_name = 'partial_table'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(configs, 1);
    sqlx::query("SELECT id FROM partial_table LIMIT 1")
        .fetch_optional(&pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn table_names_at_mysql_identifier_limit_can_be_provisioned() {
    let (pool, _db) = metalog_it::helpers::setup_db().await;
    let name = "t".repeat(64);
    metalog_schema::ensure_table(&pool, &name, None)
        .await
        .unwrap();
    let registry = metalog_schema::ColumnRegistry::new(pool.clone(), &name)
        .await
        .unwrap();
    registry
        .resolve_or_allocate_dim("host", "str", 64)
        .await
        .unwrap();
    metalog_schema::PartitionManager::new(pool, &name, 7)
        .run_maintenance()
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_schema::PartitionManager;

    /// Returns the count of named daily partitions (excludes p_floor, p_future).
    async fn count_named_partitions(pool: &sqlx::MySqlPool, table: &str) -> u64 {
        let db_name: String = sqlx::query_scalar("SELECT DATABASE()")
            .fetch_one(pool)
            .await
            .unwrap();

        sqlx::query_scalar(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             AND PARTITION_NAME NOT IN ('p_floor', 'p_future')",
        )
        .bind(&db_name)
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    // ── ensure_lookahead_partitions ──────────────────────────────────────────

    #[tokio::test]
    async fn lookahead_creates_seven_daily_partitions() {
        let (pool, _c) = setup_db_with_table("test_part_lookahead").await;

        let mgr = PartitionManager::new(pool.clone(), "test_part_lookahead", 7);
        let created = mgr.ensure_lookahead_partitions().await.unwrap();

        // Should have created exactly 7 named partitions.
        assert_eq!(created, 7, "expected 7 lookahead partitions");
        let count = count_named_partitions(&pool, "test_part_lookahead").await;
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn lookahead_is_idempotent() {
        let (pool, _c) = setup_db_with_table("test_part_idem").await;

        let mgr = PartitionManager::new(pool.clone(), "test_part_idem", 7);

        // First call creates partitions.
        let first = mgr.ensure_lookahead_partitions().await.unwrap();
        assert_eq!(first, 7);

        // Second call: all partitions already exist, created = 0.
        let second = mgr.ensure_lookahead_partitions().await.unwrap();
        assert_eq!(second, 0, "second call should create 0 new partitions");

        // Total partition count unchanged.
        let count = count_named_partitions(&pool, "test_part_idem").await;
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn custom_lookahead_count_respected() {
        let (pool, _c) = setup_db_with_table("test_part_custom").await;

        let mgr = PartitionManager::new(pool.clone(), "test_part_custom", 3);
        let created = mgr.ensure_lookahead_partitions().await.unwrap();

        assert_eq!(created, 3);
        let count = count_named_partitions(&pool, "test_part_custom").await;
        assert_eq!(count, 3);
    }

    // ── run_maintenance ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_maintenance_succeeds_and_creates_partitions() {
        let (pool, _c) = setup_db_with_table("test_part_maint").await;

        let mgr = PartitionManager::new(pool.clone(), "test_part_maint", 7);
        // run_maintenance acquires advisory lock internally.
        mgr.run_maintenance().await.unwrap();

        let count = count_named_partitions(&pool, "test_part_maint").await;
        assert_eq!(count, 7);
    }

    #[tokio::test]
    async fn run_maintenance_twice_is_idempotent() {
        let (pool, _c) = setup_db_with_table("test_part_maint2").await;

        let mgr = PartitionManager::new(pool.clone(), "test_part_maint2", 7);
        mgr.run_maintenance().await.unwrap();
        mgr.run_maintenance().await.unwrap();

        let count = count_named_partitions(&pool, "test_part_maint2").await;
        assert_eq!(count, 7, "partition count must not change on second maintenance");
    }

    // ── IndexManager ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn ensure_index_creates_and_is_idempotent() {
        let (pool, _c) = setup_db_with_table("test_part_index").await;
        let mgr = metalog_schema::IndexManager::new(pool.clone());

        // First call creates the index.
        mgr.ensure_index("test_part_index", "min_timestamp")
            .await
            .unwrap();

        // Second call should be a no-op (duplicate key silenced).
        mgr.ensure_index("test_part_index", "min_timestamp")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn drop_index_and_recreate() {
        let (pool, _c) = setup_db_with_table("test_part_drop_idx").await;
        let mgr = metalog_schema::IndexManager::new(pool.clone());

        mgr.ensure_index("test_part_drop_idx", "max_timestamp")
            .await
            .unwrap();
        mgr.drop_index("test_part_drop_idx", "max_timestamp")
            .await
            .unwrap();

        // Drop again — should be a no-op.
        mgr.drop_index("test_part_drop_idx", "max_timestamp")
            .await
            .unwrap();

        // Can recreate after dropping.
        mgr.ensure_index("test_part_drop_idx", "max_timestamp")
            .await
            .unwrap();
    }
}

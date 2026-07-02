#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_schema::{IndexManager, PartitionManager};

    #[tokio::test]
    async fn ensure_lookahead_partitions_creates_additional_days() {
        let (pool, _container) = setup_db_with_table("test_part_lookahead").await;

        // setup_db_with_table already created 7 daily partitions.
        // Request 14 → should create 7 more (days 8–14).
        let pm = PartitionManager::new(pool.clone(), "test_part_lookahead", 14);
        let created = pm.ensure_lookahead_partitions().await.unwrap();
        assert!(
            created >= 1,
            "expected at least 1 new partition, got {created}"
        );

        // Idempotent: second call should create 0.
        let created2 = pm.ensure_lookahead_partitions().await.unwrap();
        assert_eq!(created2, 0, "second call should be idempotent");
    }

    #[tokio::test]
    async fn run_maintenance_acquires_lock_and_creates_partitions() {
        let (pool, _container) = setup_db_with_table("test_part_maint").await;

        let pm = PartitionManager::new(pool.clone(), "test_part_maint", 7);
        // Should acquire advisory lock and succeed.
        pm.run_maintenance().await.unwrap();

        // Second call should also succeed (lock is released after first call).
        pm.run_maintenance().await.unwrap();
    }

    #[tokio::test]
    async fn index_manager_add_and_drop_index() {
        let (pool, _container) = setup_db_with_table("test_idx").await;

        let im = IndexManager::new(pool.clone());

        // Add index.
        im.ensure_index("test_idx", "file_path").await.unwrap();

        // Verify index exists.
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'test_idx' \
             AND INDEX_NAME = 'idx_file_path'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 1, "index should exist after ensure_index");

        // Drop index.
        im.drop_index("test_idx", "file_path").await.unwrap();

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'test_idx' \
             AND INDEX_NAME = 'idx_file_path'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 0, "index should be gone after drop_index");

        // Idempotent drop.
        im.drop_index("test_idx", "file_path").await.unwrap();
    }

    #[tokio::test]
    async fn index_manager_skips_invalid_column_name() {
        let (pool, _container) = setup_db_with_table("test_idx_invalid").await;

        let im = IndexManager::new(pool.clone());

        // SQL injection attempt should be silently skipped.
        im.ensure_index("test_idx_invalid", "'; DROP TABLE test_idx_invalid; --")
            .await
            .unwrap();

        // Verify no index was created.
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'test_idx_invalid' \
             AND INDEX_NAME LIKE '%DROP%'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 0, "no index should be created for invalid column");
    }
}

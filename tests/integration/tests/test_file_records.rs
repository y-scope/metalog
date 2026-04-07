#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_metastore::FileRecords;
    use metalog_timeutil::epoch_nanos;

    #[tokio::test]
    async fn promote_stuck_buffering() {
        let (pool, _container) = setup_db_with_table("test_promote").await;

        // Insert a file in IR_ARCHIVE_BUFFERING with old timestamp.
        let old_ts = epoch_nanos() - 3_600_000_000_000; // 1 hour ago
        sqlx::query(
            "INSERT INTO `test_promote` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES (?, ?, 'IR_ARCHIVE_BUFFERING', 10, 30, 0)",
        )
        .bind(old_ts)
        .bind(old_ts + 1000)
        .execute(&pool)
        .await
        .unwrap();

        let fr = FileRecords::new(pool.clone(), "test_promote");

        // Promote stuck files.
        let stale_before = epoch_nanos() - 1_800_000_000_000; // 30 min threshold
        let promoted = fr.promote_stuck_buffering(stale_before).await.unwrap();
        assert_eq!(promoted, 1);

        // Verify state changed.
        let state: String =
            sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM `test_promote` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "IR_ARCHIVE_CONSOLIDATION_PENDING");
    }

    #[tokio::test]
    async fn transition_expired_to_purging() {
        let (pool, _container) = setup_db_with_table("test_expire").await;

        let now = epoch_nanos();
        let expired_ts = now - 1_000_000_000; // 1 second ago

        // Insert an expired IR_CLOSED file.
        sqlx::query(
            "INSERT INTO `test_expire` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES (?, ?, 'IR_CLOSED', 10, 30, ?)",
        )
        .bind(expired_ts)
        .bind(expired_ts + 1000)
        .bind(expired_ts)
        .execute(&pool)
        .await
        .unwrap();

        let fr = FileRecords::new(pool.clone(), "test_expire");
        let transitioned = fr.transition_expired_to_purging(now).await.unwrap();
        assert_eq!(transitioned, 1);

        let state: String =
            sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM `test_expire` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "IR_PURGING");
    }

    #[tokio::test]
    async fn delete_expired_files() {
        let (pool, _container) = setup_db_with_table("test_delete").await;

        let now = epoch_nanos();
        let expired_ts = now - 1_000_000_000;

        // Insert an IR_PURGING file.
        sqlx::query(
            "INSERT INTO `test_delete` (min_timestamp, max_timestamp, state, \
             clp_ir_storage_backend, clp_ir_bucket, clp_ir_path, record_count, retention_days, \
             expires_at) VALUES (?, ?, 'IR_PURGING', 's3', 'logs', '/data/test.ir', 10, 30, ?)",
        )
        .bind(expired_ts)
        .bind(expired_ts + 1000)
        .bind(expired_ts)
        .execute(&pool)
        .await
        .unwrap();

        let fr = FileRecords::new(pool.clone(), "test_delete");
        let result = fr.delete_expired_files(now).await.unwrap();
        assert_eq!(result.deleted_count, 1);
        assert_eq!(result.ir_paths.len(), 1);
        assert_eq!(result.ir_paths[0].path, "/data/test.ir");

        // Verify deleted.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `test_delete`")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

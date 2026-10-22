#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_metastore::FileRecords;
    use metalog_timeutil::epoch_nanos;
    use metalog_types::file_state::FileState;

    #[tokio::test]
    async fn promote_stuck_ir_archive_buffering() {
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
        let promoted = fr
            .promote_stuck_buffering(
                FileState::IrArchiveBuffering,
                FileState::IrArchiveConsolidationPending,
                stale_before,
            )
            .await
            .unwrap();
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
    async fn promote_stuck_file_archive_buffering() {
        let (pool, _container) = setup_db_with_table("test_promote_file").await;

        let old_ts = epoch_nanos() - 3_600_000_000_000; // 1 hour ago
        sqlx::query(
            "INSERT INTO `test_promote_file` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES (?, ?, 'FILE_ARCHIVE_BUFFERING', 10, 30, 0)",
        )
        .bind(old_ts)
        .bind(old_ts + 1000)
        .execute(&pool)
        .await
        .unwrap();

        let fr = FileRecords::new(pool.clone(), "test_promote_file");

        let stale_before = epoch_nanos() - 1_800_000_000_000;
        let promoted = fr
            .promote_stuck_buffering(
                FileState::FileArchiveBuffering,
                FileState::FileArchiveConsolidationPending,
                stale_before,
            )
            .await
            .unwrap();
        assert_eq!(promoted, 1);

        let state: String =
            sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM `test_promote_file` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "FILE_ARCHIVE_CONSOLIDATION_PENDING");
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
    async fn transition_expired_file_archive_buffering_to_purging() {
        let (pool, _container) = setup_db_with_table("test_expire_fab").await;

        let now = epoch_nanos();
        let expired_ts = now - 1_000_000_000;

        // Insert an expired FILE_ARCHIVE_BUFFERING file.
        sqlx::query(
            "INSERT INTO `test_expire_fab` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES (?, ?, 'FILE_ARCHIVE_BUFFERING', 10, 30, ?)",
        )
        .bind(expired_ts)
        .bind(expired_ts + 1000)
        .bind(expired_ts)
        .execute(&pool)
        .await
        .unwrap();

        let fr = FileRecords::new(pool.clone(), "test_expire_fab");
        let transitioned = fr.transition_expired_to_purging(now).await.unwrap();
        assert_eq!(transitioned, 1);

        let state: String =
            sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM `test_expire_fab` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "ARCHIVE_PURGING");
    }

    #[tokio::test]
    async fn finish_purge_uses_the_complete_partitioned_primary_key() {
        use metalog_storage::{Backend, FilesystemBackend};

        let (pool, _container) = setup_db_with_table("test_delete").await;
        let dir = tempfile::tempdir().unwrap();
        let storage = FilesystemBackend::new(dir.path().to_str().unwrap());
        let now = epoch_nanos();
        for (offset, path) in [(2, "first.ir"), (1, "second.ir")] {
            let timestamp = now - offset * 1_000_000_000;
            sqlx::query(
                "INSERT INTO test_delete (id, min_timestamp, max_timestamp, state, \
                 file_storage_backend, file_bucket, file_path, record_count, retention_days, \
                 expires_at) VALUES (42, ?, ?, 'IR_PURGING', 'fs', 'logs', ?, 10, 30, ?)",
            )
            .bind(timestamp)
            .bind(timestamp)
            .bind(path)
            .bind(timestamp)
            .execute(&pool)
            .await
            .unwrap();
            storage.put("logs", path, b"data").await.unwrap();
        }
        let files = FileRecords::new(pool.clone(), "test_delete");
        let pending = files.find_purging_files(now, None).await.unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].id, pending[1].id);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_delete")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 2,
            "reading deletion work must preserve retry metadata"
        );
        for path in &pending[0].paths {
            assert!(files.retire_storage_path(path, now).await.unwrap());
            storage.delete(&path.bucket, &path.path).await.unwrap();
        }
        assert_eq!(files.finish_purge(&pending[0], now).await.unwrap(), 1);
        let remaining = files.find_purging_files(now, None).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].min_timestamp, pending[1].min_timestamp);
        assert!(storage.exists("logs", "second.ir").await.unwrap());
        assert!(!storage.exists("logs", "first.ir").await.unwrap());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metalog_ingestion::{BatchingWriter, IngestionService};
    use metalog_it::helpers::setup_db_with_table;
    use metalog_schema::ColumnRegistry;
    use metalog_types::{file_state::FileState, FileRecord};
    use serde_json::Value;

    fn ir_record(min_ts: i64, max_ts: i64, path: &str) -> FileRecord {
        FileRecord {
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            state: FileState::IrBuffering,
            file_path: Some(path.to_string()),
            record_count: 100,
            raw_size_bytes: 1024,
            ..FileRecord::default()
        }
    }

    fn archive_record(min_ts: i64, max_ts: i64, path: &str) -> FileRecord {
        FileRecord {
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            state: FileState::ArchiveClosed,
            archive_path: Some(path.to_string()),
            record_count: 500,
            raw_size_bytes: 4096,
            ..FileRecord::default()
        }
    }

    // ── single record ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn single_ir_record_lands_in_db() {
        let (pool, _c) = setup_db_with_table("test_ingest_single").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));

        writer
            .submit_wait("test_ingest_single", ir_record(1000, 2000, "/data/a.ir"))
            .await
            .unwrap();
        writer.stop().await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_single`")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn record_fields_persisted_correctly() {
        let (pool, _c) = setup_db_with_table("test_ingest_fields").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));

        let rec = FileRecord {
            min_timestamp: 111_000,
            max_timestamp: 222_000,
            state: FileState::IrBuffering,
            file_path: Some("/data/b.ir".to_string()),
            record_count: 42,
            raw_size_bytes: 8192,
            retention_days: 7,
            ..FileRecord::default()
        };

        writer.submit_wait("test_ingest_fields", rec).await.unwrap();
        writer.stop().await;

        let row: (i64, i64, i64, i32) = sqlx::query_as(
            "SELECT min_timestamp, max_timestamp, record_count, retention_days \
             FROM `test_ingest_fields` LIMIT 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.0, 111_000);
        assert_eq!(row.1, 222_000);
        assert_eq!(row.2, 42);
        assert_eq!(row.3, 7);
    }

    // ── batch of records ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn batch_of_records_all_land() {
        let (pool, _c) = setup_db_with_table("test_ingest_batch").await;
        let writer = Arc::new(
            BatchingWriter::new(pool.clone(), false).with_batch_size(10),
        );

        let n = 50usize;
        for i in 0..n {
            let rec = ir_record(i as i64 * 1000, i as i64 * 1000 + 999, "/data/f.ir");
            writer.submit_wait("test_ingest_batch", rec).await.unwrap();
        }
        writer.stop().await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_batch`")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, n as i64);
    }

    #[tokio::test]
    async fn archive_record_stored_with_archive_path() {
        let (pool, _c) = setup_db_with_table("test_ingest_archive").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));

        writer
            .submit_wait(
                "test_ingest_archive",
                archive_record(5000, 6000, "archives/test.zst"),
            )
            .await
            .unwrap();
        writer.stop().await;

        let path: String =
            sqlx::query_scalar("SELECT archive_path FROM `test_ingest_archive` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(path, "archives/test.zst");
    }

    // ── dimension columns ────────────────────────────────────────────────────

    #[tokio::test]
    async fn dim_column_allocated_and_stored() {
        let (pool, _c) = setup_db_with_table("test_ingest_dim").await;

        // Provide a registry so the writer can resolve/allocate dim columns.
        let registry =
            Arc::new(ColumnRegistry::new(pool.clone(), "test_ingest_dim").await.unwrap());
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        writer.set_registry("test_ingest_dim", registry).await;

        let rec = FileRecord {
            min_timestamp: 1000,
            max_timestamp: 2000,
            state: FileState::IrBuffering,
            file_path: Some("/data/dim.ir".to_string()),
            record_count: 1,
            dims: [("hostname".to_string(), Value::String("host-1".to_string()))]
                .into_iter()
                .collect(),
            ..FileRecord::default()
        };

        writer.submit_wait("test_ingest_dim", rec).await.unwrap();
        writer.stop().await;

        // The first dim should be allocated to dim_f01.
        let val: Option<String> =
            sqlx::query_scalar("SELECT dim_f01 FROM `test_ingest_dim` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(val.as_deref(), Some("host-1"));
    }

    #[tokio::test]
    async fn second_ingest_reuses_dim_slot() {
        let (pool, _c) = setup_db_with_table("test_ingest_dim2").await;

        let registry =
            Arc::new(ColumnRegistry::new(pool.clone(), "test_ingest_dim2").await.unwrap());
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        writer.set_registry("test_ingest_dim2", registry).await;

        let make = |host: &str| FileRecord {
            min_timestamp: 1000,
            max_timestamp: 2000,
            state: FileState::IrBuffering,
            file_path: Some("/data/x.ir".to_string()),
            record_count: 1,
            dims: [("hostname".to_string(), Value::String(host.to_string()))]
                .into_iter()
                .collect(),
            ..FileRecord::default()
        };

        writer.submit_wait("test_ingest_dim2", make("host-A")).await.unwrap();
        writer.submit_wait("test_ingest_dim2", make("host-B")).await.unwrap();
        writer.stop().await;

        // Both rows should use the same column dim_f01 (same dim key).
        let vals: Vec<String> =
            sqlx::query_scalar("SELECT dim_f01 FROM `test_ingest_dim2` ORDER BY id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(vals, vec!["host-A", "host-B"]);
    }

    // ── IngestionService ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn service_rejects_invalid_record() {
        let (pool, _c) = setup_db_with_table("test_ingest_svc_invalid").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        let svc = IngestionService::new(writer.clone(), true);

        // max_timestamp < min_timestamp — should be rejected before hitting the writer.
        let bad = FileRecord {
            min_timestamp: 5000,
            max_timestamp: 1000,
            file_path: Some("/data/bad.ir".to_string()),
            ..FileRecord::default()
        };

        let result = svc.ingest("test_ingest_svc_invalid", bad).await;
        assert!(!result.accepted);
        assert!(result.error.is_some());

        writer.stop().await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_svc_invalid`")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0, "invalid record must not reach the DB");
    }

    #[tokio::test]
    async fn service_batch_ingest_mixed_valid_invalid() {
        let (pool, _c) = setup_db_with_table("test_ingest_batch_svc").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        let svc = IngestionService::new(writer.clone(), true);

        let records = vec![
            ir_record(1000, 2000, "/data/ok1.ir"),     // index 0 — valid
            FileRecord {
                // index 1 — invalid: no path
                min_timestamp: 3000,
                max_timestamp: 4000,
                ..FileRecord::default()
            },
            ir_record(5000, 6000, "/data/ok2.ir"),     // index 2 — valid
            FileRecord {
                // index 3 — invalid: negative timestamp
                min_timestamp: -1,
                max_timestamp: 1000,
                file_path: Some("/data/bad.ir".to_string()),
                ..FileRecord::default()
            },
        ];

        let result = svc.batch_ingest("test_ingest_batch_svc", records).await;

        assert_eq!(result.accepted_count, 2);
        assert_eq!(result.rejected_count, 2);
        assert_eq!(result.failures.len(), 2);
        // Failure indices must be the original batch positions.
        assert_eq!(result.failures[0].index, 1);
        assert_eq!(result.failures[1].index, 3);

        writer.stop().await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_batch_svc`")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 2);
    }
}

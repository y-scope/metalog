#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metalog_ingestion::{BatchingWriter, IngestionService};
    use metalog_it::helpers::setup_db_with_table;
    use metalog_schema::ColumnRegistry;
    use metalog_types::{file_record::DimMeta, file_state::FileState, FileRecord};
    use serde_json::Value;

    fn ir_record(min_ts: i64, max_ts: i64, path: &str) -> FileRecord {
        FileRecord {
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            state: FileState::IrBuffering,
            file_path: Some(path.to_string()),
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
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
            archive_storage_backend: Some("s3".into()),
            archive_bucket: Some("test".into()),
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

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_single`")
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
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
            record_count: 42,
            raw_size_bytes: 8192,
            retention_days: 7,
            ..FileRecord::default()
        };

        writer.submit_wait("test_ingest_fields", rec).await.unwrap();
        writer.stop().await;

        let row: (i64, i64, i64, i32) = sqlx::query_as(
            "SELECT min_timestamp, max_timestamp, CAST(record_count AS SIGNED), \
             CAST(retention_days AS SIGNED) FROM `test_ingest_fields` LIMIT 1",
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
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false).with_batch_size(10));

        let n = 50usize;
        for i in 0..n {
            let rec = ir_record(i as i64 * 1000, i as i64 * 1000 + 999, "/data/f.ir");
            writer.submit_wait("test_ingest_batch", rec).await.unwrap();
        }
        writer.stop().await;

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_batch`")
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

        let path: String = sqlx::query_scalar(
            "SELECT CAST(archive_path AS CHAR) FROM `test_ingest_archive` LIMIT 1",
        )
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
        let registry = Arc::new(
            ColumnRegistry::new(pool.clone(), "test_ingest_dim")
                .await
                .unwrap(),
        );
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        writer.set_registry("test_ingest_dim", registry).await;

        let rec = FileRecord {
            min_timestamp: 1000,
            max_timestamp: 2000,
            state: FileState::IrBuffering,
            file_path: Some("/data/dim.ir".to_string()),
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
            record_count: 1,
            dim_meta: vec![DimMeta {
                key: "hostname".into(),
                base_type: "str".into(),
                width: 256,
            }],
            dims: [("hostname".to_string(), Value::String("host-1".to_string()))]
                .into_iter()
                .collect(),
            ..FileRecord::default()
        };

        writer.submit_wait("test_ingest_dim", rec).await.unwrap();
        writer.stop().await;

        // The first dim should be allocated to dim_f01.
        let val: Option<String> =
            sqlx::query_scalar("SELECT CAST(dim_f01 AS CHAR) FROM `test_ingest_dim` LIMIT 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(val.as_deref(), Some("host-1"));
    }

    #[tokio::test]
    async fn second_ingest_reuses_dim_slot() {
        let (pool, _c) = setup_db_with_table("test_ingest_dim2").await;

        let registry = Arc::new(
            ColumnRegistry::new(pool.clone(), "test_ingest_dim2")
                .await
                .unwrap(),
        );
        let writer = Arc::new(BatchingWriter::new(pool.clone(), false));
        writer.set_registry("test_ingest_dim2", registry).await;

        let make = |host: &str| FileRecord {
            min_timestamp: 1000,
            max_timestamp: 2000,
            state: FileState::IrBuffering,
            file_path: Some(format!("/data/{host}.ir")),
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
            record_count: 1,
            dim_meta: vec![DimMeta {
                key: "hostname".into(),
                base_type: "str".into(),
                width: 256,
            }],
            dims: [("hostname".to_string(), Value::String(host.to_string()))]
                .into_iter()
                .collect(),
            ..FileRecord::default()
        };

        writer
            .submit_wait("test_ingest_dim2", make("host-A"))
            .await
            .unwrap();
        writer
            .submit_wait("test_ingest_dim2", make("host-B"))
            .await
            .unwrap();
        writer.stop().await;

        // Both rows should use the same column dim_f01 (same dim key).
        let vals: Vec<String> =
            sqlx::query_scalar("SELECT CAST(dim_f01 AS CHAR) FROM `test_ingest_dim2` ORDER BY id")
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
            file_storage_backend: Some("s3".into()),
            file_bucket: Some("test".into()),
            ..FileRecord::default()
        };

        let result = svc.ingest("test_ingest_svc_invalid", bad).await;
        assert!(!result.accepted);
        assert!(result.error.is_some());

        writer.stop().await;

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_svc_invalid`")
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
            ir_record(1000, 2000, "/data/ok1.ir"), // index 0 — valid
            FileRecord {
                // index 1 — invalid: no path
                min_timestamp: 3000,
                max_timestamp: 4000,
                ..FileRecord::default()
            },
            ir_record(5000, 6000, "/data/ok2.ir"), // index 2 — valid
            FileRecord {
                // index 3 — invalid: negative timestamp
                min_timestamp: -1,
                max_timestamp: 1000,
                file_path: Some("/data/bad.ir".to_string()),
                file_storage_backend: Some("s3".into()),
                file_bucket: Some("test".into()),
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

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM `test_ingest_batch_svc`")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    fn dimension(mut rec: FileRecord, value: &str) -> FileRecord {
        rec.dim_meta.push(DimMeta {
            key: "host".into(),
            base_type: "str".into(),
            width: 256,
        });
        rec.dims.insert("host".into(), Value::String(value.into()));
        rec
    }

    #[tokio::test]
    async fn newer_snapshot_updates_payload_before_entering_protected_state() {
        let table = "test_ingest_guard";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        let mut initial = dimension(ir_record(1000, 2000, "/data/update.ir"), "before");
        initial.state = FileState::IrArchiveBuffering;
        assert!(svc.ingest(table, initial.clone()).await.accepted);
        let mut newer = dimension(ir_record(1000, 3000, "/data/update.ir"), "after");
        newer.state = FileState::IrArchiveConsolidationPending;
        newer.record_count = 200;
        newer.file_size_bytes = 4096;
        assert!(svc.ingest(table, newer).await.accepted);
        let row: (i64, String, i64, i64, String) = sqlx::query_as(
            "SELECT max_timestamp,CAST(state AS CHAR),CAST(record_count AS \
             SIGNED),CAST(file_size_bytes AS SIGNED),CAST(dim_f01 AS CHAR) FROM test_ingest_guard",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row,
            (
                3000,
                "IR_ARCHIVE_CONSOLIDATION_PENDING".into(),
                200,
                4096,
                "after".into()
            )
        );
        initial.max_timestamp = 4000;
        initial.record_count = 999;
        assert!(svc.ingest(table, initial).await.accepted);
        let unchanged: (i64, String, i64, i64, String) = sqlx::query_as(
            "SELECT max_timestamp,CAST(state AS CHAR),CAST(record_count AS \
             SIGNED),CAST(file_size_bytes AS SIGNED),CAST(dim_f01 AS CHAR) FROM test_ingest_guard",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(unchanged, row);
        writer.stop().await;
    }

    #[tokio::test]
    async fn equal_timestamp_finalizes_without_reopening_or_crossing_lifecycles() {
        let table = "test_ingest_finalize";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        let initial = ir_record(1000, 2000, "/data/finalize.ir");
        assert!(svc.ingest(table, initial.clone()).await.accepted);
        let mut closed = initial.clone();
        closed.state = FileState::IrClosed;
        assert!(svc.ingest(table, closed).await.accepted);
        let mut replay = initial.clone();
        replay.max_timestamp = 3000;
        replay.record_count = 999;
        assert!(svc.ingest(table, replay).await.accepted);
        let mut wrong_chain = initial;
        wrong_chain.state = FileState::FileArchiveBuffering;
        wrong_chain.max_timestamp = 4000;
        wrong_chain.record_count = 999;
        assert!(svc.ingest(table, wrong_chain).await.accepted);
        let row: (i64, String, i64) = sqlx::query_as(
            "SELECT max_timestamp,CAST(state AS CHAR),CAST(record_count AS SIGNED) FROM \
             test_ingest_finalize",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (2000, "IR_CLOSED".into(), 100));
        writer.stop().await;
    }

    #[tokio::test]
    async fn all_protected_states_reject_newer_snapshots_without_changing_payload() {
        let table = "test_ingest_protected";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        for (i, state) in FileState::upsert_guard_states().iter().enumerate() {
            let path = format!("/data/protected_{i}.ir");
            let mut initial = ir_record(1000, 2000, &path);
            initial.state = *state;
            assert!(svc.ingest(table, initial.clone()).await.accepted);
            initial.max_timestamp = 3000;
            initial.record_count = 999;
            assert!(svc.ingest(table, initial).await.accepted);
        }
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM test_ingest_protected WHERE max_timestamp=2000 AND \
             record_count=100",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, FileState::upsert_guard_states().len() as i64);
        writer.stop().await;
    }

    #[tokio::test]
    async fn mixed_batch_persists_dimensions_first_seen_after_the_first_record() {
        let table = "test_ingest_mixed_dims";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true));
        let svc = IngestionService::new(writer.clone(), true);
        let result = svc
            .batch_ingest(
                table,
                vec![
                    ir_record(1000, 2000, "/data/a.ir"),
                    dimension(ir_record(1000, 2000, "/data/b.ir"), "web-01"),
                ],
            )
            .await;
        assert_eq!(result.accepted_count, 2);
        let vals: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT CAST(dim_f01 AS CHAR) FROM test_ingest_mixed_dims ORDER BY file_path",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(vals, vec![None, Some("web-01".into())]);
        writer.stop().await;
    }

    #[tokio::test]
    async fn flush_errors_reject_records_and_success_means_committed() {
        let table = "test_ingest_commit";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        let result = svc
            .ingest(
                "no_such_ingestion_table",
                ir_record(1000, 2000, "/data/a.ir"),
            )
            .await;
        assert!(!result.accepted);
        assert!(result.error.unwrap().contains("flush failed"));
        let result = svc.ingest(table, ir_record(1000, 2000, "/data/a.ir")).await;
        assert!(result.accepted);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_ingest_commit")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "accepted is returned only after commit, before stop drains writers"
        );
        writer.stop().await;
        assert!(
            !svc.ingest(table, ir_record(1000, 2000, "/data/b.ir"))
                .await
                .accepted
        );
    }

    #[tokio::test]
    async fn failed_chunk_reports_failure_without_discarding_other_chunks() {
        let table = "test_ingest_chunk_failure";
        let (pool, _c) = setup_db_with_table(table).await;
        sqlx::query(
            "ALTER TABLE test_ingest_chunk_failure ADD CONSTRAINT reject_one CHECK (file_path <> \
             '/reject.ir')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let writer = Arc::new(
            BatchingWriter::new(pool.clone(), true)
                .with_batch_size(1001)
                .with_flush_interval(std::time::Duration::from_secs(60)),
        );
        let svc = IngestionService::new(writer.clone(), true);
        let records = (0..1001)
            .map(|i| {
                ir_record(
                    1000,
                    2000,
                    &if i == 0 {
                        "/reject.ir".into()
                    } else {
                        format!("/{i}.ir")
                    },
                )
            })
            .collect();
        let result = svc.batch_ingest(table, records).await;
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.rejected_count, 1000);
        assert_eq!(result.failures.first().unwrap().index, 0);
        assert_eq!(result.failures.last().unwrap().index, 999);
        let paths: Vec<String> =
            sqlx::query_scalar("SELECT CAST(file_path AS CHAR) FROM test_ingest_chunk_failure")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(paths, vec!["/1000.ir"]);
        writer.stop().await;
    }

    #[tokio::test]
    async fn dimension_allocation_errors_do_not_acknowledge_incomplete_rows() {
        let table = "test_ingest_dim_error";
        let (pool, _c) = setup_db_with_table(table).await;
        let registry = ColumnRegistry::new(pool.clone(), table).await.unwrap();
        registry
            .resolve_or_allocate_dim("host", "int", 0)
            .await
            .unwrap();
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        let result = svc
            .ingest(
                table,
                dimension(ir_record(1000, 2000, "/data/a.ir"), "web-01"),
            )
            .await;
        assert!(!result.accepted);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_ingest_dim_error")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        writer.stop().await;
    }

    #[tokio::test]
    async fn invalid_records_and_identifiers_are_rejected_before_queueing() {
        let (pool, _c) = setup_db_with_table("test_ingest_validate").await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true));
        let svc = IngestionService::new(writer.clone(), true);
        let result = svc
            .ingest(
                "test_ingest_validate`; DROP TABLE _table;--",
                ir_record(1000, 2000, "/a.ir"),
            )
            .await;
        assert!(!result.accepted);
        let mut bad = ir_record(1000, 2000, "/bad.ir");
        bad.file_size_bytes = -1;
        let result = svc
            .batch_ingest(
                "test_ingest_validate",
                vec![bad, ir_record(1000, 2000, "/good.ir")],
            )
            .await;
        assert_eq!(result.accepted_count, 1);
        assert_eq!(result.rejected_count, 1);
        assert_eq!(result.failures[0].index, 0);
        writer.stop().await;
    }
    #[tokio::test]
    async fn text_values_roundtrip_with_no_backslash_escapes() {
        let table = "test_ingest_literal";
        let (pool, _c) = setup_db_with_table(table).await;
        let write_pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(2)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let writer = Arc::new(BatchingWriter::new(write_pool, true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        let path = "/quoted'\\é\n.ir";
        let result = svc.ingest(table, ir_record(1000, 2000, path)).await;
        assert!(result.accepted, "{:?}", result.error);
        let actual: String =
            sqlx::query_scalar("SELECT CAST(file_path AS CHAR) FROM test_ingest_literal")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(actual, path);
        writer.stop().await;
    }
    #[tokio::test]
    async fn optional_mysql_pool_cannot_bypass_the_guard_connection() {
        let table = "test_ingest_mysql_pool";
        let (pool, _container) = setup_db_with_table(table).await;
        // This deliberately unreachable compatibility pool must not execute
        // commits outside the connection holding the storage mutation lock.
        let mysql_pool = mysql_async::Pool::new(
            mysql_async::OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(1)
                .user(Some("root"))
                .db_name(Some("test")),
        );
        let writer = Arc::new(
            BatchingWriter::new(pool.clone(), true)
                .with_mysql_pool(mysql_pool.clone())
                .with_batch_size(1),
        );
        let svc = IngestionService::new(writer.clone(), true);
        let path = "/quoted'\\é.ir";
        assert!(
            svc.ingest(table, ir_record(1000, 2000, path))
                .await
                .accepted
        );
        let mut newer = ir_record(1000, 3000, path);
        newer.state = FileState::IrClosed;
        newer.record_count = 200;
        let result = svc.ingest(table, newer).await;
        assert!(result.accepted, "{:?}", result.error);
        let row: (String, i64, String, i64) = sqlx::query_as(
            "SELECT CAST(file_path AS CHAR),max_timestamp,CAST(state AS CHAR),CAST(record_count \
             AS SIGNED) FROM test_ingest_mysql_pool",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (path.into(), 3000, "IR_CLOSED".into(), 200));
        writer.stop().await;
        drop(writer);
        drop(svc);
        mysql_pool.disconnect().await.unwrap();
    }
    #[tokio::test]
    async fn retired_objects_cannot_be_referenced_by_ingestion() {
        let table = "test_ingest_retired";
        let (pool, _c) = setup_db_with_table(table).await;
        let path = metalog_metastore::StoragePath {
            backend: "s3".into(),
            bucket: "test".into(),
            path: "/retired.ir".into(),
        };
        let mut guard = metalog_metastore::StorageMutationGuard::acquire(&pool)
            .await
            .unwrap();
        guard.retire(&path).await.unwrap();
        guard.release().await.unwrap();
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        for rec in [
            ir_record(1000, 2000, &path.path),
            archive_record(1000, 2000, &path.path),
        ] {
            let result = svc.ingest(table, rec).await;
            assert!(!result.accepted);
            assert!(result.error.unwrap().contains("retired"));
        }
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_ingest_retired")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert!(
            svc.ingest(table, ir_record(1000, 2000, "/new.ir"))
                .await
                .accepted
        );
        writer.stop().await;
    }
    #[tokio::test]
    async fn dimension_invalidation_is_observed_before_the_next_commit() {
        let table = "test_ingest_invalidation";
        let (pool, _c) = setup_db_with_table(table).await;
        let writer = Arc::new(BatchingWriter::new(pool.clone(), true).with_batch_size(1));
        let svc = IngestionService::new(writer.clone(), true);
        assert!(
            svc.ingest(table, dimension(ir_record(1000, 2000, "/a.ir"), "before"))
                .await
                .accepted
        );
        sqlx::query(
            "UPDATE _dim_registry SET state='INVALIDATED' WHERE table_name=? AND dim_key='host'",
        )
        .bind(table)
        .execute(&pool)
        .await
        .unwrap();
        let result = svc
            .ingest(table, dimension(ir_record(1000, 2000, "/b.ir"), "after"))
            .await;
        assert!(result.accepted, "{:?}", result.error);
        let column: String = sqlx::query_scalar(
            "SELECT CAST(column_name AS CHAR) FROM _dim_registry WHERE table_name=? AND \
             dim_key='host' AND state='ACTIVE'",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_ne!(
            column, "dim_f01",
            "invalidated physical slots must not receive new values"
        );
        let value: String = sqlx::query_scalar(&format!(
            "SELECT CAST(`{column}` AS CHAR) FROM test_ingest_invalidation WHERE file_path='/b.ir'"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(value, "after");
        writer.stop().await;
    }

    #[tokio::test]
    async fn configured_storage_rejects_ambiguous_file_and_archive_locations() {
        let table = "test_ingest_canonical_locations";
        let (pool, _container) = setup_db_with_table(table).await;
        let directory = tempfile::tempdir().unwrap();
        let mut storage = metalog_storage::Registry::new();
        storage.register(
            "local",
            Arc::new(metalog_storage::FilesystemBackend::new(
                directory.path().to_str().unwrap(),
            )),
        );
        let writer = Arc::new(
            BatchingWriter::new(pool.clone(), true)
                .with_storage(Arc::new(storage))
                .with_batch_size(1),
        );
        let service = IngestionService::new(writer.clone(), true);
        for path in ["/file.ir", "a//b.ir", "a/./b.ir", "a/../b.ir", "a\\b.ir"] {
            let mut file = ir_record(1000, 2000, path);
            file.file_storage_backend = Some("local".into());
            let mut archive = archive_record(1000, 2000, path);
            archive.archive_storage_backend = Some("local".into());
            for record in [file, archive] {
                let result = service.ingest(table, record).await;
                assert!(!result.accepted, "ambiguous location accepted: {path}");
                assert!(result.error.unwrap().contains("location"));
            }
        }
        let mut invalid_bucket = ir_record(1000, 2000, "file.ir");
        invalid_bucket.file_storage_backend = Some("local".into());
        invalid_bucket.file_bucket = Some("nested/bucket".into());
        assert!(!service.ingest(table, invalid_bucket).await.accepted);

        let mut canonical = ir_record(1000, 2000, "dir/file.ir");
        canonical.file_storage_backend = Some("local".into());
        assert!(service.ingest(table, canonical).await.accepted);
        // An unconfigured backend is metadata-only on this node. Do not rewrite
        // or apply filesystem semantics to another backend's raw object key.
        let mut external = ir_record(1000, 2000, "/raw/external.ir");
        external.file_storage_backend = Some("external".into());
        assert!(service.ingest(table, external).await.accepted);
        let paths: Vec<String> = sqlx::query_scalar(
            "SELECT CAST(file_path AS CHAR) FROM test_ingest_canonical_locations ORDER BY \
             file_path",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(paths, vec!["/raw/external.ir", "dir/file.ir"]);
        writer.stop().await;
    }
}

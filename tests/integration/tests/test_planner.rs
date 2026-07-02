#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use metalog_consolidation::{
        marshal_payload, unmarshal_payload, FileGroup, InFlightSet, Planner, PlannerConfig,
        Policy, Queue, TaskPayload, TASK_PAYLOAD_VERSION,
    };
    use metalog_it::helpers::setup_db_with_table;
    use metalog_metastore::FileRecords;
    use metalog_timeutil::epoch_nanos;
    use metalog_types::FileRecord;
    use tokio_util::sync::CancellationToken;

    /// Test policy that groups all candidates into one FileGroup.
    struct AllInOnePolicy;

    impl Policy for AllInOnePolicy {
        fn select_files(&self, candidates: &[FileRecord]) -> Vec<FileGroup> {
            if candidates.is_empty() {
                return vec![];
            }
            vec![FileGroup {
                records: candidates.to_vec(),
                archive_path: "/archives/test.clp".into(),
                archive_backend: "s3".into(),
                archive_bucket: "archives".into(),
            }]
        }
    }

    /// Inserts files with the given state into the data table.
    async fn insert_files(
        pool: &sqlx::MySqlPool,
        table: &str,
        state: &str,
        paths: &[&str],
        max_ts_offset_secs: i64,
    ) {
        let now = epoch_nanos();
        for (i, path) in paths.iter().enumerate() {
            sqlx::query(&format!(
                "INSERT INTO `{table}` \
                 (min_timestamp, max_timestamp, state, file_storage_backend, file_bucket, file_path, \
                  file_size_bytes, raw_size_bytes, record_count, retention_days, expires_at) \
                 VALUES (?, ?, ?, 's3', 'logs', ?, 1024, 2048, 100, 30, ?)"
            ))
            .bind(now - 60_000_000_000) // min_timestamp: 1 minute ago
            .bind(now - max_ts_offset_secs * 1_000_000_000) // max_timestamp
            .bind(state)
            .bind(path)
            .bind(now + 2_592_000_000_000i64) // expires_at: 30 days from now
            .execute(pool)
            .await
            .unwrap();

            // Suppress unused warning for index.
            let _ = i;
        }
    }

    #[tokio::test]
    async fn run_creates_tasks_from_candidates() {
        let (pool, _container) = setup_db_with_table("test_plan").await;

        // Seed 3 consolidation-pending files.
        insert_files(
            &pool,
            "test_plan",
            "IR_ARCHIVE_CONSOLIDATION_PENDING",
            &["/logs/a.ir", "/logs/b.ir", "/logs/c.ir"],
            30, // max_timestamp 30s ago
        )
        .await;

        let file_recs = Arc::new(FileRecords::new(pool.clone(), "test_plan"));
        let queue = Arc::new(Queue::new(pool.clone()));
        let policy = Arc::new(AllInOnePolicy);
        let in_flight = Arc::new(InFlightSet::new());

        let planner = Planner::new(PlannerConfig {
            file_recs,
            queue: queue.clone(),
            policy,
            in_flight,
            table_name: "test_plan".into(),
            archive_backend: "s3".into(),
            archive_bucket: "archives".into(),
            interval: Duration::from_millis(10),
            stale_threshold: Duration::ZERO,
        });

        let token = CancellationToken::new();
        let handle = tokio::spawn({
            let t = token.clone();
            async move { planner.run(t).await }
        });

        // Poll until a task is created (5s timeout).
        let mut found = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if queue.count_active_tasks("test_plan").await.unwrap() >= 1 {
                found = true;
                break;
            }
        }
        assert!(found, "planner should create at least one task");

        token.cancel();
        handle.await.unwrap();

        // Claim the task and verify payload.
        let tasks = queue.claim_tasks("test_plan", "w1", 10).await.unwrap();
        assert_eq!(tasks.len(), 1);

        let payload = unmarshal_payload(&tasks[0].input).unwrap();
        assert_eq!(payload.table_name, "test_plan");
        let consolidation = payload.consolidation.unwrap();
        assert_eq!(consolidation.ir_paths.len(), 3);
    }

    #[tokio::test]
    async fn run_respects_backpressure() {
        let (pool, _container) = setup_db_with_table("test_bp").await;

        let queue = Arc::new(Queue::new(pool.clone()));

        // Pre-seed 101 pending tasks (backpressure limit is 100).
        let data = marshal_payload(&TaskPayload {
            table_name: "test_bp".into(),
            consolidation: None,
        })
        .unwrap();
        for _ in 0..101 {
            queue
                .create_tasks("test_bp", TASK_PAYLOAD_VERSION, &[data.clone()])
                .await
                .unwrap();
        }

        // Seed a consolidation-pending file so the planner would try to create a task.
        insert_files(
            &pool,
            "test_bp",
            "IR_ARCHIVE_CONSOLIDATION_PENDING",
            &["/logs/x.ir"],
            30,
        )
        .await;

        let file_recs = Arc::new(FileRecords::new(pool.clone(), "test_bp"));
        let policy = Arc::new(AllInOnePolicy);
        let in_flight = Arc::new(InFlightSet::new());

        let planner = Planner::new(PlannerConfig {
            file_recs,
            queue: queue.clone(),
            policy,
            in_flight,
            table_name: "test_bp".into(),
            archive_backend: "s3".into(),
            archive_bucket: "archives".into(),
            interval: Duration::from_millis(10),
            stale_threshold: Duration::ZERO,
        });

        let token = CancellationToken::new();
        let handle = tokio::spawn({
            let t = token.clone();
            async move { planner.run(t).await }
        });

        // Give planner a few cycles to observe backpressure.
        tokio::time::sleep(Duration::from_millis(200)).await;
        token.cancel();
        handle.await.unwrap();

        // Still 101 tasks (no new ones created).
        assert_eq!(queue.count_active_tasks("test_bp").await.unwrap(), 101);
    }

    #[tokio::test]
    async fn run_promotes_stuck_buffering() {
        let (pool, _container) = setup_db_with_table("test_promote").await;

        // 2 buffering files from 1 hour ago (stuck), 1 consolidation-pending file.
        insert_files(
            &pool,
            "test_promote",
            "IR_ARCHIVE_BUFFERING",
            &["/logs/stuck1.ir", "/logs/stuck2.ir"],
            3600, // max_timestamp 1 hour ago → stale
        )
        .await;

        insert_files(
            &pool,
            "test_promote",
            "IR_ARCHIVE_CONSOLIDATION_PENDING",
            &["/logs/ready.ir"],
            30,
        )
        .await;

        let file_recs = Arc::new(FileRecords::new(pool.clone(), "test_promote"));
        let queue = Arc::new(Queue::new(pool.clone()));
        let policy = Arc::new(AllInOnePolicy);
        let in_flight = Arc::new(InFlightSet::new());

        let planner = Planner::new(PlannerConfig {
            file_recs: file_recs.clone(),
            queue: queue.clone(),
            policy,
            in_flight,
            table_name: "test_promote".into(),
            archive_backend: "s3".into(),
            archive_bucket: "archives".into(),
            interval: Duration::from_millis(10),
            stale_threshold: Duration::from_secs(1800),
        });

        let token = CancellationToken::new();
        let handle = tokio::spawn({
            let t = token.clone();
            async move { planner.run(t).await }
        });

        // Wait for at least one task (stuck files promoted + consolidated).
        let mut found = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if queue.count_active_tasks("test_promote").await.unwrap() >= 1 {
                found = true;
                break;
            }
        }
        assert!(found, "planner should create task after promoting stuck files");

        token.cancel();
        handle.await.unwrap();

        // Verify stuck files were promoted to consolidation-pending.
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM `test_promote` \
             WHERE state = 'IR_ARCHIVE_CONSOLIDATION_PENDING'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(count.0 >= 3, "all 3 files should be consolidation-pending");
    }
}

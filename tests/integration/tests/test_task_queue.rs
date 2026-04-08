#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use metalog_consolidation::{
        marshal_payload, unmarshal_payload, ConsolidationPayload, Queue, TaskPayload,
        TASK_PAYLOAD_VERSION,
    };
    use metalog_it::helpers::setup_db_with_table;

    fn test_payload(table: &str) -> Vec<u8> {
        marshal_payload(&TaskPayload {
            table_name: table.into(),
            consolidation: None,
        })
        .unwrap()
    }

    fn test_payload_with_consolidation(table: &str) -> Vec<u8> {
        marshal_payload(&TaskPayload {
            table_name: table.into(),
            consolidation: Some(ConsolidationPayload {
                ir_backend: "s3".into(),
                ir_paths: vec!["/a.ir".into()],
                ir_buckets: vec!["logs".into()],
                archive_backend: "s3".into(),
                archive_bucket: "archives".into(),
                archive_path: "/out.clp".into(),
                file_ids: vec![1],
                min_timestamp: 1000,
            }),
        })
        .unwrap()
    }

    // ── Create & Claim ───────────────────────────────────────────────

    #[tokio::test]
    async fn create_and_claim() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let data = test_payload_with_consolidation("test");
        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();
        assert_eq!(created, 1);

        let tasks = queue.claim_tasks("test", "worker-1", 10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, "processing");

        let decoded = unmarshal_payload(&tasks[0].input).unwrap();
        assert_eq!(decoded.table_name, "test");
        let cons = decoded.consolidation.unwrap();
        assert_eq!(cons.ir_paths, vec!["/a.ir"]);
    }

    #[tokio::test]
    async fn claim_empty_queue() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let tasks = queue.claim_tasks("test", "worker-1", 10).await.unwrap();
        assert!(tasks.is_empty());
    }

    // ── Batch Create ─────────────────────────────────────────────────

    #[tokio::test]
    async fn create_tasks_batch() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let inputs: Vec<Vec<u8>> = (0..10).map(|_| test_payload("test")).collect();
        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();
        assert_eq!(created, 10);

        let count = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn create_tasks_empty() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[])
            .await
            .unwrap();
        assert_eq!(created, 0);
    }

    #[tokio::test]
    async fn create_tasks_large_batch() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        // Exceeds MAX_ROWS_PER_INSERT (100), exercises chunking.
        let inputs: Vec<Vec<u8>> = (0..250).map(|_| test_payload("test")).collect();
        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();
        assert_eq!(created, 250);

        let count = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(count, 250);
    }

    // ── Claim Batch Size ─────────────────────────────────────────────

    #[tokio::test]
    async fn claim_tasks_batch_size() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let inputs: Vec<Vec<u8>> = (0..5).map(|_| test_payload("test")).collect();
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();

        // Claim 3 of 5.
        let tasks = queue.claim_tasks("test", "worker-1", 3).await.unwrap();
        assert_eq!(tasks.len(), 3);

        // Claim remaining 2.
        let tasks2 = queue.claim_tasks("test", "worker-2", 10).await.unwrap();
        assert_eq!(tasks2.len(), 2);

        // Nothing left.
        let tasks3 = queue.claim_tasks("test", "worker-3", 10).await.unwrap();
        assert!(tasks3.is_empty());
    }

    // ── Complete ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn complete_task() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        let affected = queue.complete_task(task_id, b"result").await.unwrap();
        assert_eq!(affected, 1);

        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "completed");
    }

    // ── Fail & Dead Letter ───────────────────────────────────────────

    #[tokio::test]
    async fn fail_task() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        let affected = queue.fail_task(task_id).await.unwrap();
        assert_eq!(affected, 1);

        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "failed");
    }

    #[tokio::test]
    async fn fail_task_dead_letter_after_max_retries() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        // Set retry_count to max-1 so the next fail triggers dead_letter.
        sqlx::query("UPDATE _task_queue SET retry_count = 2 WHERE task_id = ?")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();

        let affected = queue.fail_task(task_id).await.unwrap();
        assert_eq!(affected, 1);

        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "dead_letter");
    }

    // ── Reclaim ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn reclaim_stale_task() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        // Reclaim the task (simulates stale detection).
        queue.reclaim_task(task_id).await.unwrap();

        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "pending");
    }

    #[tokio::test]
    async fn reclaim_dead_letters_when_max_retries() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        // Set retry_count >= max retries so reclaim dead-letters.
        sqlx::query("UPDATE _task_queue SET retry_count = 3 WHERE task_id = ?")
            .bind(task_id)
            .execute(&pool)
            .await
            .unwrap();

        queue.reclaim_task(task_id).await.unwrap();

        let (state,): (String,) =
            sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "dead_letter");
    }

    // ── Find Stale ───────────────────────────────────────────────────

    #[tokio::test]
    async fn find_stale_tasks() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        assert_eq!(tasks.len(), 1);

        // Backdate claimed_at so it appears stale.
        sqlx::query("UPDATE _task_queue SET claimed_at = 1 WHERE task_id = ?")
            .bind(tasks[0].task_id)
            .execute(&pool)
            .await
            .unwrap();

        let stale = queue
            .find_stale_tasks("test", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].task_id, tasks[0].task_id);
    }

    #[tokio::test]
    async fn find_stale_tasks_empty() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let stale = queue
            .find_stale_tasks("test", Duration::from_secs(300))
            .await
            .unwrap();
        assert!(stale.is_empty());
    }

    // ── Count ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn count_active_tasks() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let inputs: Vec<Vec<u8>> = (0..3).map(|_| test_payload("test")).collect();
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();

        // Claim 1 → 2 pending + 1 processing = 3 active.
        queue.claim_tasks("test", "w1", 1).await.unwrap();

        let count = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(count, 3);
    }

    // ── Cleanup ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn cleanup_old_tasks() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool.clone());

        let data = test_payload("test");
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        queue
            .complete_task(tasks[0].task_id, b"done")
            .await
            .unwrap();

        // Backdate completed_at so it's old enough for cleanup.
        sqlx::query("UPDATE _task_queue SET completed_at = 1 WHERE task_id = ?")
            .bind(tasks[0].task_id)
            .execute(&pool)
            .await
            .unwrap();

        let deleted = queue
            .cleanup_old_tasks("test", Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        let count = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(count, 0);
    }

    // ── Throughput ────────────────────────────────────────────────────

    #[tokio::test]
    async fn throughput_batch_create() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let inputs: Vec<Vec<u8>> = (0..1000)
            .map(|i| test_payload_with_consolidation(&format!("test_{i}")))
            .collect();

        let t0 = Instant::now();
        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();
        let elapsed = t0.elapsed();

        assert_eq!(created, 1000);
        let rate = 1000.0 / elapsed.as_secs_f64();
        println!(
            "batch_create: 1000 tasks in {:.1}ms ({:.0} tasks/s)",
            elapsed.as_millis(),
            rate
        );
        // Sanity: should be at least 500 tasks/s on any reasonable DB.
        assert!(rate > 500.0, "throughput too low: {rate:.0} tasks/s");
    }

    #[tokio::test]
    async fn throughput_claim_cycle() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        // Pre-populate 500 tasks.
        let inputs: Vec<Vec<u8>> = (0..500).map(|_| test_payload("test")).collect();
        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();

        // Claim in batches of 10.
        let t0 = Instant::now();
        let mut total_claimed = 0;
        loop {
            let tasks = queue.claim_tasks("test", "bench-worker", 10).await.unwrap();
            if tasks.is_empty() {
                break;
            }
            total_claimed += tasks.len();
        }
        let elapsed = t0.elapsed();

        assert_eq!(total_claimed, 500);
        let rate = 500.0 / elapsed.as_secs_f64();
        println!(
            "claim_cycle: 500 tasks (batch=10) in {:.1}ms ({:.0} tasks/s)",
            elapsed.as_millis(),
            rate
        );
        assert!(rate > 200.0, "claim throughput too low: {rate:.0} tasks/s");
    }

    #[tokio::test]
    async fn throughput_full_lifecycle() {
        let (pool, _container) = setup_db_with_table("test").await;
        let queue = Queue::new(pool);

        let count = 200;
        let inputs: Vec<Vec<u8>> = (0..count).map(|_| test_payload("test")).collect();

        // Measure full create → claim → complete cycle.
        let t0 = Instant::now();

        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &inputs)
            .await
            .unwrap();

        let tasks = queue
            .claim_tasks("test", "lifecycle-worker", count as i32)
            .await
            .unwrap();
        assert_eq!(tasks.len(), count);

        for task in &tasks {
            queue.complete_task(task.task_id, b"done").await.unwrap();
        }

        let elapsed = t0.elapsed();
        let rate = count as f64 / elapsed.as_secs_f64();
        println!(
            "full_lifecycle: {count} tasks (create+claim+complete) in {:.1}ms ({:.0} tasks/s)",
            elapsed.as_millis(),
            rate
        );
        assert!(rate > 100.0, "lifecycle throughput too low: {rate:.0} tasks/s");

        // Verify all completed.
        let active = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(active, 0);
    }
}

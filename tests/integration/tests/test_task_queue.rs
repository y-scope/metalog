#[cfg(test)]
mod tests {

    use metalog_consolidation::{
        marshal_payload,
        unmarshal_payload,
        ConsolidationPayload,
        Queue,
        TaskPayload,
        TASK_PAYLOAD_VERSION,
    };
    use metalog_it::helpers::setup_db_with_table;

    #[tokio::test]
    async fn create_and_claim() {
        let (pool, _container) = setup_db_with_table("test").await;

        let queue = Queue::new(pool);

        let payload = TaskPayload {
            table_name: "test".into(),
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
        };
        let data = marshal_payload(&payload).unwrap();

        // Create task.
        let created = queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();
        assert_eq!(created, 1);

        // Claim task.
        let tasks = queue.claim_tasks("test", "worker-1", 10).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, "processing");

        // Unmarshal payload.
        let decoded = unmarshal_payload(&tasks[0].input).unwrap();
        assert_eq!(decoded.table_name, "test");
    }

    #[tokio::test]
    async fn claim_empty_queue() {
        let (pool, _container) = setup_db_with_table("test").await;

        let queue = Queue::new(pool);
        let tasks = queue.claim_tasks("test", "worker-1", 10).await.unwrap();
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn complete_task() {
        let (pool, _container) = setup_db_with_table("test").await;

        let queue = Queue::new(pool.clone());
        let data = marshal_payload(&TaskPayload {
            table_name: "test".into(),
            consolidation: None,
        })
        .unwrap();

        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data])
            .await
            .unwrap();

        let tasks = queue.claim_tasks("test", "w1", 1).await.unwrap();
        let task_id = tasks[0].task_id;

        let output = b"result_data";
        let affected = queue.complete_task(task_id, output).await.unwrap();
        assert_eq!(affected, 1);

        // Verify state.
        let (state,): (String,) = sqlx::query_as("SELECT state FROM _task_queue WHERE task_id = ?")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "completed");
    }

    #[tokio::test]
    async fn count_active_tasks() {
        let (pool, _container) = setup_db_with_table("test").await;

        let queue = Queue::new(pool);

        let data = marshal_payload(&TaskPayload {
            table_name: "test".into(),
            consolidation: None,
        })
        .unwrap();

        queue
            .create_tasks("test", TASK_PAYLOAD_VERSION, &[data.clone(), data])
            .await
            .unwrap();

        let count = queue.count_active_tasks("test").await.unwrap();
        assert_eq!(count, 2);
    }
}

use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use metalog_consolidation::{
    execute_task,
    unmarshal_payload,
    unmarshal_result,
    ArchiveCreator,
    InFlightSet,
    Planner,
    PlannerConfig,
    Queue,
    TimeWindowPolicy,
};
use metalog_it::{clp, helpers::setup_db_with_table};
use metalog_metastore::FileRecords;
use metalog_retention::{RetentionModule, RetentionPolicy};
use metalog_storage::{Backend, FilesystemBackend, Registry, StorageError};
use metalog_timeutil::epoch_nanos;
use metalog_types::file_state::FileState;
use sqlx::MySqlPool;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::sync::CancellationToken;

const ONE: &[u8] = include_bytes!("../../fixtures/consolidation/one.ir");
const TWO: &[u8] = include_bytes!("../../fixtures/consolidation/two.ir");
const EXT: &[u8] = include_bytes!("../../fixtures/bloom/ext.lz4");

struct FaultStorage {
    fs: FilesystemBackend,
    fail_delete: AtomicBool,
    fail_upload_after_write: AtomicBool,
}
#[async_trait::async_trait]
impl Backend for FaultStorage {
    async fn get(
        &self,
        b: &str,
        p: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
        self.fs.get(b, p).await
    }

    async fn put(&self, b: &str, p: &str, data: &[u8]) -> Result<(), StorageError> {
        self.fs.put(b, p, data).await
    }

    async fn put_file(&self, b: &str, p: &str, file: &Path) -> Result<(), StorageError> {
        self.fs.put_file(b, p, file).await?;
        if self.fail_upload_after_write.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Other("injected ambiguous upload".into()));
        }
        Ok(())
    }

    async fn delete(&self, b: &str, p: &str) -> Result<(), StorageError> {
        if p.ends_with(".ir") && self.fail_delete.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Other("injected delete failure".into()));
        }
        self.fs.delete(b, p).await
    }

    async fn exists(&self, b: &str, p: &str) -> Result<bool, StorageError> {
        self.fs.exists(b, p).await
    }
}
fn storage() -> (tempfile::TempDir, Arc<FaultStorage>, Arc<Registry>) {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(FaultStorage {
        fs: FilesystemBackend::new(temp.path().to_str().unwrap()),
        fail_delete: AtomicBool::new(false),
        fail_upload_after_write: AtomicBool::new(false),
    });
    let mut registry = Registry::new();
    registry.register("local", backend.clone());
    (temp, backend, Arc::new(registry))
}
fn planner(pool: &MySqlPool, table: &str, storage: Arc<Registry>, enabled: bool) -> Planner {
    Planner::new(PlannerConfig {
        file_recs: Arc::new(FileRecords::new(pool.clone(), table)),
        queue: Arc::new(Queue::new(pool.clone())),
        policy: Arc::new(TimeWindowPolicy::new(Duration::from_secs(86400), 1, 100)),
        in_flight: Arc::new(InFlightSet::new()),
        table_name: table.into(),
        archive_backend: "local".into(),
        archive_bucket: "archives".into(),
        interval: Duration::from_secs(1),
        stale_threshold: Duration::ZERO,
        buffering_state: FileState::IrArchiveBuffering,
        pending_state: FileState::IrArchiveConsolidationPending,
    })
    .with_storage(storage)
    .with_planning_enabled(enabled)
}
async fn seed(
    pool: &MySqlPool,
    table: &str,
    timestamp: i64,
    id: i64,
    bucket: &str,
    path: &str,
    state: &str,
) {
    sqlx::query(&format!(
        "INSERT INTO `{table}` (id, min_timestamp, max_timestamp, state, file_storage_backend, \
         file_bucket, file_path, ext, record_count, file_size_bytes, raw_size_bytes, expires_at) \
         VALUES (?, ?, ?, ?, 'local', ?, ?, ?, 2, 400, 500, ?)"
    ))
    .bind(id)
    .bind(timestamp)
    .bind(timestamp + 1)
    .bind(state)
    .bind(bucket)
    .bind(path)
    .bind(EXT)
    .bind(epoch_nanos() + 86_400_000_000_000i64)
    .execute(pool)
    .await
    .unwrap();
}
async fn archive_path(pool: &MySqlPool, table: &str) -> String {
    sqlx::query_scalar(&format!(
        "SELECT CAST(archive_path AS CHAR) FROM `{table}` WHERE state = 'ARCHIVE_CLOSED' LIMIT 1"
    ))
    .fetch_one(pool)
    .await
    .unwrap()
}
async fn verify_archive(backend: &dyn Backend, bucket: &str, path: &str, expected: usize) {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("archive.clp");
    let mut reader = backend.get(bucket, path).await.unwrap();
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await.unwrap();
    tokio::fs::write(&file, data).await.unwrap();
    let out = temp.path().join("decoded");
    let output = tokio::process::Command::new(clp::binary())
        .arg("x")
        .arg(file)
        .arg(&out)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = tokio::fs::read_to_string(out.join("original"))
        .await
        .unwrap();
    let values: Vec<serde_json::Value> = records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(values.len(), expected, "decoded records: {records}");
    assert!(records.contains("worker alpha completed 12 items"));
    if expected == 4 {
        assert!(records.contains("worker beta failed 1 item"));
    }
}

#[tokio::test]
async fn real_ir_restart_cleanup_partition_identity_and_retention() {
    let (pool, _db) = setup_db_with_table("consolidate").await;
    let (_dir, backend, registry) = storage();
    let now = epoch_nanos();
    backend.put("first", "same.ir", ONE).await.unwrap();
    backend.put("second", "same.ir", TWO).await.unwrap();
    seed(
        &pool,
        "consolidate",
        now,
        42,
        "first",
        "same.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    seed(
        &pool,
        "consolidate",
        now + 1,
        42,
        "second",
        "same.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    // Same auto-increment ID, different full primary key: this row must not change.
    seed(
        &pool,
        "consolidate",
        now + 2,
        42,
        "third",
        "same.ir",
        "IR_CLOSED",
    )
    .await;
    let queue = Queue::new(pool.clone());
    planner(&pool, "consolidate", registry.clone(), true)
        .plan_once()
        .await
        .unwrap();
    planner(&pool, "consolidate", registry.clone(), true)
        .plan_once()
        .await
        .unwrap();
    assert_eq!(queue.count_active_tasks("consolidate").await.unwrap(), 1);
    sqlx::query("UPDATE consolidate SET expires_at = 1 WHERE min_timestamp = ?")
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();
    let files = FileRecords::new(pool.clone(), "consolidate");
    assert_eq!(
        files
            .transition_expired_to_purging(epoch_nanos())
            .await
            .unwrap(),
        0
    );
    let task = queue.claim_tasks("", "worker", 1).await.unwrap().remove(0);
    let creator = ArchiveCreator::new(registry.clone(), clp::binary(), Duration::from_secs(30));
    execute_task(&queue, &creator, task).await;
    backend.fail_delete.store(true, Ordering::SeqCst);
    planner(&pool, "consolidate", registry.clone(), false)
        .plan_once()
        .await
        .unwrap();
    let path = archive_path(&pool, "consolidate").await;
    assert!(backend.exists("archives", &path).await.unwrap());
    let states: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT CAST(state AS CHAR), ext FROM consolidate ORDER BY min_timestamp")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        states.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        ["ARCHIVE_CLOSED", "ARCHIVE_CLOSED", "IR_CLOSED"]
    );
    assert!(states.iter().all(|r| r.1 == EXT));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _task_queue WHERE state='completed'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    // New coordinator resumes after publication, without deleting the live archive.
    planner(&pool, "consolidate", registry.clone(), false)
        .plan_once()
        .await
        .unwrap();
    assert!(!backend.exists("first", "same.ir").await.unwrap());
    assert!(!backend.exists("second", "same.ir").await.unwrap());
    verify_archive(backend.as_ref(), "archives", &path, 4).await;
    RetentionModule::new()
        .run_once(&files, Some(&registry), 0, &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        backend.exists("archives", &path).await.unwrap(),
        "second row still references archive"
    );
    sqlx::query("UPDATE consolidate SET expires_at = 1 WHERE state='ARCHIVE_CLOSED'")
        .execute(&pool)
        .await
        .unwrap();
    RetentionModule::new()
        .run_once(&files, Some(&registry), 0, &CancellationToken::new())
        .await
        .unwrap();
    assert!(!backend.exists("archives", &path).await.unwrap());
}

#[tokio::test]
async fn ambiguous_upload_retries_with_new_destination_and_late_completion_is_fenced() {
    let (pool, _db) = setup_db_with_table("retry_upload").await;
    let (_dir, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    seed(
        &pool,
        "retry_upload",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    let queue = Queue::new(pool.clone());
    let plan = planner(&pool, "retry_upload", registry.clone(), true);
    plan.plan_once().await.unwrap();
    let first = queue.claim_tasks("", "worker", 1).await.unwrap().remove(0);
    let input = unmarshal_payload(&first.input)
        .unwrap()
        .consolidation
        .unwrap();
    backend
        .fail_upload_after_write
        .store(true, Ordering::SeqCst);
    let creator = ArchiveCreator::new(registry.clone(), clp::binary(), Duration::from_secs(30));
    execute_task(&queue, &creator, first.clone()).await;
    assert!(backend
        .exists("archives", &input.attempt_path(first.task_id))
        .await
        .unwrap());
    plan.plan_once().await.unwrap();
    let second = queue
        .claim_tasks("", "replacement", 1)
        .await
        .unwrap()
        .remove(0);
    assert_ne!(first.task_id, second.task_id);
    assert_eq!(second.retry_count, 1);
    assert_eq!(
        queue.complete_task(first.task_id, b"late").await.unwrap(),
        0
    );
    execute_task(&queue, &creator, second.clone()).await;
    plan.plan_once().await.unwrap();
    let path = archive_path(&pool, "retry_upload").await;
    assert_eq!(path, input.attempt_path(second.task_id));
    assert!(!backend
        .exists("archives", &input.attempt_path(first.task_id))
        .await
        .unwrap());
    verify_archive(backend.as_ref(), "archives", &path, 2).await;
}

#[tokio::test]
async fn live_task_lock_prevents_reclaim_then_retries_are_bounded_and_sources_survive() {
    let (pool, _db) = setup_db_with_table("stale_task").await;
    let (_dir, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    seed(
        &pool,
        "stale_task",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    let queue = Queue::new(pool.clone());
    let plan = planner(&pool, "stale_task", registry.clone(), true);
    plan.plan_once().await.unwrap();
    let first = queue.claim_tasks("", "old", 1).await.unwrap().remove(0);
    sqlx::query("UPDATE _task_queue SET claimed_at=1 WHERE task_id=?")
        .bind(first.task_id)
        .execute(&pool)
        .await
        .unwrap();
    let mut lock = queue.task_lock(first.task_id).await.unwrap();
    plan.plan_once().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM _task_queue WHERE task_id=?")
            .bind(first.task_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "processing"
    );
    lock.release().await.unwrap();
    plan.plan_once().await.unwrap();
    execute_task(
        &queue,
        &ArchiveCreator::new(
            registry.clone(),
            "/no/such/executable",
            Duration::from_secs(1),
        ),
        first.clone(),
    )
    .await;
    assert_eq!(
        queue.complete_task(first.task_id, b"late").await.unwrap(),
        0
    );
    for retry in 1..=3 {
        let task = queue
            .claim_tasks("", "replacement", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(task.retry_count, retry);
        queue.fail_task(task.task_id).await.unwrap();
        plan.plan_once().await.unwrap();
    }
    assert_eq!(queue.count_active_tasks("stale_task").await.unwrap(), 0);
    plan.plan_once().await.unwrap();
    assert_eq!(
        queue.count_active_tasks("stale_task").await.unwrap(),
        0,
        "dead letters must not be replanned forever"
    );
    assert!(backend.exists("logs", "one.ir").await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _task_queue WHERE state='dead_letter'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn shared_input_is_kept_until_other_tables_release_it() {
    let (pool, _db) = setup_db_with_table("shared_input").await;
    metalog_schema::ensure_table(&pool, "other", None)
        .await
        .unwrap();
    let (_dir, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    seed(
        &pool,
        "shared_input",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    seed(
        &pool,
        "other",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_CLOSED",
    )
    .await;
    let queue = Queue::new(pool.clone());
    let plan = planner(&pool, "shared_input", registry.clone(), true);
    plan.plan_once().await.unwrap();
    let task = queue.claim_tasks("", "w", 1).await.unwrap().remove(0);
    execute_task(
        &queue,
        &ArchiveCreator::new(registry.clone(), clp::binary(), Duration::from_secs(30)),
        task,
    )
    .await;
    plan.plan_once().await.unwrap();
    assert!(backend.exists("logs", "one.ir").await.unwrap());
    sqlx::query("UPDATE other SET expires_at=1")
        .execute(&pool)
        .await
        .unwrap();
    RetentionModule::new()
        .run_once(
            &FileRecords::new(pool.clone(), "other"),
            Some(&registry),
            0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(!backend.exists("logs", "one.ir").await.unwrap());
    let path = archive_path(&pool, "shared_input").await;
    assert!(backend.exists("archives", &path).await.unwrap());
}

#[tokio::test]
async fn subprocess_failure_and_timeout_never_publish_placeholder_archives() {
    use std::os::unix::fs::PermissionsExt;
    let (pool, _db) = setup_db_with_table("timeout_task").await;
    let (dir, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    seed(
        &pool,
        "timeout_task",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    let queue = Queue::new(pool.clone());
    let plan = planner(&pool, "timeout_task", registry.clone(), true);
    plan.plan_once().await.unwrap();
    let script = dir.path().join("timeout.sh");
    std::fs::write(&script, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let task = queue.claim_tasks("", "w", 1).await.unwrap().remove(0);
    execute_task(
        &queue,
        &ArchiveCreator::new(registry.clone(), script, Duration::from_millis(100)),
        task.clone(),
    )
    .await;
    let output: Vec<u8> = sqlx::query_scalar("SELECT output FROM _task_queue WHERE task_id=?")
        .bind(task.task_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(unmarshal_result(&output)
        .unwrap()
        .error
        .contains("exceeded"));
    plan.plan_once().await.unwrap();
    assert!(backend.exists("logs", "one.ir").await.unwrap());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM timeout_task WHERE state='ARCHIVE_CLOSED'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[tokio::test]
async fn minio_real_ir_and_file_chains_upload_read_and_finalize() {
    use aws_sdk_s3::{
        config::{BehaviorVersion, Credentials, Region},
        Client,
    };
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        GenericImage,
    };
    let minio = GenericImage::new("metalog-minio-test", "2025-10-15")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::healthcheck())
        .start()
        .await
        .unwrap();
    let endpoint = format!(
        "http://127.0.0.1:{}",
        minio.get_host_port_ipv4(9000).await.unwrap()
    );
    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "test",
            ))
            .endpoint_url(&endpoint)
            .force_path_style(true)
            .build(),
    );
    for bucket in ["input-one", "input-two", "archives"] {
        client.create_bucket().bucket(bucket).send().await.unwrap();
    }
    let backend = Arc::new(
        metalog_storage::S3Backend::new(&metalog_config::StorageBackendConfig {
            type_name: "minio".into(),
            bucket: "archives".into(),
            base_url: String::new(),
            base_path: String::new(),
            endpoint,
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            session_token: String::new(),
            region: "us-east-1".into(),
            force_path_style: true,
        })
        .await
        .unwrap(),
    );
    let mut registry = Registry::new();
    registry.register("local", backend.clone());
    let registry = Arc::new(registry);
    for (table, pending, buffering, one, two, extension) in [
        (
            "s3_ir",
            FileState::IrArchiveConsolidationPending,
            FileState::IrArchiveBuffering,
            ONE,
            TWO,
            "ir",
        ),
        (
            "s3_file",
            FileState::FileArchiveConsolidationPending,
            FileState::FileArchiveBuffering,
            include_bytes!("../../fixtures/consolidation/one.jsonl").as_slice(),
            include_bytes!("../../fixtures/consolidation/two.jsonl").as_slice(),
            "jsonl",
        ),
    ] {
        let (pool, _db) = setup_db_with_table(table).await;
        // A valid MinIO component can exceed NAME_MAX once a local ordinal is added.
        let basename = "日本語".repeat(27);
        let padding = "x".repeat(254 - basename.len() - 1 - extension.len());
        let path = format!("{table}/{basename}{padding}.{extension}");
        backend.put("input-one", &path, one).await.unwrap();
        backend.put("input-two", &path, two).await.unwrap();
        let now = epoch_nanos();
        seed(
            &pool,
            table,
            now,
            1,
            "input-one",
            &path,
            pending.as_db_str(),
        )
        .await;
        seed(
            &pool,
            table,
            now + 1,
            2,
            "input-two",
            &path,
            pending.as_db_str(),
        )
        .await;
        let queue = Arc::new(Queue::new(pool.clone()));
        let plan = Planner::new(PlannerConfig {
            file_recs: Arc::new(FileRecords::new(pool.clone(), table)),
            queue: queue.clone(),
            policy: Arc::new(TimeWindowPolicy::new(Duration::from_secs(86400), 2, 100)),
            in_flight: Arc::new(InFlightSet::new()),
            table_name: table.into(),
            archive_backend: "local".into(),
            archive_bucket: "archives".into(),
            interval: Duration::from_secs(1),
            stale_threshold: Duration::ZERO,
            buffering_state: buffering,
            pending_state: pending,
        })
        .with_storage(registry.clone());
        plan.plan_once().await.unwrap();
        let task = queue.claim_tasks("", "worker", 1).await.unwrap().remove(0);
        execute_task(
            &queue,
            &ArchiveCreator::new(registry.clone(), clp::binary(), Duration::from_secs(30)),
            task,
        )
        .await;
        plan.plan_once().await.unwrap();
        let archive = archive_path(&pool, table).await;
        verify_archive(backend.as_ref(), "archives", &archive, 4).await;
        assert!(!backend.exists("input-one", &path).await.unwrap());
        assert!(!backend.exists("input-two", &path).await.unwrap());
    }
}

#[tokio::test]
async fn spark_policy_reads_dimensions_and_keeps_backends_separate() {
    let (pool, _db) = setup_db_with_table("spark_policy").await;
    let (_dir, backend, mut registry) = storage();
    Arc::get_mut(&mut registry)
        .unwrap()
        .register("second", backend);
    let columns = metalog_schema::ColumnRegistry::new(pool.clone(), "spark_policy")
        .await
        .unwrap();
    let col = columns
        .resolve_or_allocate_dim("application_id", "str", 256)
        .await
        .unwrap();
    let now = epoch_nanos();
    for id in 1..=4 {
        seed(
            &pool,
            "spark_policy",
            now + id,
            id,
            "logs",
            &format!("{id}.ir"),
            "IR_ARCHIVE_CONSOLIDATION_PENDING",
        )
        .await;
        sqlx::query(&format!("UPDATE spark_policy SET `{col}` = ? WHERE id = ?"))
            .bind(if id % 2 == 1 { "job-a" } else { "job-b" })
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    let config = serde_json::from_value(serde_json::json!({
        "policies": [{
            "type": "spark_job",
            "config": {"grouping_dim_key": "application_id", "min_files": 2, "max_files": 2}
        }]
    }))
    .unwrap();
    let queue = Arc::new(Queue::new(pool.clone()));
    let plan = Planner::new(PlannerConfig {
        file_recs: Arc::new(FileRecords::new(pool.clone(), "spark_policy")),
        queue: queue.clone(),
        policy: metalog_consolidation::build_policy(&config).unwrap(),
        in_flight: Arc::new(InFlightSet::new()),
        table_name: "spark_policy".into(),
        archive_backend: "local".into(),
        archive_bucket: "archives".into(),
        interval: Duration::from_secs(1),
        stale_threshold: Duration::ZERO,
        buffering_state: FileState::IrArchiveBuffering,
        pending_state: FileState::IrArchiveConsolidationPending,
    })
    .with_storage(registry);
    // Different backends must not be merged into a task with a single ir_backend.
    sqlx::query("UPDATE spark_policy SET file_storage_backend='second' WHERE id=1")
        .execute(&pool)
        .await
        .unwrap();
    plan.plan_once().await.unwrap();
    let tasks = queue.claim_tasks("", "worker", 10).await.unwrap();
    assert_eq!(tasks.len(), 1);
    let input = unmarshal_payload(&tasks[0].input)
        .unwrap()
        .consolidation
        .unwrap();
    assert_eq!(input.file_ids, [2, 4]);
    sqlx::query("UPDATE spark_policy SET file_storage_backend='local' WHERE id=1")
        .execute(&pool)
        .await
        .unwrap();
    plan.plan_once().await.unwrap();
    let tasks = queue.claim_tasks("", "worker", 10).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        unmarshal_payload(&tasks[0].input)
            .unwrap()
            .consolidation
            .unwrap()
            .file_ids,
        [1, 3]
    );
}

#[tokio::test]
async fn queued_go_payload_is_adopted_and_compressed_with_durable_reservations() {
    let (pool, _db) = setup_db_with_table("go_tasks").await;
    let (_dir, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    backend.put("logs", "two.ir", TWO).await.unwrap();
    let now = epoch_nanos();
    seed(
        &pool,
        "go_tasks",
        now,
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    seed(
        &pool,
        "go_tasks",
        now + 1,
        2,
        "logs",
        "two.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    let queue = Queue::new(pool.clone());
    queue
        .create_tasks(
            "go_tasks",
            1,
            &[include_bytes!("../../fixtures/consolidation/go-payload.lz4").to_vec()],
        )
        .await
        .unwrap();
    let task = queue.claim_tasks("", "worker", 1).await.unwrap().remove(0);
    execute_task(
        &queue,
        &ArchiveCreator::new(registry.clone(), clp::binary(), Duration::from_secs(30)),
        task,
    )
    .await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _consolidation_file")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    planner(&pool, "go_tasks", registry, false)
        .plan_once()
        .await
        .unwrap();
    let archive = archive_path(&pool, "go_tasks").await;
    verify_archive(backend.as_ref(), "archives", &archive, 4).await;
}

#[tokio::test]
async fn worker_only_node_serves_many_coordinators_with_a_small_pool() {
    let (initial, db) = metalog_it::helpers::setup_db().await;
    let (scratch, backend, registry) = storage();
    let mut connection = db.writer_config();
    connection.pool_size = 5;
    let pool = metalog_db::new_pool(&connection).await.unwrap();
    initial.close().await;
    let tables: Vec<_> = (0..6).map(|i| format!("many_tables_{i}")).collect();
    for (index, table) in tables.iter().enumerate() {
        metalog_schema::ensure_table(
            &pool,
            table,
            Some(
                r#"{"consolidation":{"enabled":true,"policies":[
            {"type":"time_window","config":{"min_files":1,"window_size":"24h"}}
        ]}}"#,
            ),
        )
        .await
        .unwrap();
        let key = format!("{table}.ir");
        backend.put("logs", &key, ONE).await.unwrap();
        seed(
            &pool,
            table,
            epoch_nanos(),
            index as i64 + 1,
            "logs",
            &key,
            "IR_ARCHIVE_CONSOLIDATION_PENDING",
        )
        .await;
    }
    let mut config: metalog_config::NodeConfig = serde_json::from_value(serde_json::json!({
        "database":{"primary":{"database":"test","pool_size":5}},
        "storage": {
            "default_backend": "local",
            "backends": {"local": {
                "type": "filesystem", "bucket": "archives", "base_path": scratch.path()
            }}
        },
        "worker":{"concurrency":1,"clp_binary_path":clp::binary()}
    }))
    .unwrap();
    config.database.primary = connection;
    config.validate().unwrap();
    assert!(!config.coordinator.enabled);
    let mut undersized = config.database.primary.clone();
    undersized.pool_size = 2;
    let small_pool = metalog_db::new_pool(&undersized).await.unwrap();
    let mut coordinator_config = config.clone();
    coordinator_config.worker.concurrency = 0;
    coordinator_config.coordinator.enabled = true;
    let mut invalid_node = metalog_node::NodeBuilder::new(
        coordinator_config,
        Arc::new(metalog_node::Resources::new(
            small_pool.clone(),
            true,
            Duration::from_secs(60),
        )),
    )
    .with_storage(registry.clone())
    .build();
    let error = invalid_node.start(&tables[..1]).await.unwrap_err();
    assert!(error.to_string().contains("four primary pool connections"));
    invalid_node.stop().await;
    small_pool.close().await;
    let mut workers = metalog_node::NodeBuilder::new(
        config.clone(),
        Arc::new(metalog_node::Resources::new(
            pool.clone(),
            true,
            Duration::from_secs(60),
        )),
    )
    .with_storage(registry.clone())
    .build();
    config.worker.concurrency = 0;
    config.coordinator.enabled = true;
    let mut coordinators = metalog_node::NodeBuilder::new(
        config,
        Arc::new(metalog_node::Resources::new(
            pool.clone(),
            true,
            Duration::from_secs(60),
        )),
    )
    .with_storage(registry)
    .build();
    workers.start(&[]).await.unwrap();
    coordinators.start(&tables).await.unwrap();
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let mut completed = 0;
            for table in &tables {
                let count: i64 = sqlx::query_scalar(&format!(
                    "SELECT COUNT(*) FROM `{table}` WHERE state='ARCHIVE_CLOSED'"
                ))
                .fetch_one(&pool)
                .await
                .unwrap();
                if count == 1
                    && !backend
                        .exists("logs", &format!("{table}.ir"))
                        .await
                        .unwrap()
                {
                    completed += 1;
                }
            }
            if completed == tables.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    workers.stop().await;
    coordinators.stop().await;
}

#[tokio::test]
async fn missing_uploaded_archive_is_rebuilt_before_sources_are_removed() {
    let (pool, _db) = setup_db_with_table("missing_archive").await;
    let (_scratch, backend, registry) = storage();
    backend.put("logs", "one.ir", ONE).await.unwrap();
    seed(
        &pool,
        "missing_archive",
        epoch_nanos(),
        1,
        "logs",
        "one.ir",
        "IR_ARCHIVE_CONSOLIDATION_PENDING",
    )
    .await;
    let queue = Queue::new(pool.clone());
    let plan = planner(&pool, "missing_archive", registry.clone(), true);
    plan.plan_once().await.unwrap();
    let first = queue.claim_tasks("", "worker", 1).await.unwrap().remove(0);
    let input = unmarshal_payload(&first.input)
        .unwrap()
        .consolidation
        .unwrap();
    let creator = ArchiveCreator::new(registry, clp::binary(), Duration::from_secs(30));
    execute_task(&queue, &creator, first.clone()).await;
    backend
        .delete("archives", &input.attempt_path(first.task_id))
        .await
        .unwrap();
    plan.plan_once().await.unwrap();
    assert!(backend.exists("logs", "one.ir").await.unwrap());
    let next = queue
        .claim_tasks("", "replacement", 1)
        .await
        .unwrap()
        .remove(0);
    assert_ne!(next.task_id, first.task_id);
    execute_task(&queue, &creator, next.clone()).await;
    plan.plan_once().await.unwrap();
    assert_eq!(
        archive_path(&pool, "missing_archive").await,
        input.attempt_path(next.task_id)
    );
    verify_archive(
        backend.as_ref(),
        "archives",
        &input.attempt_path(next.task_id),
        2,
    )
    .await;
    assert!(!backend.exists("logs", "one.ir").await.unwrap());
}

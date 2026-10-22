use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use metalog_it::helpers::setup_db_with_table;
use metalog_metastore::{FileRecords, StorageMutationGuard, StoragePath};
use metalog_retention::{RetentionModule, RetentionPolicy};
use metalog_storage::{Backend, FilesystemBackend, Registry, StorageError};
use metalog_timeutil::epoch_nanos;
use sqlx::MySqlPool;
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;

struct FaultBackend {
    inner: FilesystemBackend,
    fail_archive: AtomicBool,
    block_delete: AtomicBool,
    delete_started: tokio::sync::Notify,
}

#[tonic::async_trait]
impl Backend for FaultBackend {
    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
        self.inner.get(bucket, key).await
    }

    async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StorageError> {
        self.inner.put(bucket, key, data).await
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        self.delete_started.notify_one();
        if self.block_delete.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if key == "archive" && self.fail_archive.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Other(
                "injected transient deletion failure".into(),
            ));
        }
        self.inner.delete(bucket, key).await
    }

    async fn exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        self.inner.exists(bucket, key).await
    }
}

fn backend(dir: &tempfile::TempDir) -> Arc<FaultBackend> {
    Arc::new(FaultBackend {
        inner: FilesystemBackend::new(dir.path().to_str().unwrap()),
        fail_archive: AtomicBool::new(false),
        block_delete: AtomicBool::new(false),
        delete_started: tokio::sync::Notify::new(),
    })
}

fn registry(backend: Arc<FaultBackend>) -> Registry {
    let mut registry = Registry::new();
    registry.register("fs", backend);
    registry
}

async fn insert_file(
    pool: &MySqlPool,
    table: &str,
    timestamp: i64,
    expiry: i64,
    file: Option<&str>,
    archive: Option<&str>,
) {
    let sql = format!(
        "INSERT INTO `{table}` (min_timestamp, max_timestamp, state, expires_at, retention_days, \
         file_storage_backend, file_bucket, file_path, archive_storage_backend, archive_bucket, \
         archive_path) VALUES (?, ?, ?, ?, 30, 'fs', 'logs', ?, 'fs', 'logs', ?)"
    );
    sqlx::query(&sql)
        .bind(timestamp)
        .bind(timestamp)
        .bind(if archive.is_some() {
            "ARCHIVE_CLOSED"
        } else {
            "IR_CLOSED"
        })
        .bind(expiry)
        .bind(file)
        .bind(archive)
        .execute(pool)
        .await
        .unwrap();
}

async fn count(pool: &MySqlPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM `{table}`"))
        .fetch_one(pool)
        .await
        .unwrap()
}

fn module() -> RetentionModule {
    RetentionModule::with_config(Duration::from_secs(60), u32::MAX)
}

#[tokio::test]
async fn legacy_storage_aliases_in_any_table_block_retention_before_state_changes() {
    let (pool, _container) = setup_db_with_table("retention_alias_expired").await;
    metalog_schema::ensure_table(&pool, "retention_alias_live", None)
        .await
        .unwrap();
    // Inactive tables still hold live storage references.
    sqlx::query("UPDATE _table SET active = FALSE WHERE table_name = 'retention_alias_live'")
        .execute(&pool)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.put("logs", "a", b"shared object").await.unwrap();
    let registry = registry(storage.clone());
    let files = FileRecords::new(pool.clone(), "retention_alias_expired");
    let token = CancellationToken::new();
    let retention = module();
    let now = epoch_nanos();

    for (expired_path, live_path) in [
        ("a", "/a"),
        ("/a", "a"),
        ("a", "./a"),
        ("a", "a/../a"),
        ("a", "a//part"),
        ("a", "a/"),
        ("a", r"a\part"),
    ] {
        insert_file(
            &pool,
            "retention_alias_expired",
            now,
            now - 1_000_000_000,
            Some(expired_path),
            None,
        )
        .await;
        insert_file(
            &pool,
            "retention_alias_live",
            now,
            now + 60_000_000_000,
            None,
            Some(live_path),
        )
        .await;
        let error = retention
            .run_once(&files, Some(&registry), 0, &token)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("noncanonical storage location"));
        assert!(storage.exists("logs", "a").await.unwrap());
        let state: String =
            sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM retention_alias_expired")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "IR_CLOSED", "audit must precede state transitions");
        assert_eq!(count(&pool, "retention_alias_live").await, 1);
        let tombstones: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _storage_tombstone")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(tombstones, 0);
        sqlx::query("DELETE FROM retention_alias_expired")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM retention_alias_live")
            .execute(&pool)
            .await
            .unwrap();
    }

    // Native AWS keys remain verbatim, but unresolved bucket/backend identities
    // still block retention even when no backend requires canonical key syntax.
    insert_file(
        &pool,
        "retention_alias_live",
        now,
        now + 60_000_000_000,
        None,
        Some("/a"),
    )
    .await;
    files.audit_canonical_storage_locations(&[]).await.unwrap();
    sqlx::query("UPDATE retention_alias_live SET archive_bucket = NULL")
        .execute(&pool)
        .await
        .unwrap();
    // PAD SPACE collations compare an all-space key equal to ''. It is still
    // a real object identity and must not evade the unresolved-bucket audit.
    sqlx::query("UPDATE retention_alias_live SET archive_path = ' '")
        .execute(&pool)
        .await
        .unwrap();
    assert!(files.audit_canonical_storage_locations(&[]).await.is_err());
    assert!(files.audit_canonical_storage_locations(&[]).await.is_err());
    sqlx::query("UPDATE retention_alias_live SET archive_bucket = 'logs', archive_path = 'a'")
        .execute(&pool)
        .await
        .unwrap();
    for bucket in ["", "logs/nested", ".", "..", r"logs\nested"] {
        sqlx::query("UPDATE retention_alias_live SET archive_bucket = ?")
            .bind(bucket)
            .execute(&pool)
            .await
            .unwrap();
        assert!(files
            .audit_canonical_storage_locations(&["fs".to_owned()])
            .await
            .is_err());
    }
    sqlx::query(
        "UPDATE retention_alias_live SET archive_bucket = 'logs', archive_storage_backend = NULL",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(files.audit_canonical_storage_locations(&[]).await.is_err());
    sqlx::query("DELETE FROM retention_alias_live")
        .execute(&pool)
        .await
        .unwrap();

    // Clearing the legacy ambiguity permits a later cycle without a restart.
    insert_file(
        &pool,
        "retention_alias_expired",
        now,
        now - 1_000_000_000,
        Some("a"),
        None,
    )
    .await;
    retention
        .run_once(&files, Some(&registry), 0, &token)
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_alias_expired").await, 0);
    assert!(!storage.exists("logs", "a").await.unwrap());
}

#[tokio::test]
async fn partial_storage_failure_keeps_durable_retry_state_across_restart() {
    let (pool, _container) = setup_db_with_table("retention_retry").await;
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.fail_archive.store(true, Ordering::SeqCst);
    storage.put("logs", "ir", b"ir").await.unwrap();
    storage.put("logs", "archive", b"archive").await.unwrap();
    let registry = registry(storage.clone());
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_retry",
        now,
        now - 1_000_000_000,
        Some("ir"),
        Some("archive"),
    )
    .await;
    let files = FileRecords::new(pool.clone(), "retention_retry");
    let token = CancellationToken::new();
    module()
        .run_once(&files, Some(&registry), 0, &token)
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_retry").await, 1);
    let state: String = sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM retention_retry")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "ARCHIVE_PURGING");
    assert!(!storage.exists("logs", "ir").await.unwrap());
    assert!(storage.exists("logs", "archive").await.unwrap());
    // A new module models process restart: retries come from metadata, not RAM.
    module()
        .run_once(&files, Some(&registry), 0, &token)
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_retry").await, 0);
    assert!(!storage.exists("logs", "archive").await.unwrap());
}

#[tokio::test]
async fn cancellation_during_storage_io_keeps_purging_row_for_retry() {
    let (pool, _container) = setup_db_with_table("retention_cancel").await;
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.block_delete.store(true, Ordering::SeqCst);
    storage.put("logs", "ir", b"ir").await.unwrap();
    let registry = registry(storage.clone());
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_cancel",
        now,
        now - 1_000_000_000,
        Some("ir"),
        None,
    )
    .await;
    let files = FileRecords::new(pool.clone(), "retention_cancel");
    let token = CancellationToken::new();
    let retention = module();
    let run = retention.run_once(&files, Some(&registry), 0, &token);
    tokio::pin!(run);
    tokio::select! {
        _ = storage.delete_started.notified() => {},
        result = &mut run => { panic!("retention ended before blocking deletion: {result:?}"); },
        _ = tokio::time::sleep(Duration::from_secs(5)) => { panic!("deletion never started"); },
    }
    // Retirement is durable before the network request begins. The short SQL
    // guard is already released, but an arriving reference must be rejected.
    let mut guard =
        tokio::time::timeout(Duration::from_secs(1), StorageMutationGuard::acquire(&pool))
            .await
            .unwrap()
            .unwrap();
    assert!(guard
        .ensure_writable(&[StoragePath {
            backend: "fs".into(),
            bucket: "logs".into(),
            path: "ir".into(),
        }])
        .await
        .is_err());
    guard.release().await.unwrap();
    token.cancel();
    tokio::time::timeout(Duration::from_secs(1), &mut run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count(&pool, "retention_cancel").await, 1);
    storage.block_delete.store(false, Ordering::SeqCst);
    module()
        .run_once(&files, Some(&registry), 0, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_cancel").await, 0);
}

#[tokio::test]
async fn reference_committed_before_retirement_protects_the_shared_object() {
    let (pool, _container) = setup_db_with_table("retention_race").await;
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.put("logs", "shared", b"shared").await.unwrap();
    let registry = Arc::new(registry(storage.clone()));
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_race",
        now,
        now - 1_000_000_000,
        Some("shared"),
        None,
    )
    .await;
    let mut guard = StorageMutationGuard::acquire(&pool).await.unwrap();
    let run_pool = pool.clone();
    let run_registry = registry.clone();
    let handle = tokio::spawn(async move {
        module()
            .run_once(
                &FileRecords::new(run_pool, "retention_race"),
                Some(&run_registry),
                0,
                &CancellationToken::new(),
            )
            .await
    });
    // This models ingestion committing while it owns the reference-mutation
    // guard. Retirement cannot inspect references until this commit completes.
    guard
        .ensure_writable(&[StoragePath {
            backend: "fs".into(),
            bucket: "logs".into(),
            path: "shared".into(),
        }])
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO retention_race (min_timestamp, max_timestamp, state, expires_at, \
         retention_days, file_storage_backend, file_bucket, file_path) VALUES (?, ?, 'IR_CLOSED', \
         ?, 30, 'fs', 'logs', 'shared')",
    )
    .bind(now + 1)
    .bind(now + 1)
    .bind(now + 60_000_000_000)
    .execute(guard.connection())
    .await
    .unwrap();
    guard.release().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(count(&pool, "retention_race").await, 1);
    assert!(storage.exists("logs", "shared").await.unwrap());
    let retired: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _storage_tombstone")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(retired, 0);
}

#[tokio::test]
async fn shared_archive_and_ir_references_are_protected_across_tables() {
    let (pool, _container) = setup_db_with_table("retention_old").await;
    metalog_schema::ensure_table(&pool, "retention_live", None)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    for key in ["shared_ir", "shared_archive"] {
        storage.put("logs", key, b"shared").await.unwrap();
    }
    let registry = registry(storage.clone());
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_old",
        now,
        now - 1_000_000_000,
        Some("shared_ir"),
        Some("shared_archive"),
    )
    .await;
    // The same object can appear in the opposite field in another table.
    insert_file(
        &pool,
        "retention_live",
        now + 1,
        now + 60_000_000_000,
        Some("shared_archive"),
        Some("shared_ir"),
    )
    .await;
    module()
        .run_once(
            &FileRecords::new(pool.clone(), "retention_old"),
            Some(&registry),
            0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_old").await, 0);
    assert_eq!(count(&pool, "retention_live").await, 1);
    assert!(storage.exists("logs", "shared_ir").await.unwrap());
    assert!(storage.exists("logs", "shared_archive").await.unwrap());
    sqlx::query("UPDATE retention_live SET expires_at = 1")
        .execute(&pool)
        .await
        .unwrap();
    module()
        .run_once(
            &FileRecords::new(pool.clone(), "retention_live"),
            Some(&registry),
            0,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_live").await, 0);
    assert!(!storage.exists("logs", "shared_ir").await.unwrap());
    assert!(!storage.exists("logs", "shared_archive").await.unwrap());
}

#[tokio::test]
async fn crash_after_object_deletion_recovers_from_the_purging_row() {
    let (pool, _container) = setup_db_with_table("retention_crash").await;
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    let registry = registry(storage.clone());
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_crash",
        now,
        now - 1_000_000_000,
        Some("already_deleted"),
        None,
    )
    .await;
    let files = FileRecords::new(pool.clone(), "retention_crash");
    files.transition_expired_to_purging(now).await.unwrap();
    // Object already gone, but crash happened before metadata commit.
    module()
        .run_once(&files, Some(&registry), 0, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_crash").await, 0);
}

#[tokio::test]
async fn unavailable_backend_keeps_metadata_and_grace_period_delays_deletion() {
    let (pool, _container) = setup_db_with_table("retention_grace").await;
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.put("logs", "ir", b"ir").await.unwrap();
    let registry = registry(storage.clone());
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_grace",
        now,
        now - 1_000_000_000,
        Some("ir"),
        None,
    )
    .await;
    let files = FileRecords::new(pool.clone(), "retention_grace");
    let token = CancellationToken::new();
    assert!(module().run_once(&files, None, 0, &token).await.is_err());
    module()
        .run_once(&files, Some(&registry), 60_000_000_000, &token)
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT CAST(state AS CHAR) FROM retention_grace")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "IR_CLOSED");
    module()
        .run_once(&files, Some(&Registry::new()), 0, &token)
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_grace").await, 1);
    assert!(storage.exists("logs", "ir").await.unwrap());
    module()
        .run_once(&files, Some(&registry), 0, &token)
        .await
        .unwrap();
    assert_eq!(count(&pool, "retention_grace").await, 0);
    assert!(!storage.exists("logs", "ir").await.unwrap());
}

#[tokio::test]
async fn configured_coordinator_runs_retention_and_joins_on_stop() {
    let (pool, _container) = setup_db_with_table("retention_node").await;
    sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = 'retention_node'")
        .bind(r#"{"retention":{"enabled":true,"scan_interval_secs":1,"delete_rate":1000}}"#)
        .execute(&pool)
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let storage = backend(&dir);
    storage.put("logs", "ir", b"ir").await.unwrap();
    let registry = Arc::new(registry(storage.clone()));
    let now = epoch_nanos();
    insert_file(
        &pool,
        "retention_node",
        now,
        now - 1_000_000_000,
        Some("ir"),
        None,
    )
    .await;
    let resources = Arc::new(metalog_node::Resources::new(
        pool.clone(),
        true,
        Duration::from_secs(60),
    ));
    let mut node = metalog_node::NodeBuilder::new(
        serde_json::from_value(serde_json::json!({"database": {"primary": {"database": "test"}}}))
            .unwrap(),
        resources.clone(),
    )
    .with_storage(registry)
    .build();
    node.start(&["retention_node".into()]).await.unwrap();
    let unit = resources.coordinator("retention_node").unwrap();
    assert!(unit.table_config().retention.enabled);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if count(&pool, "retention_node").await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!storage.exists("logs", "ir").await.unwrap());
    node.stop().await;
    assert!(unit.token().is_cancelled());
    assert!(resources.coordinator("retention_node").is_none());
}

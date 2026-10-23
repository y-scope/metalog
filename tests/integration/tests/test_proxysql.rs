use std::{collections::HashMap, time::Duration};

use metalog_it::{
    helpers::setup_proxy_db,
    proxysql::{READER_BACKEND_LIMIT, READER_USER, WRITER_USER},
};
use metalog_metastore::{AdvisoryLock, AdvisoryLockError};
use sqlx::{mysql::MySqlRow, Executor, MySqlPool, Row};
use tokio::time::{sleep, timeout, Instant};

async fn owner(pool: &MySqlPool, name: &str) -> Option<u64> {
    sqlx::query_scalar("SELECT CAST(IS_USED_LOCK(?) AS UNSIGNED)")
        .bind(name)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn wait_for_release(pool: &MySqlPool, name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let held_by = owner(pool, name).await;
        if held_by.is_none() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "backend session {held_by:?} retained lock {name} after frontend disconnect"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

// ProxySQL's admin listener uses the text protocol and may report text columns
// with binary metadata. Decode its actual UTF-8 bytes in either representation.
fn admin_text(row: &MySqlRow, column: usize) -> String {
    row.try_get::<String, _>(column)
        .unwrap_or_else(|_| String::from_utf8(row.try_get::<Vec<u8>, _>(column).unwrap()).unwrap())
}

#[tokio::test]
async fn writer_prepared_locks_and_text_writes_share_one_backend_session() {
    let (writer, database) = setup_proxy_db().await;
    let direct = database.direct_pool().await;
    let proxy_admin = database.proxysql().unwrap().admin_pool().await;
    let (kind, version) = metalog_db::detect_database_type(&writer).await.unwrap();
    assert!(kind.is_mariadb(), "backend dialect was masked: {version}");

    let variables: HashMap<_, _> = proxy_admin
        .fetch_all(
            "SELECT variable_name, variable_value FROM runtime_global_variables WHERE \
             variable_name IN ('mysql-query_retries_on_failure', \
             'mysql-connect_retries_on_failure', 'mysql-multiplexing', \
             'mysql-kill_backend_connection_when_disconnect', 'mysql-select_version_forwarding', \
             'mysql-query_processor_first_comment_parsing')",
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (admin_text(row, 0), admin_text(row, 1)))
        .collect();
    for name in [
        "mysql-query_retries_on_failure",
        "mysql-connect_retries_on_failure",
        "mysql-query_processor_first_comment_parsing",
    ] {
        assert_eq!(variables.get(name).map(String::as_str), Some("0"));
    }
    for name in [
        "mysql-multiplexing",
        "mysql-kill_backend_connection_when_disconnect",
    ] {
        assert!(matches!(
            variables.get(name).map(String::as_str),
            Some("true" | "1")
        ));
    }
    assert_eq!(
        variables
            .get("mysql-select_version_forwarding")
            .map(String::as_str),
        Some("1")
    );
    let policy = proxy_admin
        .fetch_one(
            "SELECT COUNT(*) FROM runtime_mysql_query_rules WHERE username='metalog_writer' AND \
             destination_hostgroup=10 AND multiplex=0 AND retries=0 AND cache_ttl IS NULL",
        )
        .await
        .unwrap();
    let rules: Vec<Vec<String>> = proxy_admin
        .fetch_all(
            "SELECT rule_id, username, COALESCE(destination_hostgroup,-1), \
             COALESCE(multiplex,-1), COALESCE(retries,-1), COALESCE(cache_ttl,-1) FROM \
             runtime_mysql_query_rules",
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (0..6).map(|index| admin_text(row, index)).collect())
        .collect();
    assert_eq!(admin_text(&policy, 0), "1", "runtime rules: {rules:?}");

    writer
        .execute("CREATE TABLE proxy_guard_probe (backend_id BIGINT UNSIGNED NOT NULL)")
        .await
        .unwrap();
    let mut guard = AdvisoryLock::acquire(&writer, "proxy_affinity", 1)
        .await
        .unwrap();
    let backend_id = owner(&direct, "proxy_affinity").await.unwrap();
    // A server-side expression in a write cannot be answered using a proxy
    // frontend connection ID. This is the same COM_QUERY path as ingestion.
    guard
        .connection()
        .execute("INSERT INTO proxy_guard_probe VALUES (CONNECTION_ID())")
        .await
        .unwrap();
    let prepared_owner: Option<u64> =
        sqlx::query_scalar("SELECT CAST(IS_USED_LOCK(?) AS UNSIGNED)")
            .bind("proxy_affinity")
            .fetch_one(guard.connection())
            .await
            .unwrap();
    assert_eq!(prepared_owner, Some(backend_id));
    let stored: u64 = sqlx::query_scalar("SELECT backend_id FROM proxy_guard_probe")
        .fetch_one(&direct)
        .await
        .unwrap();
    assert_eq!(stored, backend_id);
    assert!(AdvisoryLock::acquire(&writer, "proxy_affinity", 0)
        .await
        .is_err());
    guard.release().await.unwrap();
    assert_eq!(owner(&direct, "proxy_affinity").await, None);

    // A successful SQL response containing NULL/0 does not confirm release.
    // Model lost ownership explicitly, then require the guard to fail closed.
    let mut lost = AdvisoryLock::acquire(&writer, "proxy_lost_owner", 1)
        .await
        .unwrap();
    let released: Option<i64> = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
        .bind("proxy_lost_owner")
        .fetch_one(lost.connection())
        .await
        .unwrap();
    assert_eq!(released, Some(1));
    let mut other = AdvisoryLock::acquire(&direct, "proxy_lost_owner", 1)
        .await
        .unwrap();
    assert!(matches!(
        lost.release().await,
        Err(AdvisoryLockError::LostOwnership(_))
    ));
    drop(lost);
    assert!(owner(&direct, "proxy_lost_owner").await.is_some());
    other.release().await.unwrap();
}

#[tokio::test]
async fn cancellation_releases_held_and_waiting_backend_locks() {
    let (writer, database) = setup_proxy_db().await;
    let direct = database.direct_pool().await;
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let held_pool = writer.clone();
    let held = tokio::spawn(async move {
        let _guard = AdvisoryLock::acquire(&held_pool, "proxy_cancel_held", 1)
            .await
            .unwrap();
        ready_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready_rx.await.unwrap();
    assert!(owner(&direct, "proxy_cancel_held").await.is_some());
    held.abort();
    assert!(held.await.unwrap_err().is_cancelled());
    wait_for_release(&direct, "proxy_cancel_held").await;

    let mut blocker = AdvisoryLock::acquire(&direct, "proxy_cancel_waiting", 1)
        .await
        .unwrap();
    let waiting_pool = writer.clone();
    let waiting = tokio::spawn(async move {
        AdvisoryLock::acquire(&waiting_pool, "proxy_cancel_waiting", 30).await
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let queries: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER=? AND INFO LIKE \
             '%GET_LOCK%'",
        )
        .bind(WRITER_USER)
        .fetch_one(&direct)
        .await
        .unwrap();
        if queries > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "GET_LOCK did not reach the backend"
        );
        sleep(Duration::from_millis(25)).await;
    }
    waiting.abort();
    assert!(waiting.await.err().unwrap().is_cancelled());
    // Keep the blocker held until the cancelled server-side waiter disappears:
    // observing no owner immediately after release could miss a late acquire.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let queries: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER=? AND INFO LIKE \
             '%GET_LOCK%'",
        )
        .bind(WRITER_USER)
        .fetch_one(&direct)
        .await
        .unwrap();
        if queries == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled GET_LOCK remained active on the backend"
        );
        sleep(Duration::from_millis(25)).await;
    }
    blocker.release().await.unwrap();
    wait_for_release(&direct, "proxy_cancel_waiting").await;
    let mut next = timeout(
        Duration::from_secs(5),
        AdvisoryLock::acquire(&writer, "proxy_cancel_waiting", 3),
    )
    .await
    .unwrap()
    .unwrap();
    next.release().await.unwrap();
}

#[tokio::test]
async fn failed_backend_session_cannot_migrate_a_guarded_write() {
    let (writer, database) = setup_proxy_db().await;
    let direct = database.direct_pool().await;
    writer
        .execute("CREATE TABLE proxy_failed_session (value INT NOT NULL)")
        .await
        .unwrap();
    let mut guard = AdvisoryLock::acquire(&writer, "proxy_failed_backend", 1)
        .await
        .unwrap();
    let backend_id = owner(&direct, "proxy_failed_backend").await.unwrap();
    // This ID comes from the independent database's advisory-lock ownership,
    // never from an untrusted string or a proxy frontend connection identifier.
    direct
        .execute(format!("KILL CONNECTION {backend_id}").as_str())
        .await
        .unwrap();
    assert!(timeout(
        Duration::from_secs(5),
        guard
            .connection()
            .execute("INSERT INTO proxy_failed_session VALUES (1)")
    )
    .await
    .unwrap()
    .is_err());
    assert!(guard.release().await.is_err());
    drop(guard);
    let committed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_failed_session")
        .fetch_one(&direct)
        .await
        .unwrap();
    assert_eq!(
        committed, 0,
        "a protected write was replayed without its lock"
    );
    let mut fresh = AdvisoryLock::acquire(&writer, "proxy_failed_backend", 1)
        .await
        .unwrap();
    fresh.release().await.unwrap();
}

#[tokio::test]
async fn reader_frontends_multiplex_without_writes_or_cached_results() {
    let (writer, database) = setup_proxy_db().await;
    let direct = database.direct_pool().await;
    let proxy_admin = database.proxysql().unwrap().admin_pool().await;
    writer
        .execute("CREATE TABLE proxy_reader_probe (id INT PRIMARY KEY)")
        .await
        .unwrap();
    writer
        .execute("INSERT INTO proxy_reader_probe VALUES (1)")
        .await
        .unwrap();
    let mut reader_config = database.reader_config();
    reader_config.pool_size = 24;
    reader_config.pool_min_idle = 0;
    let readers = metalog_db::new_pool(&reader_config).await.unwrap();
    let mut frontends = Vec::new();
    for _ in 0..24 {
        let mut connection = timeout(Duration::from_secs(5), readers.acquire())
            .await
            .unwrap()
            .unwrap();
        let count: i64 = timeout(
            Duration::from_secs(5),
            sqlx::query_scalar("SELECT COUNT(*) FROM proxy_reader_probe")
                .fetch_one(&mut *connection),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
        frontends.push(connection);
    }
    assert_eq!(readers.size(), 24);
    // Start all prepared requests together, retaining each frontend throughout.
    // The server-side sleep makes demand overlap so the backend cap and its
    // queue are exercised instead of only proving sequential idle sharing.
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(frontends.len()));
    let mut burst = tokio::task::JoinSet::new();
    for mut connection in frontends {
        let barrier = barrier.clone();
        burst.spawn(async move {
            barrier.wait().await;
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM proxy_reader_probe WHERE id > ? AND SLEEP(0.05)=0",
            )
            .bind(0_i32)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
            assert_eq!(count, 1);
            connection
        });
    }
    let mut frontends = Vec::new();
    timeout(Duration::from_secs(10), async {
        while let Some(result) = burst.join_next().await {
            frontends.push(result.unwrap());
        }
    })
    .await
    .expect("reader burst did not drain through the bounded backend pool");
    let backends: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER=?")
            .bind(READER_USER)
            .fetch_one(&direct)
            .await
            .unwrap();
    assert!(
        backends > 0 && backends <= READER_BACKEND_LIMIT as i64,
        "24 idle frontend sessions used {backends} backend reader connections"
    );
    let pool_stats = proxy_admin
        .fetch_one(
            "SELECT SUM(ConnUsed), MAX(MaxConnUsed) FROM stats_mysql_connection_pool WHERE \
             hostgroup=20",
        )
        .await
        .unwrap();
    let used: usize = admin_text(&pool_stats, 0).parse().unwrap();
    let peak: usize = admin_text(&pool_stats, 1).parse().unwrap();
    assert!(used <= READER_BACKEND_LIMIT && peak > 1 && peak <= READER_BACKEND_LIMIT);
    let first = &mut *frontends[0];
    assert!(first
        .execute("INSERT INTO proxy_reader_probe VALUES (999)")
        .await
        .is_err());
    assert!(first
        .execute("CREATE TABLE proxy_reader_forbidden (id INT)")
        .await
        .is_err());
    assert!(writer
        .execute("CREATE USER 'proxy_forbidden_root'@'%' IDENTIFIED BY 'test_only'")
        .await
        .is_err());

    // User-supplied leading comments must not override endpoint routing,
    // disable affinity, enable retries, or cache otherwise fresh reads.
    const ANNOTATED_READ: &str = "/* hostgroup=999;cache_ttl=60000;query_retries=9;\
                                  create_new_connection=1 */ SELECT COUNT(*) FROM \
                                  proxy_reader_probe";
    for connection in &mut frontends {
        let text_count: i64 = connection
            .fetch_one(ANNOTATED_READ)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        let prepared_count: i64 = sqlx::query_scalar(ANNOTATED_READ)
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        assert_eq!((text_count, prepared_count), (1, 1));
    }

    writer
        .execute(
            "/* hostgroup=999;cache_ttl=60000;query_retries=9;create_new_connection=1 */ INSERT \
             INTO proxy_reader_probe VALUES (2)",
        )
        .await
        .unwrap();
    for connection in &mut frontends {
        let count: i64 = sqlx::query_scalar(ANNOTATED_READ)
            .fetch_one(&mut **connection)
            .await
            .unwrap();
        assert_eq!(count, 2, "reader returned stale or unauthorized data");
        let text_count: i64 = connection
            .fetch_one(ANNOTATED_READ)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        assert_eq!(text_count, 2, "text query returned a cached result");
    }

    // Distinct role endpoints must not silently accept the other credential.
    for mut wrong in [database.writer_config(), database.reader_config()] {
        wrong.port = if wrong.user == WRITER_USER {
            database.reader_config().port
        } else {
            database.writer_config().port
        };
        if let Ok(pool) = metalog_db::new_pool(&wrong).await {
            assert!(pool.execute("SELECT 1").await.is_err());
        }
    }
}

#[tokio::test]
async fn existing_pools_reconnect_after_proxy_restart() {
    let (writer, database) = setup_proxy_db().await;
    let direct = database.direct_pool().await;
    let reader = metalog_db::new_pool(&database.reader_config())
        .await
        .unwrap();
    writer
        .execute("CREATE TABLE proxy_restart_probe (id INT PRIMARY KEY)")
        .await
        .unwrap();
    writer
        .execute("INSERT INTO proxy_restart_probe VALUES (1)")
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_restart_probe")
        .fetch_one(&reader)
        .await
        .unwrap();
    assert_eq!(before, 1);
    let guard = AdvisoryLock::acquire(&writer, "proxy_restart_lock", 1)
        .await
        .unwrap();
    let proxy = database.proxysql().unwrap();
    proxy.stop().await;
    wait_for_release(&direct, "proxy_restart_lock").await;
    drop(guard);
    proxy.start().await;
    for pool in [&writer, &reader] {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let result: Result<i64, _> =
                sqlx::query_scalar("SELECT COUNT(*) FROM proxy_restart_probe")
                    .fetch_one(pool)
                    .await;
            if let Ok(count) = result {
                assert_eq!(count, 1);
                break;
            }
            assert!(Instant::now() < deadline, "existing pool did not reconnect");
            sleep(Duration::from_millis(100)).await;
        }
    }
    writer
        .execute("INSERT INTO proxy_restart_probe VALUES (2)")
        .await
        .unwrap();
    let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_restart_probe")
        .fetch_one(&reader)
        .await
        .unwrap();
    assert_eq!(after, 2);
}

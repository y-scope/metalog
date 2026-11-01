#![cfg(unix)]

use std::{
    collections::BTreeSet,
    fs::File,
    net::TcpListener,
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use metalog_config::DatabaseConfig;
use metalog_proto::{
    coordinator::{
        admin_service_client::AdminServiceClient,
        dimension_value,
        metadata_ingestion_service_client::MetadataIngestionServiceClient,
        DimEntry,
        DimensionValue,
        FileFields,
        FileInfo,
        IngestRequest,
        MetadataRecord,
        RegisterTableRequest,
        StringDimension,
    },
    query::{
        split_query_service_client::SplitQueryServiceClient,
        Order,
        OrderBy,
        StreamSplitsRequest,
    },
};
use sqlx::{Executor, MySqlPool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::transport::{Channel, Endpoint};

const TABLE: &str = "production_smoke";

/// Always reap test-owned servers, including assertion failures. Logs go to files
/// so a full pipe cannot stall a server and the failure includes startup context.
struct ServerProcess {
    child: Child,
    node_id: String,
    endpoint: String,
    health_port: Option<u16>,
    log_path: PathBuf,
}

impl ServerProcess {
    fn spawn(
        directory: &Path,
        node_id: &str,
        primary: &DatabaseConfig,
        replica: &DatabaseConfig,
        grpc_port: u16,
        health_port: Option<u16>,
    ) -> Self {
        Self::spawn_with_worker(
            directory,
            node_id,
            primary,
            replica,
            grpc_port,
            health_port,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_with_worker(
        directory: &Path,
        node_id: &str,
        primary: &DatabaseConfig,
        replica: &DatabaseConfig,
        grpc_port: u16,
        health_port: Option<u16>,
        binary: Option<&Path>,
    ) -> Self {
        let storage_dir = directory.join("objects");
        std::fs::create_dir_all(storage_dir.join("smoke")).unwrap();
        let config_path = directory.join(format!("{node_id}.yaml"));
        fn database_yaml(role: &str, config: &DatabaseConfig) -> String {
            let quote = |value: &str| format!("'{}'", value.replace('\'', "''"));
            format!(
                "  {role}:\n    host: {}\n    port: {}\n    database: {}\n    user: {}\n    \
                 password: {}\n    pool_size: 5\n    pool_min_idle: 1\n",
                quote(&config.host),
                config.port,
                quote(&config.database),
                quote(&config.user),
                quote(&config.password),
            )
        }
        let config = format!(
            r#"database:
{}{}storage:
  default_backend: local
  backends:
    local:
      type: filesystem
      base_path: '{}'
      bucket: smoke
grpc:
  port: {grpc_port}
  ingestion: true
  query: true
  admin: true
  metadata: true
coordinator:
  enabled: true
  node_id_env_var: METALOG_SMOKE_NODE
  ha_strategy: lease
  reconciliation_interval_secs: 1
  heartbeat_interval_secs: 1
  dead_node_threshold_secs: 4
  lease_ttl_secs: 4
  lease_renewal_interval_secs: 1
health:
  enabled: {}
  port: {}
worker:
  concurrency: 0
telemetry:
  enabled: false
"#,
            database_yaml("primary", primary),
            database_yaml("replica", replica),
            storage_dir.display(),
            health_port.is_some(),
            health_port.unwrap_or(8081),
        );
        let config = if let Some(binary) = binary {
            config.replace(
                "  concurrency: 0",
                &format!(
                    "  concurrency: 1\n  clp_binary_path: '{}'\n  clp_process_timeout_secs: 300",
                    binary.display()
                ),
            )
        } else {
            config
        };
        std::fs::write(&config_path, config).unwrap();
        let log_path = directory.join(format!("{node_id}.log"));
        let log = File::create(&log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_metalog"))
            .args(["serve", "--config"])
            .arg(config_path)
            // Host DB_* overrides must never redirect a test into an external DB.
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("METALOG_SMOKE_NODE", node_id)
            .env("RUST_BACKTRACE", "1")
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn actual metalog binary");
        Self {
            child,
            node_id: node_id.into(),
            endpoint: format!("http://127.0.0.1:{grpc_port}"),
            health_port,
            log_path,
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    async fn wait_for_health(&mut self, path: &str, expected_status: u16) {
        let port = self.health_port.expect("health listener configured");
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "{} exited while waiting for {path} status {expected_status}\n{}",
                self.node_id,
                self.logs()
            );
            let probe = async {
                let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
                socket
                    .write_all(
                        format!(
                            "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .await?;
                let mut response = String::new();
                socket.read_to_string(&mut response).await?;
                Ok::<_, std::io::Error>(response)
            };
            if let Ok(Ok(response)) = tokio::time::timeout(Duration::from_secs(4), probe).await {
                if response.starts_with(&format!("HTTP/1.1 {expected_status}")) {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{} {path} status did not become {expected_status}\n{}",
                self.node_id,
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn connect(&mut self) -> Channel {
        let deadline = Instant::now() + Duration::from_secs(40);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                panic!(
                    "{} exited during startup: {status}\n{}",
                    self.node_id,
                    self.logs()
                );
            }
            let endpoint = Endpoint::from_shared(self.endpoint.clone())
                .unwrap()
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(15));
            if let Ok(channel) = endpoint.connect().await {
                return channel;
            }
            assert!(
                Instant::now() < deadline,
                "{} did not start\n{}",
                self.node_id,
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "{} did not exit\n{}",
                self.node_id,
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn kill(&mut self) {
        self.child.kill().unwrap();
        let status = self.wait_exit(Duration::from_secs(5)).await;
        assert_eq!(status.signal(), Some(9), "expected SIGKILL");
    }

    async fn terminate(&mut self) {
        assert!(Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap()
            .success());
        let status = self.wait_exit(Duration::from_secs(20)).await;
        assert!(
            status.success(),
            "SIGTERM shutdown failed: {status}\n{}",
            self.logs()
        );
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if std::thread::panicking() {
            eprintln!("{} server log:\n{}", self.node_id, self.logs());
        }
    }
}

fn request(path: &str, source: &str, min_timestamp: i64) -> IngestRequest {
    IngestRequest {
        table_name: TABLE.into(),
        record: Some(MetadataRecord {
            file: Some(FileFields {
                state: "IR_CLOSED".into(),
                min_timestamp,
                max_timestamp: min_timestamp + 100,
                raw_size_bytes: 100,
                record_count: 10,
                retention_days: 30,
                expires_at: 0,
                file: Some(FileInfo {
                    file_storage_backend: "local".into(),
                    file_bucket: "smoke".into(),
                    file_path: path.into(),
                    file_size_bytes: 100,
                }),
                archive: None,
            }),
            sketch: vec![metalog_proto::coordinator::SketchEntry {
                sketch_key: "trace_id".into(),
                data: include_bytes!("../../../tests/fixtures/bloom/snapshot.msgpack").to_vec(),
            }],
            dim: vec![DimEntry {
                key: "source".into(),
                value: Some(DimensionValue {
                    value: Some(dimension_value::Value::Str(StringDimension {
                        value: source.into(),
                        max_length: 256,
                    })),
                }),
            }],
            ..Default::default()
        }),
    }
}

async fn ingest(channel: Channel, path: &str, source: &str, min_timestamp: i64) {
    let response = MetadataIngestionServiceClient::new(channel)
        .ingest(request(path, source, min_timestamp))
        .await
        .unwrap()
        .into_inner();
    assert!(response.accepted, "ingestion rejected: {}", response.error);
}

async fn paths(channel: Channel) -> BTreeSet<String> {
    let mut stream = SplitQueryServiceClient::new(channel)
        .stream_splits(StreamSplitsRequest {
            table: TABLE.into(),
            projection: vec![
                "__FILE.file_path".into(),
                "__FILE.record_count".into(),
                "__DIM.source".into(),
            ],
            filter_expression: "__FILE.state = 'IR_CLOSED'".into(),
            sketch_expression: "trace_id = 'abc'".into(),
            order_by: vec![OrderBy {
                column: "__FILE.max_timestamp".into(),
                order: Order::Desc as i32,
            }],
            include_cursor: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut paths = BTreeSet::new();
    let mut done = false;
    while let Some(response) = tokio::time::timeout(Duration::from_secs(15), stream.message())
        .await
        .unwrap()
        .unwrap()
    {
        if let Some(split) = response.split {
            assert!(
                response.cursor.is_some(),
                "requested continuation cursor is missing"
            );
            assert!(split.dimensions.contains_key("source"));
            let file = split.file.unwrap();
            assert_eq!(file.record_count, 10);
            assert!(paths.insert(file.file_path), "duplicate split");
        }
        if response.done {
            assert!(!response.stats.unwrap().truncated);
            done = true;
        }
    }
    assert!(done, "stream must end with completion statistics");
    paths
}

async fn owner(pool: &MySqlPool) -> Option<String> {
    sqlx::query_scalar("SELECT CAST(node_id AS CHAR) FROM _table_assignment WHERE table_name = ?")
        .bind(TABLE)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn wait_for_owner(pool: &MySqlPool, expected: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(current) = owner(pool).await {
            if expected.is_none_or(|expected| current == expected) {
                return current;
            }
        }
        assert!(
            Instant::now() < deadline,
            "HA did not assign the expected owner {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn two_actual_servers_ingest_query_fail_over_and_shutdown_cleanly() {
    let (_writer_pool, database) = metalog_it::helpers::setup_db().await;
    // Observe ownership and durable writes independently of the tested proxy.
    let pool = database.direct_pool().await;
    let primary = database.writer_config();
    let replica = database.reader_config();
    if database.proxysql().is_some() {
        assert_ne!(
            primary.user, replica.user,
            "reader must use its own proxy role"
        );
    }
    let directory = tempfile::tempdir().unwrap();
    // Reserve all listener ports together so none can be selected twice.
    let reservations: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    drop(reservations);
    let mut first = ServerProcess::spawn(
        directory.path(),
        "smoke-a",
        &primary,
        &replica,
        ports[0],
        Some(ports[2]),
    );
    let mut second = ServerProcess::spawn(
        directory.path(),
        "smoke-b",
        &primary,
        &replica,
        ports[1],
        Some(ports[3]),
    );
    let first_channel = first.connect().await;
    let second_channel = second.connect().await;
    first.wait_for_health("/ready", 200).await;
    second.wait_for_health("/ready", 200).await;
    let table_request = RegisterTableRequest {
        table_name: TABLE.into(),
        display_name: "Smoke".into(),
        config_json: None,
    };
    assert!(
        AdminServiceClient::new(first_channel.clone())
            .register_table(table_request.clone())
            .await
            .unwrap()
            .into_inner()
            .created
    );
    assert!(
        !AdminServiceClient::new(second_channel.clone())
            .register_table(table_request)
            .await
            .unwrap()
            .into_inner()
            .created
    );
    wait_for_owner(&pool, None).await;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let ambiguous = MetadataIngestionServiceClient::new(first_channel.clone())
        .ingest(request("/first.ir", "smoke-a", timestamp))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !ambiguous.accepted && ambiguous.error.contains("file location"),
        "the actual server must apply its configured backend's object-key policy"
    );
    for name in [
        "first.ir",
        "second.ir",
        "during-reader-outage.ir",
        "after-proxy-restart.ir",
        "after-failover.ir",
        "before-shutdown.ir",
    ] {
        std::fs::write(
            directory.path().join("objects/smoke").join(name),
            vec![0; 100],
        )
        .unwrap();
    }
    ingest(first_channel.clone(), "first.ir", "smoke-a", timestamp).await;
    ingest(
        second_channel.clone(),
        "second.ir",
        "smoke-b",
        timestamp + 1000,
    )
    .await;
    let mut pruned = SplitQueryServiceClient::new(first_channel.clone())
        .stream_splits(StreamSplitsRequest {
            table: TABLE.into(),
            sketch_expression: "trace_id = 'definitely-absent'".into(),
            order_by: vec![OrderBy {
                column: "min_timestamp".into(),
                order: Order::Asc as i32,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let final_response = pruned.message().await.unwrap().unwrap();
    assert!(final_response.done && final_response.split.is_none());
    assert_eq!(final_response.stats.unwrap().splits_matched, 0);
    let mut expected = BTreeSet::from(["first.ir".into(), "second.ir".into()]);
    assert_eq!(paths(first_channel.clone()).await, expected);
    assert_eq!(paths(second_channel.clone()).await, expected);

    if let Some(proxy) = database.proxysql() {
        // Both hostgroups reach this test database. Taking only the reader route
        // offline proves the binary actually uses its separate replica settings.
        let admin = proxy.admin_pool().await;
        admin
            .execute("UPDATE mysql_servers SET status = 'OFFLINE_HARD' WHERE hostgroup_id = 20")
            .await
            .unwrap();
        admin
            .execute("LOAD MYSQL SERVERS TO RUNTIME")
            .await
            .unwrap();
        first.wait_for_health("/ready", 503).await;
        second.wait_for_health("/ready", 503).await;
        first.wait_for_health("/health", 200).await;
        second.wait_for_health("/health", 200).await;
        ingest(
            first_channel.clone(),
            "during-reader-outage.ir",
            "smoke-a",
            timestamp + 1200,
        )
        .await;
        expected.insert("during-reader-outage.ir".into());
        let committed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM production_smoke")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(committed, expected.len() as i64);
        admin
            .execute("UPDATE mysql_servers SET status = 'ONLINE' WHERE hostgroup_id = 20")
            .await
            .unwrap();
        admin
            .execute("LOAD MYSQL SERVERS TO RUNTIME")
            .await
            .unwrap();
        admin.close().await;
        first.wait_for_health("/ready", 200).await;
        second.wait_for_health("/ready", 200).await;
        assert_eq!(paths(first_channel.clone()).await, expected);
        assert_eq!(paths(second_channel.clone()).await, expected);

        // The real binaries must remain alive, withdraw readiness during a proxy
        // outage, and reconnect both pools without losing acknowledged records.
        proxy.stop().await;
        first.wait_for_health("/ready", 503).await;
        second.wait_for_health("/ready", 503).await;
        first.wait_for_health("/health", 200).await;
        second.wait_for_health("/health", 200).await;
        proxy.start().await;
        first.wait_for_health("/ready", 200).await;
        second.wait_for_health("/ready", 200).await;
        assert_eq!(paths(first_channel.clone()).await, expected);
        assert_eq!(paths(second_channel.clone()).await, expected);
        ingest(
            second_channel.clone(),
            "after-proxy-restart.ir",
            "smoke-b",
            timestamp + 1500,
        )
        .await;
        expected.insert("after-proxy-restart.ir".into());
        assert_eq!(paths(first_channel.clone()).await, expected);
        assert_eq!(paths(second_channel.clone()).await, expected);
    }

    // A proxy outage can expire table leases; kill the current owner after recovery.
    let initial_owner = wait_for_owner(&pool, None).await;
    assert!(initial_owner == first.node_id || initial_owner == second.node_id);
    let first_starts = first.logs().matches("coordinator started").count();
    let second_starts = second.logs().matches("coordinator started").count();
    let (survivor, survivor_channel, prior_starts) = if initial_owner == first.node_id {
        first.kill().await;
        (&mut second, second_channel, second_starts)
    } else {
        second.kill().await;
        (&mut first, first_channel, first_starts)
    };
    wait_for_owner(&pool, Some(&survivor.node_id)).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while survivor.logs().matches("coordinator started").count() <= prior_starts {
        assert!(
            Instant::now() < deadline,
            "takeover did not start a coordinator\n{}",
            survivor.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ingest(
        survivor_channel.clone(),
        "after-failover.ir",
        &survivor.node_id,
        timestamp + 2000,
    )
    .await;
    ingest(
        survivor_channel.clone(),
        "before-shutdown.ir",
        &survivor.node_id,
        timestamp + 3000,
    )
    .await;
    expected.extend(["after-failover.ir".into(), "before-shutdown.ir".into()]);
    assert_eq!(paths(survivor_channel).await, expected);
    survivor.terminate().await;
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM production_smoke")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        persisted,
        expected.len() as i64,
        "acknowledged records must survive both crash and graceful shutdown"
    );
    assert!(
        owner(&pool).await.is_none(),
        "graceful shutdown must release table ownership"
    );
}

#[tokio::test]
async fn failed_listener_bind_exits_nonzero() {
    let (_pool, database) = metalog_it::helpers::setup_db().await;
    let directory = tempfile::tempdir().unwrap();
    let occupied = TcpListener::bind("0.0.0.0:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let mut server = ServerProcess::spawn(
        directory.path(),
        "bind-failure",
        &database.writer_config(),
        &database.reader_config(),
        port,
        None,
    );
    let status = server.wait_exit(Duration::from_secs(20)).await;
    assert!(
        !status.success(),
        "listener bind failure must be a startup failure"
    );
    assert!(
        server
            .logs()
            .to_lowercase()
            .contains("address already in use"),
        "unexpected startup failure\n{}",
        server.logs()
    );
}

#[tokio::test]
async fn consolidation_recovers_after_worker_and_coordinator_process_death() {
    use std::os::unix::fs::PermissionsExt;
    let (pool, database) = metalog_it::helpers::setup_db().await;
    let directory = tempfile::tempdir().unwrap();
    let reservations: Vec<_> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|v| v.local_addr().unwrap().port())
        .collect();
    drop(reservations);
    let slow = directory.path().join("slow-clp");
    let marker = directory.path().join("compressor.pid");
    std::fs::write(
        &slow,
        format!(
            "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nexec sleep 60\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut first = ServerProcess::spawn_with_worker(
        directory.path(),
        "archive-a",
        &database.writer_config(),
        &database.reader_config(),
        ports[0],
        Some(ports[2]),
        Some(&slow),
    );
    let channel = first.connect().await;
    AdminServiceClient::new(channel.clone())
        .register_table(RegisterTableRequest {
            table_name: TABLE.into(),
            display_name: "Consolidation failover".into(),
            config_json: Some(
                r#"{"consolidation":{"enabled":true,"policies":[
            {"type":"time_window","config":{"min_files":2,"window_size":"24h"}}
        ]}}"#
                    .into(),
            ),
        })
        .await
        .unwrap();
    wait_for_owner(&pool, Some("archive-a")).await;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    for (index, (name, data)) in [
        (
            "one.ir",
            include_bytes!("../../../tests/fixtures/consolidation/one.ir").as_slice(),
        ),
        (
            "two.ir",
            include_bytes!("../../../tests/fixtures/consolidation/two.ir").as_slice(),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(directory.path().join("objects/smoke").join(name), data).unwrap();
        let mut record = request(name, "archive-producer", now + index as i64);
        record.record.as_mut().unwrap().file.as_mut().unwrap().state =
            "IR_ARCHIVE_CONSOLIDATION_PENDING".into();
        let result = MetadataIngestionServiceClient::new(channel.clone())
            .ingest(record)
            .await
            .unwrap()
            .into_inner();
        assert!(result.accepted, "{}", result.error);
    }
    let deadline = Instant::now() + Duration::from_secs(40);
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "first worker never compressed\n{}",
            first.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let binary = metalog_it::clp::binary();
    let mut second = ServerProcess::spawn_with_worker(
        directory.path(),
        "archive-b",
        &database.writer_config(),
        &database.reader_config(),
        ports[1],
        Some(ports[3]),
        Some(&binary),
    );
    let second_channel = second.connect().await;
    first.kill().await;
    // Reap the test-owned blocking subprocess, then simulate elapsed task timeout.
    let pid = std::fs::read_to_string(&marker).unwrap();
    let _ = Command::new("kill").args(["-TERM", pid.trim()]).status();
    sqlx::query("UPDATE _task_queue SET claimed_at=1 WHERE state='processing'")
        .execute(&pool)
        .await
        .unwrap();
    wait_for_owner(&pool, Some("archive-b")).await;
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {TABLE} WHERE state='ARCHIVE_CLOSED'"
        ))
        .fetch_one(&pool)
        .await
        .unwrap();
        if count == 2 && !directory.path().join("objects/smoke/one.ir").exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replacement failed to finalize\n{}",
            second.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut stream = SplitQueryServiceClient::new(second_channel)
        .stream_splits(StreamSplitsRequest {
            table: TABLE.into(),
            filter_expression: "__FILE.state = 'ARCHIVE_CLOSED'".into(),
            order_by: vec![OrderBy {
                column: "min_timestamp".into(),
                order: Order::Asc as i32,
            }],
            sketch_expression: "trace_id = 'abc'".into(),
            projection: vec![
                "__FILE.state".into(),
                "__FILE.archive_path".into(),
                "__FILE.archive_size_bytes".into(),
                "__DIM.source".into(),
            ],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut archives = BTreeSet::new();
    let mut splits = 0;
    while let Some(response) = stream.message().await.unwrap() {
        if let Some(split) = response.split {
            let file = split.file.unwrap();
            assert_eq!(file.state, "ARCHIVE_CLOSED");
            assert!(file.archive_size_bytes > 0);
            assert_eq!(split.dimensions["source"], "archive-producer");
            archives.insert(file.archive_path);
            splits += 1;
        }
    }
    assert_eq!(splits, 2);
    assert_eq!(archives.len(), 1);
    let path = directory
        .path()
        .join("objects/smoke")
        .join(archives.first().unwrap());
    assert!(path.is_file());
    let output = Command::new(binary)
        .arg("x")
        .arg(path)
        .arg(directory.path().join("decoded"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let decoded = std::fs::read_to_string(directory.path().join("decoded/original")).unwrap();
    assert_eq!(decoded.lines().count(), 4);
    assert!(decoded.contains("worker beta failed 1 item"));
    second.terminate().await;
}

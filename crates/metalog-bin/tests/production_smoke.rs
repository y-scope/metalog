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
use sqlx::MySqlPool;
use tonic::transport::{Channel, Endpoint};

const TABLE: &str = "production_smoke";

/// Always reap test-owned servers, including assertion failures. Logs go to files
/// so a full pipe cannot stall a server and the failure includes startup context.
struct ServerProcess {
    child: Child,
    node_id: String,
    endpoint: String,
    log_path: PathBuf,
}

impl ServerProcess {
    fn spawn(directory: &Path, node_id: &str, db_port: u16, grpc_port: u16) -> Self {
        let storage_dir = directory.join("objects");
        std::fs::create_dir_all(storage_dir.join("smoke")).unwrap();
        let config_path = directory.join(format!("{node_id}.yaml"));
        let config = format!(
            r#"database:
  primary:
    host: 127.0.0.1
    port: {db_port}
    database: test
    user: root
    password: ""
    pool_size: 5
    pool_min_idle: 1
storage:
  default_backend: local
  backends:
    local:
      type: filesystem
      base_path: '{}'
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
  enabled: false
worker:
  concurrency: 0
telemetry:
  enabled: false
"#,
            storage_dir.display()
        );
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
            log_path,
        }
    }

    fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
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
    let (pool, container) = metalog_it::helpers::setup_db().await;
    let db_port = container.get_host_port_ipv4(3306).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    // Reserve both ports together so the OS cannot return the same one twice.
    let first_port = TcpListener::bind("127.0.0.1:0").unwrap();
    let second_port = TcpListener::bind("127.0.0.1:0").unwrap();
    let ports = [
        first_port.local_addr().unwrap().port(),
        second_port.local_addr().unwrap().port(),
    ];
    drop((first_port, second_port));
    let mut first = ServerProcess::spawn(directory.path(), "smoke-a", db_port, ports[0]);
    let mut second = ServerProcess::spawn(directory.path(), "smoke-b", db_port, ports[1]);
    let first_channel = first.connect().await;
    let second_channel = second.connect().await;
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
    let initial_owner = wait_for_owner(&pool, None).await;
    assert!(initial_owner == first.node_id || initial_owner == second.node_id);
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
    let expected = BTreeSet::from(["first.ir".into(), "second.ir".into()]);
    assert_eq!(paths(first_channel.clone()).await, expected);
    assert_eq!(paths(second_channel.clone()).await, expected);

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
    assert_eq!(paths(survivor_channel).await.len(), 4);
    survivor.terminate().await;
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM production_smoke")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        persisted, 4,
        "acknowledged records must survive both crash and graceful shutdown"
    );
    assert!(
        owner(&pool).await.is_none(),
        "graceful shutdown must release table ownership"
    );
}

#[tokio::test]
async fn failed_listener_bind_exits_nonzero() {
    let (_pool, container) = metalog_it::helpers::setup_db().await;
    let db_port = container.get_host_port_ipv4(3306).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let occupied = TcpListener::bind("0.0.0.0:0").unwrap();
    let port = occupied.local_addr().unwrap().port();
    let mut server = ServerProcess::spawn(directory.path(), "bind-failure", db_port, port);
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

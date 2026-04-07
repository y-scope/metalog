//! Ingestion benchmark for metalog.
//!
//! Measures gRPC ingestion throughput with optional MariaDB backend.
//!
//! Usage:
//!   # Channel-only (no DB):
//!   cargo run -p metalog-bench-ingestion --release
//!
//!   # Full pipeline with MariaDB (testcontainers):
//!   cargo run -p metalog-bench-ingestion --release -- --with-db

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use clap::Parser;
use metalog_ingestion::{BatchingWriter, IngestionService};
use metalog_proto::{
    coordinator::{
        dimension_value,
        ingest_agg_entry,
        DimEntry,
        DimensionValue,
        FileFields,
        IngestAggEntry,
        IngestAggType,
        IrFileInfo,
        MetadataRecord,
        StringDimension,
    },
    IngestRequest,
    MetadataIngestionServiceServer,
};
use sqlx::MySqlPool;
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(name = "bench-ingestion", about = "Metalog ingestion benchmark")]
struct Args {
    /// Number of records to send.
    #[arg(long, default_value_t = 100_000)]
    records: usize,

    /// Number of distinct app IDs.
    #[arg(long, default_value_t = 10_000)]
    apps: usize,

    /// Target table name.
    #[arg(long, default_value = "clp_spark")]
    table: String,

    /// Number of independent connections.
    #[arg(long, default_value_t = 10)]
    clients: usize,

    /// Concurrent in-flight RPCs per connection.
    #[arg(long, default_value_t = 500)]
    concurrency_per_client: usize,

    /// Use a real MariaDB via testcontainers.
    #[arg(long)]
    with_db: bool,

    /// Batch size for the BatchingWriter.
    #[arg(long, default_value_t = 5_000)]
    batch_size: usize,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    println!("========================================");
    println!("  METALOG INGESTION BENCHMARK (Rust)");
    println!("========================================");
    println!("  Records     : {}", args.records);
    println!("  Apps        : {}", args.apps);
    println!("  Clients     : {}", args.clients);
    println!("  Per-client  : {}", args.concurrency_per_client);
    println!(
        "  Total conc. : {}",
        args.clients * args.concurrency_per_client
    );
    println!("  Batch size  : {}", args.batch_size);
    println!("  With DB     : {}", args.with_db);
    println!("  Table       : {}", args.table);
    println!("========================================");
    println!();

    if args.with_db {
        run_with_db(&args).await;
    } else {
        run_channel_only(&args).await;
    }
}

/// Runs benchmark with real MariaDB via testcontainers.
async fn run_with_db(args: &Args) {
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mariadb::Mariadb;

    println!("Starting MariaDB container...");
    let container = Mariadb::default().start().await.unwrap();

    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let dsn = format!("mysql://root@127.0.0.1:{port}/test");
    println!("MariaDB ready on port {port}");

    let pool = MySqlPool::connect(&dsn).await.unwrap();

    // Create schema and table.
    println!("Creating schema...");
    metalog_schema::execute_ddl_statements(&pool, metalog_schema::SCHEMA_SQL)
        .await
        .unwrap();
    metalog_schema::ensure_table(&pool, &args.table, None)
        .await
        .unwrap();
    println!("Table '{}' provisioned", args.table);

    // Create column registry and pre-allocate dim columns.
    let registry = Arc::new(
        metalog_schema::ColumnRegistry::new(pool.clone(), &args.table)
            .await
            .unwrap(),
    );
    for (key, base_type, width) in [
        ("service", "str", 128),
        ("host", "str", 256),
        ("zone", "str", 128),
    ] {
        registry
            .resolve_or_allocate_dim(key, base_type, width)
            .await
            .unwrap();
        println!(
            "  allocated dim: {key} → {}",
            registry.resolve_dim(key).unwrap()
        );
    }

    // Create mysql_async pool for high-throughput UPSERT.
    let mysql_dsn = format!("mysql://root@127.0.0.1:{port}/test");
    let mysql_pool = mysql_async::Pool::new(mysql_dsn.as_str());

    // Start server with real DB.
    let (addr, writer) = start_server_with_db(
        pool.clone(),
        mysql_pool,
        registry.clone(),
        &args.table,
        args.batch_size,
    )
    .await;

    // Run benchmark.
    let (accepted, rejected, elapsed) = run_grpc(
        addr,
        &args.table,
        args.records,
        args.apps,
        args.clients,
        args.concurrency_per_client,
    )
    .await;

    // Wait for BatchingWriter to flush remaining records.
    println!("Stopping writer (flushing remaining batches)...");
    writer.stop().await;

    // Check DB row count.
    let row_count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM `{}`", args.table))
        .fetch_one(&pool)
        .await
        .unwrap();

    print_results(args, accepted, rejected, elapsed, Some(row_count.0));
}

/// Runs benchmark without DB (channel throughput only).
async fn run_channel_only(args: &Args) {
    let (addr, _writer) = start_server_no_db(args.batch_size).await;

    let (accepted, rejected, elapsed) = run_grpc(
        addr,
        &args.table,
        args.records,
        args.apps,
        args.clients,
        args.concurrency_per_client,
    )
    .await;

    print_results(args, accepted, rejected, elapsed, None);
}

fn print_results(
    args: &Args,
    accepted: i64,
    rejected: i64,
    elapsed: Duration,
    db_rows: Option<i64>,
) {
    println!();
    println!("========================================");
    println!("  RESULTS");
    println!("========================================");
    println!(
        "  Records     : {} sent, {} accepted, {} rejected",
        args.records, accepted, rejected
    );
    if let Some(rows) = db_rows {
        println!("  DB rows     : {rows}");
    }
    println!("  Duration    : {:.1}s", elapsed.as_secs_f64());
    println!(
        "  Throughput  : {:.0} rec/s",
        accepted as f64 / elapsed.as_secs_f64()
    );
    println!("========================================");
}

/// Starts gRPC server backed by real MariaDB.
async fn start_server_with_db(
    pool: MySqlPool,
    mysql_pool: mysql_async::Pool,
    registry: Arc<metalog_schema::ColumnRegistry>,
    table_name: &str,
    batch_size: usize,
) -> (SocketAddr, Arc<BatchingWriter>) {
    let writer = Arc::new(
        BatchingWriter::new(pool, true)
            .with_mysql_pool(mysql_pool)
            .with_batch_size(batch_size)
            .with_flush_interval(Duration::from_secs(1)),
    );
    writer.set_registry(table_name, registry).await;

    let service = Arc::new(IngestionService::new(writer.clone(), true));
    let handler = metalog_grpc::IngestionHandler::new(service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(MetadataIngestionServiceServer::new(handler))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, writer)
}

/// Starts gRPC server without DB (channel throughput only).
async fn start_server_no_db(batch_size: usize) -> (SocketAddr, Arc<BatchingWriter>) {
    let pool = MySqlPool::connect_lazy("mysql://root@localhost/test").unwrap();
    let writer = Arc::new(BatchingWriter::new(pool, true).with_batch_size(batch_size));
    let service = Arc::new(IngestionService::new(writer.clone(), true));
    let handler = metalog_grpc::IngestionHandler::new(service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tonic::transport::Server::builder()
            .add_service(MetadataIngestionServiceServer::new(handler))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, writer)
}

/// Runs the gRPC benchmark: N independent connections × M concurrent RPCs each.
///
/// Simulates production: multiple SDK clients, each with many in-flight
/// requests (HTTP/2 stream multiplexing within each connection).
async fn run_grpc(
    addr: SocketAddr,
    table: &str,
    records: usize,
    apps: usize,
    num_clients: usize,
    concurrency_per_client: usize,
) -> (i64, i64, Duration) {
    let accepted = Arc::new(AtomicI64::new(0));
    let rejected = Arc::new(AtomicI64::new(0));

    let endpoint = format!("http://{addr}");
    let total_workers = num_clients * concurrency_per_client;
    let (tx, rx) = async_channel::bounded::<usize>(total_workers);

    let start = Instant::now();

    // Create N independent connections, each shared by M worker tasks.
    let mut join_set = JoinSet::new();
    for _client_id in 0..num_clients {
        let channel = tonic::transport::Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();

        // M concurrent tasks per connection (HTTP/2 stream multiplexing).
        for _ in 0..concurrency_per_client {
            let rx = rx.clone();
            let channel = channel.clone();
            let accepted = accepted.clone();
            let rejected = rejected.clone();
            let table = table.to_string();

            join_set.spawn(async move {
                let mut client = metalog_proto::MetadataIngestionServiceClient::new(channel);

                while let Ok(idx) = rx.recv().await {
                    let req = build_ingest_request(&table, idx, apps);

                    match client.ingest(tonic::Request::new(req)).await {
                        Ok(resp) => {
                            if resp.into_inner().accepted {
                                accepted.fetch_add(1, Ordering::Relaxed);
                            } else {
                                rejected.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            rejected.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    }

    for i in 0..records {
        tx.send(i).await.unwrap();
    }
    drop(tx);

    while join_set.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    (
        accepted.load(Ordering::Relaxed),
        rejected.load(Ordering::Relaxed),
        elapsed,
    )
}

/// Builds an IngestRequest matching the Go benchmark's record format.
fn build_ingest_request(table: &str, i: usize, app_count: usize) -> IngestRequest {
    let app_id = i % app_count;
    let hour_offset = (i % 24) as i64;
    let record_count = (50_000 + (i % 50_000)) as i32;

    let service = format!("app-{app_id:03}");
    let host = format!("host-{app_id:03}.app-{app_id:03}.us-east-1a");
    let zone = format!("us-east-1{}", (b'a' + (app_id % 4) as u8) as char);
    let ir_path = format!("s3://ir-bucket/{service}/ir-{i:09}.clp.zst");

    const BASE_TIMESTAMP: i64 = 1_738_368_000_000_000_000;
    const HOUR: i64 = 3_600_000_000_000;
    let min_ts = BASE_TIMESTAMP + hour_offset * HOUR;
    let max_ts = min_ts + HOUR - 1;

    IngestRequest {
        table_name: table.to_string(),
        record: Some(MetadataRecord {
            file: Some(FileFields {
                state: "IR_CLOSED".into(),
                min_timestamp: min_ts,
                max_timestamp: max_ts,
                record_count,
                raw_size_bytes: 0,
                retention_days: 30,
                expires_at: 0,
                ir: Some(IrFileInfo {
                    clp_ir_storage_backend: "s3".into(),
                    clp_ir_bucket: "ir-bucket".into(),
                    clp_ir_path: ir_path,
                    clp_ir_size_bytes: 0,
                }),
                archive: None,
            }),
            dim: vec![
                DimEntry {
                    key: "service".into(),
                    value: Some(DimensionValue {
                        value: Some(dimension_value::Value::Str(StringDimension {
                            value: service,
                            max_length: 128,
                        })),
                    }),
                },
                DimEntry {
                    key: "host".into(),
                    value: Some(DimensionValue {
                        value: Some(dimension_value::Value::Str(StringDimension {
                            value: host,
                            max_length: 128,
                        })),
                    }),
                },
                DimEntry {
                    key: "zone".into(),
                    value: Some(DimensionValue {
                        value: Some(dimension_value::Value::Str(StringDimension {
                            value: zone,
                            max_length: 128,
                        })),
                    }),
                },
            ],
            agg: vec![
                IngestAggEntry {
                    field: "level".into(),
                    qualifier: "info".into(),
                    agg_type: IngestAggType::Gte as i32,
                    value: Some(ingest_agg_entry::Value::IntVal(record_count as i64)),
                    alias_column: String::new(),
                },
                IngestAggEntry {
                    field: "level".into(),
                    qualifier: "warn".into(),
                    agg_type: IngestAggType::Gte as i32,
                    value: Some(ingest_agg_entry::Value::IntVal(
                        (record_count as f64 * 0.05) as i64,
                    )),
                    alias_column: String::new(),
                },
                IngestAggEntry {
                    field: "level".into(),
                    qualifier: "error".into(),
                    agg_type: IngestAggType::Gte as i32,
                    value: Some(ingest_agg_entry::Value::IntVal(
                        (record_count as f64 * 0.02) as i64,
                    )),
                    alias_column: String::new(),
                },
                IngestAggEntry {
                    field: "level".into(),
                    qualifier: "fatal".into(),
                    agg_type: IngestAggType::Gte as i32,
                    value: Some(ingest_agg_entry::Value::IntVal(
                        (record_count as f64 * 0.002) as i64,
                    )),
                    alias_column: String::new(),
                },
            ],
            self_describing_kv: vec![],
            sketch: vec![],
        }),
    }
}

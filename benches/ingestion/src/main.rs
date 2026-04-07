//! Ingestion benchmark for metalog.
//!
//! Measures gRPC ingestion throughput by sending records via concurrent
//! tonic clients to an in-process metalog gRPC server.
//!
//! Usage:
//!   cargo run -p metalog-bench-ingestion --release -- [OPTIONS]
//!
//! Equivalent to Go's `test/benchmarks/ingestion/main.go`.

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
    MetadataIngestionServiceClient,
    MetadataIngestionServiceServer,
};
use tokio::task::JoinSet;
use tonic::transport::Channel;

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

    /// Maximum concurrent in-flight RPCs.
    #[arg(long, default_value_t = 5_000)]
    concurrency: usize,

    /// Use blocking ingestion (higher throughput).
    #[arg(long, default_value_t = true)]
    blocking: bool,

    /// Number of HTTP/2 connections to the server.
    #[arg(long, default_value_t = 4)]
    connections: usize,
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
    println!("  Concurrency : {}", args.concurrency);
    println!("  Connections : {}", args.connections);
    println!("  Blocking    : {}", args.blocking);
    println!("  Table       : {}", args.table);
    println!("========================================");
    println!();

    // Start in-process gRPC server with BatchingWriter.
    let (addr, _writer) = start_server(args.blocking).await;

    // Run benchmark.
    let (accepted, rejected, elapsed) = run_grpc(
        addr,
        &args.table,
        args.records,
        args.apps,
        args.concurrency,
        args.connections,
    )
    .await;

    // Print results.
    println!();
    println!("========================================");
    println!("  RESULTS");
    println!("========================================");
    println!(
        "  Records     : {} sent, {} accepted, {} rejected",
        args.records, accepted, rejected
    );
    println!("  Duration    : {:.1}s", elapsed.as_secs_f64());
    println!(
        "  Throughput  : {:.0} rec/s",
        accepted as f64 / elapsed.as_secs_f64()
    );
    println!("========================================");
}

/// Starts an in-process gRPC server with the ingestion handler.
async fn start_server(blocking: bool) -> (SocketAddr, Arc<BatchingWriter>) {
    let pool = sqlx::MySqlPool::connect_lazy("mysql://root@localhost/test").unwrap();
    let writer = Arc::new(BatchingWriter::new(pool, true));
    let service = Arc::new(IngestionService::new(writer.clone(), blocking));

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

/// Runs the gRPC benchmark with multiple HTTP/2 connections.
async fn run_grpc(
    addr: SocketAddr,
    table: &str,
    records: usize,
    apps: usize,
    concurrency: usize,
    num_connections: usize,
) -> (i64, i64, Duration) {
    let accepted = Arc::new(AtomicI64::new(0));
    let rejected = Arc::new(AtomicI64::new(0));

    // Create multiple HTTP/2 connections for parallelism.
    let endpoint = format!("http://{addr}");
    let mut channels = Vec::with_capacity(num_connections);
    for _ in 0..num_connections {
        let channel = Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        channels.push(channel);
    }

    let (tx, rx) = async_channel::bounded::<usize>(concurrency);

    let start = Instant::now();

    // Spawn worker tasks, round-robin across connections.
    let mut join_set = JoinSet::new();
    for worker_id in 0..concurrency {
        let rx = rx.clone();
        let channel = channels[worker_id % num_connections].clone();
        let accepted = accepted.clone();
        let rejected = rejected.clone();
        let table = table.to_string();

        join_set.spawn(async move {
            let mut client = MetadataIngestionServiceClient::new(channel);

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
                    Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                        // Channel full — count as rejected (non-blocking mode).
                        rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        rejected.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    // Feed work.
    for i in 0..records {
        tx.send(i).await.unwrap();
    }
    drop(tx);

    while join_set.join_next().await.is_some() {}

    let elapsed = start.elapsed();
    let acc = accepted.load(Ordering::Relaxed);
    let rej = rejected.load(Ordering::Relaxed);

    (acc, rej, elapsed)
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

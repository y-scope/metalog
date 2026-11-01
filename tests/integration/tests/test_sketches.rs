use std::{collections::HashMap, sync::Arc, time::Duration};

use metalog_grpc::{IngestionHandler, QueryHandler};
use metalog_ingestion::{BatchingWriter, IngestionService};
use metalog_it::helpers::setup_db_with_table;
use metalog_proto::{
    coordinator::{
        metadata_ingestion_service_server::MetadataIngestionService,
        BatchIngestRequest,
        FileFields,
        FileInfo,
        IngestRequest,
        MetadataRecord,
        SketchEntry,
    },
    query::{
        split_query_service_server::SplitQueryService,
        KeysetCursor,
        OrderBy,
        QueryStats,
        StreamSplitsRequest,
    },
};
use metalog_query::SplitQueryEngine;
use metalog_sketches::{SketchRegistry, Snapshot, SBBF_TYPE};
use sqlx::{Executor, MySqlPool};
use tokio_stream::StreamExt;
use tonic::Request;

const WIRE: &[u8] = include_bytes!("../../fixtures/bloom/snapshot.msgpack");
fn empty_wire() -> Vec<u8> {
    rmp_serde::to_vec_named(&Snapshot {
        type_name: SBBF_TYPE.into(),
        data: vec![0; 32],
    })
    .unwrap()
}
fn record(index: i64, state: &str, wire: Option<Vec<u8>>) -> MetadataRecord {
    MetadataRecord {
        file: Some(FileFields {
            min_timestamp: index + 1000,
            max_timestamp: index + 1001,
            state: state.into(),
            record_count: 1,
            file: Some(FileInfo {
                file_storage_backend: "s3".into(),
                file_bucket: "test".into(),
                file_path: format!("{index}.ir"),
                ..Default::default()
            }),
            ..Default::default()
        }),
        sketch: wire
            .into_iter()
            .map(|data| SketchEntry {
                sketch_key: "trace_id".into(),
                data,
            })
            .collect(),
        ..Default::default()
    }
}
fn writer(pool: &MySqlPool) -> Arc<BatchingWriter> {
    Arc::new(BatchingWriter::new(pool.clone(), true).with_flush_interval(Duration::from_millis(5)))
}
fn ingestion(writer: &Arc<BatchingWriter>) -> IngestionHandler {
    IngestionHandler::new(Arc::new(IngestionService::new(writer.clone(), true)))
}
async fn ingest(handler: &IngestionHandler, table: &str, record: MetadataRecord) {
    let response = handler
        .ingest(Request::new(IngestRequest {
            table_name: table.into(),
            record: Some(record),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.accepted, "{}", response.error);
}
fn query(table: &str) -> StreamSplitsRequest {
    StreamSplitsRequest {
        table: table.into(),
        sketch_expression: "trace_id = 'abc'".into(),
        order_by: vec![OrderBy {
            column: "__FILE.min_timestamp".into(),
            order: 1,
        }],
        include_cursor: true,
        ..Default::default()
    }
}
async fn collect(
    pool: &MySqlPool,
    req: StreamSplitsRequest,
) -> (Vec<String>, QueryStats, Option<KeysetCursor>) {
    let handler = QueryHandler::new(Arc::new(SplitQueryEngine::new(pool.clone())), pool.clone());
    let mut stream = handler
        .stream_splits(Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let mut paths = vec![];
    let mut stats = None;
    let mut cursor = None;
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        if let Some(split) = response.split {
            paths.push(split.file.unwrap().file_path);
        }
        if response.done {
            stats = response.stats;
            cursor = response.cursor;
        }
    }
    (paths, stats.unwrap(), cursor)
}

#[tokio::test]
async fn grpc_bloom_roundtrip_restart_and_pruned_pagination() {
    let table = "bloom_pages";
    let (pool, db) = setup_db_with_table(table).await;
    let writer = writer(&pool);
    let ingest_handler = ingestion(&writer);
    let records: Vec<_> = (0..40)
        .map(|i| {
            record(
                i,
                if i == 38 { "IR_BUFFERING" } else { "IR_CLOSED" },
                if i == 37 {
                    None
                } else if [17, 35].contains(&i) {
                    Some(WIRE.to_vec())
                } else {
                    Some(empty_wire())
                },
            )
        })
        .collect();
    let response = ingest_handler
        .batch_ingest(Request::new(BatchIngestRequest {
            table_name: table.into(),
            records,
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.accepted_count, 40, "{:?}", response.failures);
    writer.stop().await;
    // Model legacy corruption. It must reduce pruning, never lose a candidate.
    pool.execute("UPDATE bloom_pages SET ext = X'00FF' WHERE file_path = '39.ir'")
        .await
        .unwrap();
    let stored: Vec<u8> = sqlx::query_scalar("SELECT ext FROM bloom_pages WHERE file_path='17.ir'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(metalog_sketches::decode_ext_blob(&stored)
        .unwrap()
        .sketches
        .contains_key("trace_id"));
    // Fresh reader connections and a new handler exercise durable data rather than a cache.
    let readers = metalog_db::new_pool(&db.reader_config()).await.unwrap();
    let (all, stats, _) = collect(&readers, query(table)).await;
    assert_eq!(all, ["17.ir", "35.ir", "37.ir", "38.ir", "39.ir"]);
    assert_eq!(
        (stats.splits_scanned, stats.splits_matched, stats.truncated),
        (40, 5, false)
    );
    let mut req = query(table);
    req.limit = 2;
    req.projection = vec!["__FILE.file_path".into()];
    let mut paged = vec![];
    loop {
        let (paths, stats, cursor) = collect(&readers, req.clone()).await;
        paged.extend(paths);
        if !stats.truncated {
            break;
        }
        req.cursor = cursor;
    }
    assert_eq!(paged, all);
    let mut req = query(table);
    req.sketch_expression = "trace_id IN ('absent', 'abc') AND user_id = 'not-indexed'".into();
    assert_eq!(collect(&readers, req).await.0, all);
    let mut req = query(table);
    req.sketch_expression.clear();
    assert_eq!(collect(&readers, req).await.0.len(), 40);
    let mut req = query(table);
    req.sketch_expression = "trace_id = 'definitely-absent'".into();
    assert_eq!(collect(&readers, req).await.0, ["37.ir", "38.ir", "39.ir"]);
    // The metadata API sees durable field allocation after writer shutdown.
    let fields = metalog_metastore::MetadataReader::new(readers.clone())
        .list_sketches(table)
        .await
        .unwrap();
    assert_eq!(fields.len(), 1);
    let handler = QueryHandler::new(
        Arc::new(SplitQueryEngine::new(readers.clone())),
        readers.clone(),
    );
    let mut grouped = query(table);
    grouped.group_by = vec!["__FILE.state".into()];
    assert_eq!(
        handler
            .stream_splits(Request::new(grouped))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    pool.execute(
        "UPDATE _sketch_registry SET state='INVALIDATED' WHERE table_name='bloom_pages' AND \
         state='ACTIVE'",
    )
    .await
    .unwrap();
    assert_eq!(collect(&readers, query(table)).await.0.len(), 40);
    // Reusing the logical name in a new slot must not reactivate old row snapshots.
    SketchRegistry::new(pool.clone(), table)
        .await
        .unwrap()
        .resolve_or_allocate(&["trace_id".into()])
        .await
        .unwrap();
    assert_eq!(collect(&readers, query(table)).await.0.len(), 40);
}

#[tokio::test]
async fn snapshots_follow_lifecycle_and_missing_updates_clear_stale_filters() {
    let table = "bloom_updates";
    let (pool, _db) = setup_db_with_table(table).await;
    let writer = writer(&pool);
    let handler = ingestion(&writer);
    ingest(
        &handler,
        table,
        record(0, "IR_BUFFERING", Some(empty_wire())),
    )
    .await;
    assert_eq!(collect(&pool, query(table)).await.0, ["0.ir"]);
    // Final snapshot is accepted with a same-timestamp closing transition.
    ingest(&handler, table, record(0, "IR_CLOSED", Some(WIRE.to_vec()))).await;
    assert_eq!(collect(&pool, query(table)).await.0, ["0.ir"]);
    ingest(&handler, table, record(0, "IR_CLOSED", Some(empty_wire()))).await; // equal retry cannot
                                                                               // replace it
    assert_eq!(collect(&pool, query(table)).await.0, ["0.ir"]);
    let mut new = record(0, "IR_CLOSED", Some(empty_wire()));
    new.file.as_mut().unwrap().max_timestamp += 2;
    ingest(&handler, table, new.clone()).await;
    assert!(collect(&pool, query(table)).await.0.is_empty());
    ingest(&handler, table, record(0, "IR_CLOSED", Some(WIRE.to_vec()))).await; // stale update
                                                                                // ignored
    assert!(collect(&pool, query(table)).await.0.is_empty());
    new.file.as_mut().unwrap().max_timestamp += 1;
    new.sketch.clear();
    ingest(&handler, table, new).await;
    assert_eq!(collect(&pool, query(table)).await.0, ["0.ir"]);
    let ext: Option<Vec<u8>> = sqlx::query_scalar("SELECT ext FROM bloom_updates")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(ext.is_none());
    // A replacement contains precisely its supplied fields, not an old/new merge.
    let mut replacement = record(0, "IR_CLOSED", Some(WIRE.to_vec()));
    replacement.file.as_mut().unwrap().max_timestamp += 4;
    replacement.sketch[0].sketch_key = "user_id".into();
    ingest(&handler, table, replacement).await;
    let ext: Vec<u8> = sqlx::query_scalar("SELECT ext FROM bloom_updates")
        .fetch_one(&pool)
        .await
        .unwrap();
    let payload = metalog_sketches::decode_ext_blob(&ext).unwrap();
    assert_eq!(payload.sketches.len(), 1);
    assert!(payload.sketches.contains_key("user_id"));
    // Protected lifecycle rows cannot have their snapshots overwritten by ingestion.
    pool.execute("UPDATE bloom_updates SET state='ARCHIVE_CLOSED'")
        .await
        .unwrap();
    let mut protected = record(0, "ARCHIVE_CLOSED", Some(empty_wire()));
    protected.file.as_mut().unwrap().max_timestamp += 5;
    ingest(&handler, table, protected).await;
    let after: Vec<u8> = sqlx::query_scalar("SELECT ext FROM bloom_updates")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(after, ext);
    // A malformed snapshot fails before queueing or acknowledging data.
    let invalid = handler
        .ingest(Request::new(IngestRequest {
            table_name: table.into(),
            record: Some(record(1, "IR_CLOSED", Some(vec![0]))),
        }))
        .await;
    assert_eq!(invalid.unwrap_err().code(), tonic::Code::InvalidArgument);
    writer.stop().await;
}

#[tokio::test]
async fn registry_allocation_is_concurrent_restartable_and_bounded() {
    let table = "bloom_registry";
    let (pool, _db) = setup_db_with_table(table).await;
    let mut joins = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let pool = pool.clone();
        joins.spawn(async move {
            SketchRegistry::new(pool, table)
                .await
                .unwrap()
                .resolve_or_allocate(&["trace_id".into(), "Trace_id".into()])
                .await
                .unwrap()
        });
    }
    let mut expected = None;
    while let Some(result) = joins.join_next().await {
        let mapping = result.unwrap();
        if let Some(ref expected) = expected {
            assert_eq!(&mapping, expected);
        } else {
            expected = Some(mapping);
        }
    }
    let registry = SketchRegistry::new(pool.clone(), table).await.unwrap();
    assert_ne!(
        registry.resolve("trace_id").await,
        registry.resolve("Trace_id").await
    );
    let keys: Vec<_> = (0..62).map(|i| format!("field_{i}")).collect();
    assert_eq!(registry.resolve_or_allocate(&keys).await.unwrap().len(), 64);
    assert!(registry
        .resolve_or_allocate(&["overflow".into()])
        .await
        .is_err());
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _sketch_registry WHERE table_name='bloom_registry' AND \
         state='ACTIVE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 64);
    let duplicate = HashMap::from([("trace_id".to_string(), WIRE.to_vec())]);
    assert!(metalog_sketches::encode_ext_blob(&duplicate).is_ok());
}

#[tokio::test]
async fn large_snapshots_are_chunked_and_survive_graceful_drain() {
    let table = "bloom_large";
    let (pool, _db) = setup_db_with_table(table).await;
    // Poorly compressible snapshots would make a single hex-encoded SQL INSERT
    // exceed MariaDB's default 16 MiB packet limit. They must be split by bytes.
    let mut seed = 0x12345678u32;
    let data: Vec<u8> = (0..800 * 1024)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed as u8
        })
        .collect();
    let wire = rmp_serde::to_vec_named(&Snapshot {
        type_name: SBBF_TYPE.into(),
        data: data.clone(),
    })
    .unwrap();
    let writer = BatchingWriter::new(pool.clone(), true)
        .with_batch_size(100)
        .with_flush_interval(Duration::from_secs(60));
    let mut receipts = Vec::new();
    for i in 0..12 {
        receipts.push(
            writer
                .submit_confirmed(
                    table,
                    metalog_ingestion::convert_record(&record(i, "IR_CLOSED", Some(wire.clone())))
                        .unwrap(),
                    true,
                )
                .await
                .unwrap(),
        );
    }
    writer.stop().await;
    for receipt in receipts {
        receipt.wait().await.unwrap();
    }
    let blobs: Vec<Vec<u8>> = sqlx::query_scalar("SELECT ext FROM bloom_large")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(blobs.len(), 12);
    for blob in blobs {
        assert_eq!(
            metalog_sketches::decode_ext_blob(&blob).unwrap().sketches["trace_id"].data,
            data
        );
    }
}

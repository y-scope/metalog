//! Exercise a running Compose stack with real IR, gRPC, MinIO and archive extraction.
//! Usage: AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... cargo run --example
//! consolidation_smoke -p metalog-integration-tests -- GRPC_URL S3_URL [SECOND_GRPC_URL]
use std::{collections::BTreeSet, time::Duration};

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    primitives::ByteStream,
    Client,
};
use metalog_proto::{
    coordinator::{
        admin_service_client::AdminServiceClient,
        metadata_ingestion_service_client::MetadataIngestionServiceClient,
        FileFields,
        FileInfo,
        IngestRequest,
        MetadataRecord,
        RegisterTableRequest,
    },
    query::{
        split_query_service_client::SplitQueryServiceClient,
        Order,
        OrderBy,
        StreamSplitsRequest,
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err("usage: consolidation_smoke GRPC_URL S3_URL [SECOND_GRPC_URL]".into());
    }
    let now = metalog_timeutil::epoch_nanos();
    let table = format!("compose_consolidation_{now}");
    let bucket = std::env::var("METALOG_TEST_BUCKET").unwrap_or_else(|_| "logs".into());
    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                std::env::var("AWS_ACCESS_KEY_ID")?,
                std::env::var("AWS_SECRET_ACCESS_KEY")?,
                None,
                None,
                "compose-smoke",
            ))
            .endpoint_url(&args[1])
            .force_path_style(true)
            .build(),
    );
    AdminServiceClient::connect(args[0].clone())
        .await?
        .register_table(RegisterTableRequest {
            table_name: table.clone(),
            display_name: "Consolidation smoke test".into(),
            config_json: Some(
                r#"{"consolidation":{"enabled":true,"policies":[
            {"type":"time_window","config":{"min_files":2,"window_size":"24h"}}
        ]}}"#
                    .into(),
            ),
        })
        .await?;
    let mut keys = Vec::new();
    for (index, data) in [
        include_bytes!("../../fixtures/consolidation/one.ir").as_slice(),
        include_bytes!("../../fixtures/consolidation/two.ir").as_slice(),
    ]
    .into_iter()
    .enumerate()
    {
        let path = format!("integration/{table}/{index}.ir");
        client
            .put_object()
            .bucket(&bucket)
            .key(&path)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await?;
        let endpoint = if index == 1 {
            args.get(2).unwrap_or(&args[0])
        } else {
            &args[0]
        };
        let response = MetadataIngestionServiceClient::connect(endpoint.clone())
            .await?
            .ingest(IngestRequest {
                table_name: table.clone(),
                record: Some(MetadataRecord {
                    file: Some(FileFields {
                        state: "IR_ARCHIVE_CONSOLIDATION_PENDING".into(),
                        min_timestamp: now + index as i64,
                        max_timestamp: now + index as i64 + 1,
                        raw_size_bytes: 100,
                        record_count: 2,
                        retention_days: 30,
                        expires_at: 0,
                        file: Some(FileInfo {
                            file_storage_backend: "minio".into(),
                            file_bucket: bucket.clone(),
                            file_path: path.clone(),
                            file_size_bytes: data.len() as i64,
                        }),
                        archive: None,
                    }),
                    ..Default::default()
                }),
            })
            .await?
            .into_inner();
        assert!(response.accepted, "ingestion failed: {}", response.error);
        keys.push(path);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    let archive = loop {
        let mut stream = SplitQueryServiceClient::connect(args[0].clone())
            .await?
            .stream_splits(StreamSplitsRequest {
                table: table.clone(),
                filter_expression: "__FILE.state = 'ARCHIVE_CLOSED'".into(),
                order_by: vec![OrderBy {
                    column: "min_timestamp".into(),
                    order: Order::Asc as i32,
                }],
                projection: vec!["archive_path".into(), "archive_size_bytes".into()],
                ..Default::default()
            })
            .await?
            .into_inner();
        let mut rows = 0;
        let mut paths = BTreeSet::new();
        while let Some(response) = stream.message().await? {
            if let Some(file) = response.split.and_then(|split| split.file) {
                assert!(file.archive_size_bytes > 0);
                rows += 1;
                paths.insert(file.archive_path);
            }
        }
        if rows == 2 {
            assert_eq!(paths.len(), 1);
            break paths.into_iter().next().unwrap();
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("archive finalization timed out".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let bytes = client
        .get_object()
        .bucket(&bucket)
        .key(&archive)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("archive.clp");
    tokio::fs::write(&path, bytes).await?;
    let output = tokio::process::Command::new(metalog_it::clp::binary())
        .arg("x")
        .arg(path)
        .arg(dir.path().join("decoded"))
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let decoded = tokio::fs::read_to_string(dir.path().join("decoded/original")).await?;
    assert_eq!(decoded.lines().count(), 4);
    assert!(decoded.contains("worker beta failed 1 item"));
    // Metadata publication precedes cleanup; give the finalizer time to finish it.
    loop {
        let mut deleted = 0;
        for key in &keys {
            match client.head_object().bucket(&bucket).key(key).send().await {
                Err(error) if error.as_service_error().is_some_and(|e| e.is_not_found()) => {
                    deleted += 1
                }
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
        }
        if deleted == keys.len() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("source cleanup timed out".into());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!(
        "PASS: {table}: 2 metadata rows, 4 extracted records, sources deleted; archive {archive}"
    );
    Ok(())
}

use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client,
};
use metalog_config::StorageBackendConfig;
use metalog_storage::{Backend, S3Backend};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage,
};
use tokio::io::AsyncReadExt;

#[tokio::test]
async fn minio_exact_keys_roundtrip_idempotent_delete_and_auth_failure() {
    let container = GenericImage::new("metalog-minio-test", "2025-10-15")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::healthcheck())
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
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
    client
        .create_bucket()
        .bucket("metalog-test")
        .send()
        .await
        .unwrap();
    let mut config = StorageBackendConfig {
        type_name: "s3".into(),
        bucket: "metalog-test".into(),
        base_url: String::new(),
        base_path: String::new(),
        endpoint,
        access_key: "minioadmin".into(),
        secret_key: "minioadmin".into(),
        session_token: String::new(),
        region: "us-east-1".into(),
        force_path_style: true,
    };
    let backend = S3Backend::new(&config).await.unwrap();
    assert!(backend.requires_canonical_keys());
    let keys = [
        "a.json",
        "nested/b.json",
        "space and 日本語.json",
        "plus+percent%25",
    ];
    for (n, key) in keys.iter().enumerate() {
        let data = format!("payload {n}");
        backend
            .put("metalog-test", key, data.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("PUT {key:?}: {e}"));
        assert!(backend.exists("metalog-test", key).await.unwrap());
    }
    let objects = client
        .list_objects_v2()
        .bucket("metalog-test")
        .send()
        .await
        .unwrap();
    assert_eq!(objects.contents().len(), keys.len());
    for (n, key) in keys.iter().enumerate() {
        assert!(objects
            .contents()
            .iter()
            .any(|object| object.key() == Some(*key)));
        let mut body = backend.get("metalog-test", key).await.unwrap();
        let mut bytes = Vec::new();
        body.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, format!("payload {n}").as_bytes());
        backend.delete("metalog-test", key).await.unwrap();
        backend.delete("metalog-test", key).await.unwrap();
        assert!(!backend.exists("metalog-test", key).await.unwrap());
        assert!(backend
            .get("metalog-test", key)
            .await
            .err()
            .unwrap()
            .is_not_found());
    }
    // MinIO aliases leading slashes; reject ambiguous identities before any
    // request can overwrite or delete the canonical object's contents.
    backend
        .put("metalog-test", "a/b", b"original")
        .await
        .unwrap();
    for alias in ["/a/b", "a//b", "a/./b", "a/c/../b", "a/b/", "a\\b"] {
        assert!(backend
            .put("metalog-test", alias, b"wrong-object")
            .await
            .is_err());
        assert!(backend.delete("metalog-test", alias).await.is_err());
        assert!(backend.get("metalog-test", alias).await.is_err());
        assert!(backend.exists("metalog-test", alias).await.is_err());
    }
    let mut body = backend.get("metalog-test", "a/b").await.unwrap();
    let mut bytes = Vec::new();
    body.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, b"original");
    backend.delete("metalog-test", "a/b").await.unwrap();

    config.secret_key = "wrong-password".into();
    let invalid = S3Backend::new(&config).await.unwrap();
    assert!(invalid
        .put("metalog-test", "forbidden", b"bad")
        .await
        .is_err());
    assert!(invalid.exists("metalog-test", "forbidden").await.is_err());
    // Native AWS uses literal keys; configuring a compatible endpoint switches
    // to the stricter policy regardless of the backend's `s3` type label.
    let mut native_config = config.clone();
    native_config.endpoint.clear();
    let native = S3Backend::new(&native_config).await.unwrap();
    assert!(!native.requires_canonical_keys());
    let mut native_registry = metalog_storage::Registry::new();
    native_registry.register("aws", std::sync::Arc::new(native));
    assert!(native_registry
        .validate_location("aws", "metalog-test", "/a//b")
        .is_ok());
    // Exercise the complete retention path through SQL and the real S3 backend.
    let (pool, _db) = metalog_it::helpers::setup_db_with_table("s3_retention").await;
    backend
        .put("metalog-test", "expired 日本語", b"expired")
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO s3_retention (min_timestamp, max_timestamp, state, expires_at, \
         retention_days, file_storage_backend, file_bucket, file_path) VALUES (100, 100, \
         'IR_CLOSED', 1, 1, 'minio', 'metalog-test', ?)",
    )
    .bind("expired 日本語")
    .execute(&pool)
    .await
    .unwrap();
    let mut registry = metalog_storage::Registry::new();
    registry.register("minio", std::sync::Arc::new(backend));
    use metalog_retention::RetentionPolicy;
    metalog_retention::RetentionModule::new()
        .run_once(
            &metalog_metastore::FileRecords::new(pool.clone(), "s3_retention"),
            Some(&registry),
            0,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(!registry
        .get("minio")
        .unwrap()
        .exists("metalog-test", "expired 日本語")
        .await
        .unwrap());
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM s3_retention")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

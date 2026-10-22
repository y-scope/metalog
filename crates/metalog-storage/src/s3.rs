use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::{
    config::{retry::RetryConfig, timeout::TimeoutConfig, Credentials, Region},
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    Client,
};
use metalog_config::StorageBackendConfig;
use tokio::io::AsyncRead;

use crate::{Backend, StorageError};

/// S3 and S3-compatible storage. Native AWS keys are literal; custom endpoints
/// require canonical keys because servers such as MinIO normalize some paths.
pub struct S3Backend {
    client: Client,
    default_bucket: String,
    requires_canonical_keys: bool,
}

impl S3Backend {
    pub async fn new(config: &StorageBackendConfig) -> Result<Self, StorageError> {
        if config.access_key.is_empty() != config.secret_key.is_empty() {
            return Err(StorageError::Other(
                "both access_key and secret_key must be configured".into(),
            ));
        }
        let region = if config.region.is_empty() {
            aws_config::meta::region::RegionProviderChain::default_provider()
                .or_else(Region::new("us-east-1"))
        } else {
            aws_config::meta::region::RegionProviderChain::first_try(Region::new(
                config.region.clone(),
            ))
        };
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
        if !config.access_key.is_empty() {
            loader = loader.credentials_provider(Credentials::new(
                &config.access_key,
                &config.secret_key,
                (!config.session_token.is_empty()).then(|| config.session_token.clone()),
                None,
                "metalog-config",
            ));
        }
        let shared = loader.load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(config.force_path_style)
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(60))
                    .operation_attempt_timeout(Duration::from_secs(20))
                    .connect_timeout(Duration::from_secs(5))
                    .build(),
            );
        // The endpoint and identity policy must come from the same configuration.
        // Clear SDK environment/profile overrides when no endpoint is specified.
        builder.set_endpoint_url((!config.endpoint.is_empty()).then(|| config.endpoint.clone()));
        Ok(Self {
            client: Client::from_conf(builder.build()),
            default_bucket: config.bucket.clone(),
            requires_canonical_keys: !config.endpoint.is_empty() || config.type_name == "minio",
        })
    }

    fn bucket<'a>(&'a self, bucket: &'a str) -> Result<&'a str, StorageError> {
        let bucket = if bucket.is_empty() {
            &self.default_bucket
        } else {
            bucket
        };
        if bucket.is_empty() {
            return Err(StorageError::Other("S3 bucket is required".into()));
        }
        Ok(bucket)
    }

    fn validate_location<'a>(
        &'a self,
        bucket: &'a str,
        key: &str,
    ) -> Result<&'a str, StorageError> {
        let bucket = self.bucket(bucket)?;
        if self.requires_canonical_keys {
            crate::backend::validate_canonical_location(bucket, key)?;
        }
        Ok(bucket)
    }
}

fn service_error(code: Option<&str>, message: String, bucket: &str, key: &str) -> StorageError {
    match code {
        Some("NoSuchKey" | "NotFound" | "404") => StorageError::NotFound {
            bucket: bucket.into(),
            key: key.into(),
        },
        Some("AccessDenied" | "Forbidden" | "403") => StorageError::AccessDenied {
            bucket: bucket.into(),
            key: key.into(),
        },
        _ => StorageError::Other(format!(
            "S3 {}: {message}",
            code.unwrap_or("request failed")
        )),
    }
}

#[async_trait]
impl Backend for S3Backend {
    fn requires_canonical_keys(&self) -> bool {
        self.requires_canonical_keys
    }

    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
        let bucket = self.validate_location(bucket, key)?;
        let object = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                service_error(
                    e.as_service_error().and_then(|e| e.code()),
                    e.as_service_error()
                        .and_then(|e| e.message())
                        .map(str::to_owned)
                        .unwrap_or_else(|| e.to_string()),
                    bucket,
                    key,
                )
            })?;
        Ok(Box::new(object.body.into_async_read()))
    }

    async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let bucket = self.validate_location(bucket, key)?;
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| {
                service_error(
                    e.as_service_error().and_then(|e| e.code()),
                    e.as_service_error()
                        .and_then(|e| e.message())
                        .map(str::to_owned)
                        .unwrap_or_else(|| e.to_string()),
                    bucket,
                    key,
                )
            })?;
        Ok(())
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let bucket = self.validate_location(bucket, key)?;
        match self
            .client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let error = service_error(
                    e.as_service_error().and_then(|e| e.code()),
                    e.as_service_error()
                        .and_then(|e| e.message())
                        .map(str::to_owned)
                        .unwrap_or_else(|| e.to_string()),
                    bucket,
                    key,
                );
                if error.is_not_found() {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let bucket = self.validate_location(bucket, key)?;
        match self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let error = service_error(
                    e.as_service_error().and_then(|e| e.code()),
                    e.as_service_error()
                        .and_then(|e| e.message())
                        .map(str::to_owned)
                        .unwrap_or_else(|| e.to_string()),
                    bucket,
                    key,
                );
                if error.is_not_found() {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }
}

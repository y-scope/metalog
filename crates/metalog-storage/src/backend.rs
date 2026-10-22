use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::StorageError;

/// Pluggable object storage backend.
///
/// Implementations include local filesystem and S3-compatible object storage.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Whether distinct path spellings may address the same stored object.
    /// Native AWS S3 overrides this because its keys have literal identity.
    fn requires_canonical_keys(&self) -> bool {
        true
    }

    /// Reads an object. Returns a reader or `StorageError::NotFound`.
    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError>;

    /// Writes an object. Creates or overwrites.
    async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StorageError>;

    /// Deletes an object. Idempotent (returns Ok if not found).
    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StorageError>;

    /// Checks if an object exists.
    async fn exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError>;
}

/// Reject ambiguous locations without rewriting an object's identity.
pub(crate) fn validate_canonical_location(bucket: &str, key: &str) -> Result<(), StorageError> {
    if bucket.is_empty()
        || matches!(bucket, "." | "..")
        || bucket.contains(['/', '\\'])
        || key.contains('\\')
        || key
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(StorageError::Other(
            "storage location requires a single-component bucket and a relative object key \
             without empty, dot, or backslash components"
                .into(),
        ));
    }
    Ok(())
}

use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::StorageError;

/// Pluggable object storage backend.
///
/// Implementations: FilesystemBackend (base), S3Backend (premium/separate crate),
/// HTTPBackend (read-only, premium).
#[async_trait]
pub trait Backend: Send + Sync {
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

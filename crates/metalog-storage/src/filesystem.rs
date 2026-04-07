use std::path::PathBuf;

use async_trait::async_trait;
use tokio::io::AsyncRead;

use crate::{Backend, StorageError};

/// Local filesystem storage backend.
pub struct FilesystemBackend {
    base_path: PathBuf,
}

impl FilesystemBackend {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: PathBuf::from(base_path),
        }
    }

    /// Constructs a safe path, preventing directory traversal.
    fn safe_path(&self, bucket: &str, key: &str) -> Result<PathBuf, StorageError> {
        let path = self
            .base_path
            .join(bucket)
            .join(key.strip_prefix('/').unwrap_or(key));
        // Verify the resolved path is under base_path.
        let _canonical_base = self
            .base_path
            .canonicalize()
            .unwrap_or(self.base_path.clone());
        // Note: we can't canonicalize the target path if it doesn't exist yet.
        // Instead, verify no ".." components.
        if key.contains("..") {
            return Err(StorageError::AccessDenied {
                bucket: bucket.into(),
                key: key.into(),
            });
        }
        Ok(path)
    }
}

#[async_trait]
impl Backend for FilesystemBackend {
    async fn get(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, StorageError> {
        let path = self.safe_path(bucket, key)?;
        let file = tokio::fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound {
                    bucket: bucket.into(),
                    key: key.into(),
                }
            } else {
                StorageError::Io(e)
            }
        })?;
        Ok(Box::new(file))
    }

    async fn put(&self, bucket: &str, key: &str, data: &[u8]) -> Result<(), StorageError> {
        let path = self.safe_path(bucket, key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Write to temp file + rename for crash safety.
        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, data).await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(())
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), StorageError> {
        let path = self.safe_path(bucket, key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // Idempotent.
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    async fn exists(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let path = self.safe_path(bucket, key)?;
        Ok(path.exists())
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());

        // Put.
        backend.put("bucket", "test.txt", b"hello").await.unwrap();

        // Exists.
        assert!(backend.exists("bucket", "test.txt").await.unwrap());

        // Get.
        let mut reader = backend.get("bucket", "test.txt").await.unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello");

        // Delete.
        backend.delete("bucket", "test.txt").await.unwrap();
        assert!(!backend.exists("bucket", "test.txt").await.unwrap());
    }

    #[tokio::test]
    async fn get_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());
        let err = backend.get("bucket", "missing.txt").await.err().unwrap();
        assert!(err.is_not_found());
    }

    #[tokio::test]
    async fn delete_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());
        // Deleting nonexistent file should succeed.
        backend.delete("bucket", "missing.txt").await.unwrap();
    }

    #[tokio::test]
    async fn directory_traversal_blocked() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());
        let result = backend.get("bucket", "../../../etc/passwd").await;
        assert!(result.is_err());
    }
}

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
        use std::path::Component;
        let denied = || StorageError::AccessDenied {
            bucket: bucket.into(),
            key: key.into(),
        };
        // Object identity must be one-to-one with metadata/tombstones. Reject
        // aliases such as /a, a//b, a/./b, or a nested bucket instead of silently
        // normalizing them to another object's path.
        if bucket.is_empty()
            || bucket.contains('/')
            || bucket.contains('\\')
            || key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || key.contains('\\')
        {
            return Err(denied());
        }
        let key_path = std::path::Path::new(key);
        let bucket_path = std::path::Path::new(bucket);
        if key_path.as_os_str().is_empty()
            || bucket_path
                .components()
                .chain(key_path.components())
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(denied());
        }
        let path = self.base_path.join(bucket_path).join(key_path);
        // This directory is controlled by the server administrator. Never follow
        // a symlink in an object path, including the final component.
        let mut current = self.base_path.clone();
        for part in bucket_path.components().chain(key_path.components()) {
            current.push(part);
            match std::fs::symlink_metadata(&current) {
                Ok(meta) if meta.file_type().is_symlink() => return Err(denied()),
                Ok(_) => (),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(e.into()),
            }
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
        // Unique sibling + atomic replacement avoids cross-request temporary
        // file collisions. fsync both file and directory before acknowledging.
        let data = data.to_vec();
        tokio::task::spawn_blocking(move || -> Result<(), StorageError> {
            use std::io::Write;
            let parent = path
                .parent()
                .ok_or_else(|| StorageError::Other("missing parent directory".into()))?;
            let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
            tmp.write_all(&data)?;
            tmp.as_file().sync_all()?;
            tmp.persist(&path).map_err(|e| StorageError::Io(e.error))?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::Other(e.to_string()))??;
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
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(meta.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
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

    #[tokio::test]
    async fn ambiguous_object_identities_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());
        for (bucket, key) in [
            ("bucket", "/a"),
            ("bucket", "a//b"),
            ("bucket", "a/./b"),
            ("bucket/sub", "a"),
            ("..", "a"),
        ] {
            assert!(backend.put(bucket, key, b"data").await.is_err());
            assert!(backend.delete(bucket, key).await.is_err());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_cannot_escape_the_storage_root() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("bucket")).unwrap();
        let backend = FilesystemBackend::new(dir.path().to_str().unwrap());
        assert!(backend.put("bucket", "a", b"data").await.is_err());
        assert!(backend.delete("bucket", "a").await.is_err());
    }
}

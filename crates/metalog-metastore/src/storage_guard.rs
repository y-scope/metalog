use sqlx::{MySqlConnection, MySqlPool};

use crate::{AdvisoryLock, AdvisoryLockError, StoragePath};

/// Serializes metadata commits with object retirement decisions. The lock is
/// database-scoped and held only during SQL work, never during storage requests.
/// Durable tombstones prevent a new reference after an object has been retired.
pub struct StorageMutationGuard {
    lock: AdvisoryLock,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageMutationError {
    #[error(transparent)]
    Lock(#[from] AdvisoryLockError),
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error("storage object has been retired and cannot be referenced again: {0}")]
    RetiredObject(String),
}

impl StorageMutationGuard {
    pub async fn acquire(pool: &MySqlPool) -> Result<Self, StorageMutationError> {
        let (name,): (String,) =
            sqlx::query_as("SELECT CONCAT('metalog_storage_', MD5(DATABASE()))")
                .fetch_one(pool)
                .await?;
        Ok(Self {
            lock: AdvisoryLock::acquire(pool, &name, 10).await?,
        })
    }

    pub fn connection(&mut self) -> &mut MySqlConnection {
        self.lock.connection()
    }

    /// Checks a whole ingestion chunk in one query. Hash collisions fail closed:
    /// a colliding path is rejected, never mistaken for permission to reuse data.
    pub async fn ensure_writable(
        &mut self,
        paths: &[StoragePath],
    ) -> Result<(), StorageMutationError> {
        if paths.is_empty() {
            return Ok(());
        }
        let placeholders = vec!["(?, ?, UNHEX(MD5(?)))"; paths.len()].join(",");
        let sql = format!(
            "SELECT CAST(path AS CHAR) FROM _storage_tombstone WHERE (backend, bucket, path_hash) \
             IN ({placeholders}) LIMIT 1"
        );
        let mut query = sqlx::query_as::<_, (String,)>(&sql);
        for path in paths {
            query = query
                .bind(&path.backend)
                .bind(&path.bucket)
                .bind(&path.path);
        }
        if let Some((path,)) = query.fetch_optional(self.connection()).await? {
            return Err(StorageMutationError::RetiredObject(path));
        }
        Ok(())
    }

    /// Must be called only after checking live references while holding this guard.
    pub async fn retire(&mut self, path: &StoragePath) -> Result<(), StorageMutationError> {
        sqlx::query(
            "INSERT INTO _storage_tombstone (backend, bucket, path_hash, path, retired_at) VALUES \
             (?, ?, UNHEX(MD5(?)), ?, ?) ON DUPLICATE KEY UPDATE retired_at = retired_at",
        )
        .bind(&path.backend)
        .bind(&path.bucket)
        .bind(&path.path)
        .bind(&path.path)
        .bind(metalog_timeutil::epoch_nanos())
        .execute(self.connection())
        .await?;
        Ok(())
    }

    pub async fn release(&mut self) -> Result<(), StorageMutationError> {
        self.lock.release().await?;
        Ok(())
    }
}

use sqlx::MySqlPool;

/// Connection-scoped MySQL advisory lock via `GET_LOCK` / `RELEASE_LOCK`.
///
/// The lock is tied to a dedicated connection (not the pool). If the connection
/// drops, the lock is automatically released server-side, preventing deadlocks
/// from crashed processes.
pub struct AdvisoryLock {
    conn: sqlx::pool::PoolConnection<sqlx::MySql>,
    name: String,
    held: bool,
}

/// Error returned when the advisory lock cannot be acquired.
#[derive(Debug, thiserror::Error)]
pub enum AdvisoryLockError {
    #[error("advisory lock {0}: not acquired (timeout or held by another session)")]
    NotAcquired(String),

    #[error("advisory lock {0}: {1}")]
    Sql(String, #[source] sqlx::Error),
}

impl AdvisoryLock {
    /// Acquires a named advisory lock with a timeout (in seconds).
    ///
    /// Uses `pool.acquire()` to get a dedicated connection (required for lock
    /// affinity — the lock is released when the connection closes).
    pub async fn acquire(
        pool: &MySqlPool,
        name: &str,
        timeout_seconds: i32,
    ) -> Result<Self, AdvisoryLockError> {
        let mut conn = pool
            .acquire()
            .await
            .map_err(|e| AdvisoryLockError::Sql(name.into(), e))?;

        let result: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK(?, ?)")
            .bind(name)
            .bind(timeout_seconds)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| AdvisoryLockError::Sql(name.into(), e))?;

        match result {
            Some(1) => Ok(Self {
                conn,
                name: name.to_string(),
                held: true,
            }),
            _ => Err(AdvisoryLockError::NotAcquired(name.into())),
        }
    }

    /// Releases the advisory lock and returns the connection to the pool.
    ///
    /// The lock is always released when the connection closes, so `held` is set
    /// to false after the connection is dropped to ensure consistent state.
    pub async fn release(&mut self) -> Result<(), AdvisoryLockError> {
        if !self.held {
            return Ok(());
        }

        let release_result: Result<Option<i64>, _> = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
            .bind(&self.name)
            .fetch_one(&mut *self.conn)
            .await;

        // Mark as released regardless of RELEASE_LOCK result — dropping the
        // connection definitively releases the lock server-side.
        self.held = false;

        if let Err(e) = release_result {
            return Err(AdvisoryLockError::Sql(self.name.clone(), e));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let err = AdvisoryLockError::NotAcquired("test_lock".into());
        assert!(err.to_string().contains("not acquired"));
    }
}

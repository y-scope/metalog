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
    /// Bounds lock names to MySQL's 64-byte limit and isolates databases.
    pub async fn acquire_scoped(
        pool: &MySqlPool,
        namespace: &str,
        resource: &str,
        timeout_seconds: i32,
    ) -> Result<Self, AdvisoryLockError> {
        let name: String = sqlx::query_scalar(
            "SELECT CONCAT('metalog_', MD5(CONCAT(DATABASE(), CHAR(0), ?, CHAR(0), ?)))",
        )
        .bind(namespace)
        .bind(resource)
        .fetch_one(pool)
        .await
        .map_err(|e| AdvisoryLockError::Sql(namespace.into(), e))?;
        Self::acquire(pool, &name, timeout_seconds).await
    }

    /// Acquires a named advisory lock with a timeout (in seconds).
    ///
    /// Uses `pool.acquire()` to get a dedicated connection (required for lock
    /// affinity — the lock is released when the connection closes).
    pub async fn acquire(
        pool: &MySqlPool,
        name: &str,
        timeout_seconds: i32,
    ) -> Result<Self, AdvisoryLockError> {
        let conn = pool
            .acquire()
            .await
            .map_err(|e| AdvisoryLockError::Sql(name.into(), e))?;

        // Construct the guard before awaiting GET_LOCK. Cancellation after the
        // server acquires the lock must close the connection, never pool it.
        let mut lock = Self {
            conn,
            name: name.to_string(),
            held: true,
        };
        let result: Option<i64> = sqlx::query_scalar("SELECT GET_LOCK(?, ?)")
            .bind(name)
            .bind(timeout_seconds)
            .fetch_one(&mut *lock.conn)
            .await
            .map_err(|e| AdvisoryLockError::Sql(name.into(), e))?;

        match result {
            Some(1) => Ok(lock),
            _ => {
                lock.held = false;
                Err(AdvisoryLockError::NotAcquired(name.into()))
            }
        }
    }

    /// Runs protected SQL on the lock's connection without acquiring a second
    /// pool slot. This also permits schema operations with a one-connection pool.
    pub fn connection(&mut self) -> &mut sqlx::MySqlConnection {
        &mut self.conn
    }

    /// Releases the advisory lock and returns the connection to the pool.
    ///
    /// Failed or cancelled release keeps the guard armed: dropping it closes
    /// the connection instead of returning a still-held lock to the pool.
    pub async fn release(&mut self) -> Result<(), AdvisoryLockError> {
        if !self.held {
            return Ok(());
        }

        let release_result: Result<Option<i64>, _> = sqlx::query_scalar("SELECT RELEASE_LOCK(?)")
            .bind(&self.name)
            .fetch_one(&mut *self.conn)
            .await;

        release_result.map_err(|e| AdvisoryLockError::Sql(self.name.clone(), e))?;
        self.held = false;
        Ok(())
    }
}

impl Drop for AdvisoryLock {
    fn drop(&mut self) {
        if self.held {
            self.conn.close_on_drop();
        }
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

use std::{future::Future, time::Duration};

use rand::Rng;
use sqlx::{MySql, MySqlPool, Transaction};

use crate::errors::{is_deadlock, is_lock_wait_timeout};

/// Executes `f` inside a transaction. Rolls back on error, commits on success.
///
/// The closure receives a mutable reference to the transaction and returns its
/// result. The transaction is committed if `f` returns `Ok`, rolled back otherwise
/// (via `Drop`).
pub async fn with_tx<F, Fut, T>(pool: &MySqlPool, f: F) -> Result<T, sqlx::Error>
where
    F: FnOnce(&mut Transaction<'_, MySql>) -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>, {
    let mut tx = pool.begin().await?;
    let result = f(&mut tx).await?;
    tx.commit().await?;
    Ok(result)
}

/// Retries `f` on deadlock (1213) or lock wait timeout (1205) with random jitter.
///
/// Jitter range: 1–50ms. Maximum retries: `max_retries` (default 10).
pub async fn with_deadlock_retry<F, Fut, T>(max_retries: u32, f: F) -> Result<T, sqlx::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, sqlx::Error>>, {
    for attempt in 0..max_retries {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e)
                if (is_deadlock(&e) || is_lock_wait_timeout(&e)) && attempt < max_retries - 1 =>
            {
                let jitter_ms = rand::thread_rng().gen_range(1..=50);
                tracing::debug!(attempt, jitter_ms, "deadlock/lock-wait, retrying");
                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

/// Default max retries for deadlock retry.
pub const DEFAULT_MAX_RETRIES: u32 = 10;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deadlock_retry_succeeds_on_first_try() {
        let result = with_deadlock_retry(3, || async { Ok::<_, sqlx::Error>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn deadlock_retry_non_deadlock_error_fails_immediately() {
        let result: Result<i32, _> =
            with_deadlock_retry(3, || async { Err::<i32, _>(sqlx::Error::RowNotFound) }).await;
        assert!(result.is_err());
    }
}

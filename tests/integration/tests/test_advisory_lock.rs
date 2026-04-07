#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db;
    use metalog_metastore::AdvisoryLock;

    #[tokio::test]
    async fn advisory_lock_acquire_release() {
        let (pool, _container) = setup_db().await;

        let mut lock = AdvisoryLock::acquire(&pool, "test_lock", 1).await.unwrap();
        lock.release().await.unwrap();
    }

    #[tokio::test]
    async fn advisory_lock_blocks_concurrent() {
        let (pool, _container) = setup_db().await;

        let _lock1 = AdvisoryLock::acquire(&pool, "test_lock_2", 1)
            .await
            .unwrap();

        // Second acquire with 0 timeout should fail.
        let result = AdvisoryLock::acquire(&pool, "test_lock_2", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn advisory_lock_release_idempotent() {
        let (pool, _container) = setup_db().await;

        let mut lock = AdvisoryLock::acquire(&pool, "test_lock_3", 1)
            .await
            .unwrap();
        lock.release().await.unwrap();
        // Second release is a no-op.
        lock.release().await.unwrap();
    }
}

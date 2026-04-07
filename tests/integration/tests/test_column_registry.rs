#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_schema::ColumnRegistry;

    #[tokio::test]
    async fn resolve_or_allocate_dim() {
        let (pool, _container) = setup_db_with_table("test_dims").await;

        let reg = ColumnRegistry::new(pool, "test_dims").await.unwrap();

        // First allocation — creates dim_f01.
        let col = reg
            .resolve_or_allocate_dim("hostname", "str", 256)
            .await
            .unwrap();
        assert_eq!(col, "dim_f01");

        // Second call — returns cached.
        let col2 = reg
            .resolve_or_allocate_dim("hostname", "str", 256)
            .await
            .unwrap();
        assert_eq!(col2, "dim_f01");
    }

    #[tokio::test]
    async fn allocate_multiple_dims() {
        let (pool, _container) = setup_db_with_table("test_multi_dims").await;

        let reg = ColumnRegistry::new(pool, "test_multi_dims").await.unwrap();

        let col1 = reg
            .resolve_or_allocate_dim("service", "str", 128)
            .await
            .unwrap();
        let col2 = reg
            .resolve_or_allocate_dim("host", "str", 256)
            .await
            .unwrap();
        let col3 = reg
            .resolve_or_allocate_dim("zone", "str", 128)
            .await
            .unwrap();

        assert_eq!(col1, "dim_f01");
        assert_eq!(col2, "dim_f02");
        assert_eq!(col3, "dim_f03");
    }

    #[tokio::test]
    async fn snapshot() {
        let (pool, _container) = setup_db_with_table("test_snapshot").await;

        let reg = ColumnRegistry::new(pool, "test_snapshot").await.unwrap();
        reg.resolve_or_allocate_dim("app", "str", 128)
            .await
            .unwrap();

        let snap = reg.snapshot().await;
        assert_eq!(snap.resolve_dim("app"), Some("dim_f01"));
        assert_eq!(snap.resolve_dim("missing"), None);
    }

    #[tokio::test]
    async fn entry_count() {
        let (pool, _container) = setup_db_with_table("test_count").await;

        let reg = ColumnRegistry::new(pool, "test_count").await.unwrap();
        assert_eq!(reg.entry_count().await, 0);

        reg.resolve_or_allocate_dim("a", "str", 128).await.unwrap();
        reg.resolve_or_allocate_dim("b", "int", 0).await.unwrap();
        assert_eq!(reg.entry_count().await, 2);
    }
}

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

        let snap = reg.snapshot();
        assert_eq!(snap.resolve_dim("app"), Some("dim_f01"));
        assert_eq!(snap.resolve_dim("missing"), None);
    }

    #[tokio::test]
    async fn entry_count() {
        let (pool, _container) = setup_db_with_table("test_count").await;

        let reg = ColumnRegistry::new(pool, "test_count").await.unwrap();
        assert_eq!(reg.entry_count(), 0);

        reg.resolve_or_allocate_dim("a", "str", 128).await.unwrap();
        reg.resolve_or_allocate_dim("b", "int", 0).await.unwrap();
        assert_eq!(reg.entry_count(), 2);
    }
}

#[tokio::test]
async fn independent_registries_and_crash_orphans_never_reuse_a_slot() {
    let (pool, _container) = metalog_it::helpers::setup_db_with_table("registry_race").await;
    let a = metalog_schema::ColumnRegistry::new(pool.clone(), "registry_race")
        .await
        .unwrap();
    let b = metalog_schema::ColumnRegistry::new(pool.clone(), "registry_race")
        .await
        .unwrap();
    let (first, second) = tokio::join!(
        a.resolve_or_allocate_dim("host", "str", 64),
        b.resolve_or_allocate_dim("zone", "int", 0)
    );
    assert_ne!(first.unwrap(), second.unwrap());
    sqlx::query("ALTER TABLE registry_race ADD COLUMN dim_f03 DOUBLE NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        a.resolve_or_allocate_dim("after_crash", "str", 64)
            .await
            .unwrap(),
        "dim_f04"
    );
    sqlx::query(
        "UPDATE _dim_registry SET state = 'INVALIDATED' WHERE table_name = 'registry_race' AND \
         dim_key = 'host'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let fresh = metalog_schema::ColumnRegistry::new(pool.clone(), "registry_race")
        .await
        .unwrap();
    assert_eq!(
        fresh
            .resolve_or_allocate_dim("host", "str", 64)
            .await
            .unwrap(),
        "dim_f05"
    );
    assert_eq!(
        fresh
            .resolve_or_allocate_dim("host", "str", 1024)
            .await
            .unwrap(),
        "dim_f05"
    );
    assert!(fresh
        .resolve_or_allocate_dim("host", "int", 0)
        .await
        .is_err());
    let width: u64 = sqlx::query_scalar(
        "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = \
         DATABASE() AND TABLE_NAME = 'registry_race' AND COLUMN_NAME = 'dim_f05'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(width, 1024);
}

use metalog_config::DatabaseConfig;
use metalog_it::helpers::setup_db;

#[tokio::test]
async fn configured_pool_connects_with_empty_and_literal_passwords() {
    let (admin, container) = setup_db().await;
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let mut config = DatabaseConfig {
        host: "127.0.0.1".into(),
        port,
        database: "test".into(),
        user: "root".into(),
        password: String::new(),
        pool_size: 1,
        pool_min_idle: 1,
    };
    let passwordless = metalog_db::new_pool(&config).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&passwordless)
            .await
            .unwrap(),
        1
    );
    passwordless.close().await;

    // These credentials belong only to this disposable container. URI delimiters
    // must remain literal, rather than changing host/database parsing.
    sqlx::query("CREATE USER 'pool_config_test'@'%' IDENTIFIED BY 'p@ss:/?#%word'")
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query("GRANT SELECT ON test.* TO 'pool_config_test'@'%'")
        .execute(&admin)
        .await
        .unwrap();
    config.user = "pool_config_test".into();
    config.password = "p@ss:/?#%word".into();
    let authenticated = metalog_db::new_pool(&config).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&authenticated)
            .await
            .unwrap(),
        1
    );
    authenticated.close().await;
}

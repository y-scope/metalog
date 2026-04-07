use sqlx::MySqlPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mariadb::Mariadb;

/// Sets up a MariaDB testcontainer, connects, and runs schema DDL.
/// Returns (pool, container) — keep container alive for test duration.
pub async fn setup_db() -> (MySqlPool, testcontainers::ContainerAsync<Mariadb>) {
    let container = Mariadb::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let dsn = format!("mysql://root@127.0.0.1:{port}/test");
    let pool = MySqlPool::connect(&dsn).await.unwrap();

    // Run schema DDL.
    metalog_schema::execute_ddl_statements(&pool, metalog_schema::SCHEMA_SQL)
        .await
        .unwrap();

    (pool, container)
}

/// Sets up DB + provisions a named table.
pub async fn setup_db_with_table(
    table_name: &str,
) -> (MySqlPool, testcontainers::ContainerAsync<Mariadb>) {
    let (pool, container) = setup_db().await;
    metalog_schema::ensure_table(&pool, table_name, None)
        .await
        .unwrap();
    (pool, container)
}

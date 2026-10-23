use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    Executor,
    MySqlPool,
};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mariadb::Mariadb;

async fn account_pool(port: u16, user: &str, password: &str) -> MySqlPool {
    // The monitor must be able to authenticate without access to any database.
    MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(
            MySqlConnectOptions::new()
                .host("127.0.0.1")
                .port(port)
                .username(user)
                .password(password),
        )
        .await
        .unwrap()
}

async fn assert_denied(pool: &MySqlPool, statement: &str) {
    let error = pool.execute(statement).await.unwrap_err();
    let number = error
        .as_database_error()
        .and_then(|error| error.try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>())
        .map(|error| error.number());
    assert!(
        matches!(number, Some(1044 | 1142 | 1227)),
        "expected a privilege error for {statement:?}, got {error}"
    );
}

#[tokio::test]
async fn repeated_bootstrap_resets_existing_account_grants_and_roles() {
    // This exercises root provisioning directly, including the exact database
    // name in the deployed SQL, independently of the application proxy mode.
    let database = Mariadb::default().start().await.unwrap();
    let port = database.get_host_port_ipv4(3306).await.unwrap();
    let admin = MySqlPool::connect(&format!("mysql://root@127.0.0.1:{port}/test"))
        .await
        .unwrap();
    metalog_schema::execute_ddl_statements(
        &admin,
        "CREATE DATABASE metalog_metastore;
         CREATE TABLE metalog_metastore.bootstrap_probe (id INT PRIMARY KEY);
         INSERT INTO metalog_metastore.bootstrap_probe VALUES (1);
         CREATE ROLE bootstrap_legacy_writer;
         GRANT INSERT, CREATE ON metalog_metastore.* TO bootstrap_legacy_writer;",
    )
    .await
    .unwrap();
    for user in ["metalog_writer", "metalog_reader", "metalog_monitor"] {
        metalog_schema::execute_ddl_statements(
            &admin,
            &format!(
                "CREATE USER '{user}'@'%' IDENTIFIED BY 'obsolete_password';
                 GRANT ALL PRIVILEGES ON *.* TO '{user}'@'%' WITH GRANT OPTION;
                 GRANT bootstrap_legacy_writer TO '{user}'@'%';
                 SET DEFAULT ROLE bootstrap_legacy_writer FOR '{user}'@'%';"
            ),
        )
        .await
        .unwrap();
    }

    for _ in 0..2 {
        metalog_schema::execute_ddl_statements(
            &admin,
            include_str!("../../../docker/proxysql-users.sql"),
        )
        .await
        .unwrap();

        let reader = account_pool(port, "metalog_reader", "metalog_reader_dev").await;
        let writer = account_pool(port, "metalog_writer", "metalog_writer_dev").await;
        let monitor = account_pool(port, "metalog_monitor", "metalog_monitor_dev").await;

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM metalog_metastore.bootstrap_probe")
                .fetch_one(&reader)
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert_denied(
            &reader,
            "INSERT INTO metalog_metastore.bootstrap_probe VALUES (2)",
        )
        .await;
        assert_denied(
            &reader,
            "CREATE TABLE metalog_metastore.reader_forbidden (id INT)",
        )
        .await;
        assert_denied(&reader, "SELECT User FROM mysql.user").await;

        writer
            .execute("CREATE TABLE metalog_metastore.writer_allowed (id INT)")
            .await
            .unwrap();
        writer
            .execute("INSERT INTO metalog_metastore.writer_allowed VALUES (1)")
            .await
            .unwrap();
        writer
            .execute("DROP TABLE metalog_metastore.writer_allowed")
            .await
            .unwrap();
        assert_denied(
            &writer,
            "CREATE USER 'bootstrap_forbidden_admin'@'%' IDENTIFIED BY 'unused'",
        )
        .await;

        monitor.execute("SELECT 1").await.unwrap();
        assert_denied(&monitor, "SELECT * FROM metalog_metastore.bootstrap_probe").await;
        assert_denied(&monitor, "SELECT User FROM mysql.user").await;

        for pool in [&reader, &writer, &monitor] {
            let role: Option<String> = sqlx::query_scalar("SELECT CURRENT_ROLE()")
                .fetch_one(pool)
                .await
                .unwrap();
            assert_eq!(role, None, "bootstrap preserved a default role");
            assert!(pool
                .execute("SET ROLE bootstrap_legacy_writer")
                .await
                .is_err());
            pool.close().await;
        }
    }
}

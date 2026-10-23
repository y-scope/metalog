use std::ops::Deref;

use metalog_config::DatabaseConfig;
use sqlx::MySqlPool;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mariadb::Mariadb;

use crate::proxysql::ProxySqlFixture;

/// Owns the disposable database and optional proxy for the whole test. The raw
/// MariaDB container remains accessible for tests of database administration.
pub struct TestDatabase {
    proxy: Option<ProxySqlFixture>,
    database: testcontainers::ContainerAsync<Mariadb>,
    direct_config: DatabaseConfig,
}

impl TestDatabase {
    pub fn writer_config(&self) -> DatabaseConfig {
        self.proxy
            .as_ref()
            .map(ProxySqlFixture::writer_config)
            .unwrap_or_else(|| self.direct_config.clone())
    }

    pub fn reader_config(&self) -> DatabaseConfig {
        self.proxy
            .as_ref()
            .map(ProxySqlFixture::reader_config)
            .unwrap_or_else(|| self.direct_config.clone())
    }

    /// Bypasses the proxy only for root administration and independent witnesses.
    pub async fn direct_pool(&self) -> MySqlPool {
        metalog_db::new_pool(&self.direct_config).await.unwrap()
    }

    pub fn proxysql(&self) -> Option<&ProxySqlFixture> {
        self.proxy.as_ref()
    }
}

impl Deref for TestDatabase {
    type Target = testcontainers::ContainerAsync<Mariadb>;

    fn deref(&self) -> &Self::Target {
        &self.database
    }
}

/// Uses a direct MariaDB connection by default. Set METALOG_TEST_PROXYSQL=1 to
/// exercise existing database tests through the checked-in writer proxy policy.
pub async fn setup_db() -> (MySqlPool, TestDatabase) {
    setup_db_mode(std::env::var("METALOG_TEST_PROXYSQL").as_deref() == Ok("1")).await
}

/// Always starts the proxy, independently of process-wide environment variables.
pub async fn setup_proxy_db() -> (MySqlPool, TestDatabase) {
    setup_db_mode(true).await
}

async fn setup_db_mode(use_proxy: bool) -> (MySqlPool, TestDatabase) {
    let container = Mariadb::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let direct_config = DatabaseConfig {
        host: "127.0.0.1".into(),
        port,
        database: "test".into(),
        user: "root".into(),
        password: String::new(),
        pool_size: 10,
        pool_min_idle: 1,
    };
    let direct = metalog_db::new_pool(&direct_config).await.unwrap();
    let proxy = if use_proxy {
        Some(ProxySqlFixture::launch(&container, &direct).await)
    } else {
        None
    };
    let fixture = TestDatabase {
        proxy,
        database: container,
        direct_config,
    };
    let pool = if fixture.proxy.is_some() {
        metalog_db::new_pool(&fixture.writer_config())
            .await
            .unwrap()
    } else {
        direct
    };

    // Schema bootstrap must use the proxy too, including prepared DDL statements.
    metalog_schema::execute_ddl_statements(&pool, metalog_schema::SCHEMA_SQL)
        .await
        .unwrap();

    (pool, fixture)
}

/// Sets up DB + provisions a named table.
pub async fn setup_db_with_table(table_name: &str) -> (MySqlPool, TestDatabase) {
    let (pool, container) = setup_db().await;
    metalog_schema::ensure_table(&pool, table_name, None)
        .await
        .unwrap();
    (pool, container)
}

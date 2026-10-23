use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

use metalog_config::DatabaseConfig;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode},
    MySqlPool,
};
use testcontainers::{
    core::IntoContainerPort,
    runners::AsyncRunner,
    ContainerAsync,
    GenericImage,
    ImageExt,
};
use testcontainers_modules::mariadb::Mariadb;

pub const PROXYSQL_IMAGE: &str = "proxysql/proxysql";
pub const PROXYSQL_TAG: &str = "3.0.11";
pub const WRITER_USER: &str = "metalog_writer";
pub const READER_USER: &str = "metalog_reader";
pub const READER_BACKEND_LIMIT: usize = 8;

/// Disposable proxy using the exact deployment policy, with only its database
/// address/schema replaced. Its lifetime must remain inside the database's.
pub struct ProxySqlFixture {
    container: ContainerAsync<GenericImage>,
    writer: DatabaseConfig,
    reader: DatabaseConfig,
    admin_port: u16,
}

impl ProxySqlFixture {
    pub async fn launch(database: &ContainerAsync<Mariadb>, admin: &MySqlPool) -> Self {
        // Use the deployment's exact grants and development credentials.
        let bootstrap =
            include_str!("../../../docker/proxysql-users.sql").replace("metalog_metastore", "test");
        metalog_schema::execute_ddl_statements(admin, &bootstrap)
            .await
            .unwrap();

        let backend = database.get_bridge_ip_address().await.unwrap();
        let template = include_str!("../../../docker/proxysql.cnf");
        assert!(
            template.contains("address=\"mariadb\""),
            "deployment template backend placeholder changed"
        );
        let config = template
            .replace("address=\"mariadb\"", &format!("address=\"{backend}\""))
            .replace("metalog_metastore", "test");
        // Explicit host bindings survive Docker stop/start. Automatically
        // assigned bindings can change, invalidating the pools under test.
        let reservations: Vec<_> = (0..3)
            .map(|_| TcpListener::bind(("127.0.0.1", 0)).unwrap())
            .collect();
        let ports: Vec<_> = reservations
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();
        let request = GenericImage::new(PROXYSQL_IMAGE, PROXYSQL_TAG)
            .with_mapped_port(ports[0], 6032.tcp())
            .with_mapped_port(ports[1], 6033.tcp())
            .with_mapped_port(ports[2], 6034.tcp())
            .with_copy_to("/etc/proxysql.cnf", config.into_bytes())
            .with_cmd([
                "proxysql",
                "-f",
                "--idle-threads",
                "--initial",
                "-c",
                "/etc/proxysql.cnf",
            ]);
        drop(reservations);
        let container = request.start().await.unwrap();
        let writer = endpoint(
            container.get_host_port_ipv4(6033).await.unwrap(),
            WRITER_USER,
            "metalog_writer_dev",
        );
        let reader = endpoint(
            container.get_host_port_ipv4(6034).await.unwrap(),
            READER_USER,
            "metalog_reader_dev",
        );
        let admin_port = container.get_host_port_ipv4(6032).await.unwrap();
        let fixture = Self {
            container,
            writer,
            reader,
            admin_port,
        };
        fixture.wait_ready().await;
        fixture
    }

    pub fn writer_config(&self) -> DatabaseConfig {
        self.writer.clone()
    }

    pub fn reader_config(&self) -> DatabaseConfig {
        self.reader.clone()
    }

    pub fn container(&self) -> &ContainerAsync<GenericImage> {
        &self.container
    }

    pub async fn admin_pool(&self) -> MySqlPool {
        let options = MySqlConnectOptions::new()
            .host("127.0.0.1")
            .port(self.admin_port)
            .username("metalog_admin")
            .password("metalog_admin_dev")
            // Use text admin queries without MySQL backend session
            // initialization or TLS in this disposable local fixture.
            .ssl_mode(MySqlSslMode::Disabled)
            .pipes_as_concat(false)
            .no_engine_substitution(false)
            .timezone(None)
            .set_names(false);
        // Bound startup retries to establishing this test administration
        // connection; production statements are never replayed here.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match MySqlPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(2))
                .connect_with(options.clone())
                .await
            {
                Ok(pool) => return pool,
                Err(error) => assert!(
                    Instant::now() < deadline,
                    "ProxySQL admin listener did not become ready: {error}"
                ),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn stop(&self) {
        self.container.stop().await.unwrap();
    }

    pub async fn start(&self) {
        self.container.start().await.unwrap();
        self.wait_ready().await;
    }

    pub async fn restart(&self) {
        self.stop().await;
        self.start().await;
    }

    async fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let result = tokio::time::timeout(Duration::from_secs(2), async {
                let pool = metalog_db::new_pool(&self.writer).await?;
                let result = sqlx::query("SELECT 1").execute(&pool).await;
                pool.close().await;
                result
            })
            .await;
            if matches!(result, Ok(Ok(_))) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "ProxySQL {} did not become ready: {result:?}",
                self.container.id()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn endpoint(port: u16, user: &str, password: &str) -> DatabaseConfig {
    DatabaseConfig {
        host: "127.0.0.1".into(),
        port,
        database: "test".into(),
        user: user.into(),
        password: password.into(),
        pool_size: 10,
        pool_min_idle: 1,
    }
}

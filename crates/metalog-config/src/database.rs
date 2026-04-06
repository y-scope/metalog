use serde::Deserialize;

/// Database connection configuration.
///
/// Maps to the `database.primary` / `database.replica` sections in node.yaml.
/// The DSN is constructed at runtime for the MySQL wire protocol driver.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_host")]
    pub host: String,

    pub database: String,

    #[serde(default = "default_user")]
    pub user: String,

    #[serde(default)]
    pub password: String,

    #[serde(default = "default_port")]
    pub port: u16,

    /// Maximum open connections in the pool.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// Minimum idle connections maintained in the pool.
    #[serde(default = "default_pool_min_idle")]
    pub pool_min_idle: u32,
}

fn default_host() -> String {
    "localhost".into()
}

fn default_user() -> String {
    "root".into()
}

fn default_port() -> u16 {
    3306
}

fn default_pool_size() -> u32 {
    5
}

fn default_pool_min_idle() -> u32 {
    2
}

impl DatabaseConfig {
    /// Builds a MySQL DSN for sqlx.
    ///
    /// Format: `mysql://user:password@host:port/database`
    pub fn dsn(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database,
        )
    }
}

/// Contains primary (read-write) and optional replica (read-only) database configs.
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseSection {
    pub primary: DatabaseConfig,
    pub replica: Option<DatabaseConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_format() {
        let cfg = DatabaseConfig {
            host: "db.example.com".into(),
            database: "metalog".into(),
            user: "admin".into(),
            password: "secret".into(),
            port: 3307,
            pool_size: 10,
            pool_min_idle: 3,
        };
        assert_eq!(
            cfg.dsn(),
            "mysql://admin:secret@db.example.com:3307/metalog"
        );
    }

    #[test]
    fn deserialize_defaults() {
        let yaml = "database: metalog_test";
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.pool_size, 5);
        assert_eq!(cfg.pool_min_idle, 2);
        assert_eq!(cfg.user, "root");
    }

    #[test]
    fn deserialize_full() {
        let yaml = r#"
host: db.prod
database: metalog
user: svc_user
password: s3cret
port: 3307
pool_size: 20
pool_min_idle: 5
"#;
        let cfg: DatabaseConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.host, "db.prod");
        assert_eq!(cfg.pool_size, 20);
    }
}

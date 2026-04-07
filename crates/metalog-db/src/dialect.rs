use sqlx::MySqlPool;

/// Detected database type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    MySQL,
    MariaDB,
    Aurora,
}

impl DatabaseType {
    /// Returns true if this is MariaDB (affects UPSERT syntax and DDL).
    pub fn is_mariadb(&self) -> bool {
        matches!(self, Self::MariaDB)
    }
}

/// Detects the database type by querying `VERSION()`.
///
/// - Contains "mariadb" (case-insensitive) -> MariaDB
/// - Contains "aurora" (case-insensitive) -> Aurora
/// - Otherwise -> MySQL
///
/// Also detects GCP Cloud SQL (`Google` in version) and Azure (`-azure` suffix)
/// as MySQL variants since they use standard MySQL syntax.
pub async fn detect_database_type(pool: &MySqlPool) -> Result<(DatabaseType, String), sqlx::Error> {
    let version: String = sqlx::query_scalar("SELECT VERSION()")
        .fetch_one(pool)
        .await?;

    let lower = version.to_lowercase();
    let db_type = if lower.contains("mariadb") {
        DatabaseType::MariaDB
    } else if lower.contains("aurora") {
        DatabaseType::Aurora
    } else {
        DatabaseType::MySQL
    };

    tracing::info!(
        db_type = ?db_type,
        version = %version,
        "detected database type"
    );

    Ok((db_type, version))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_mariadb() {
        assert!(DatabaseType::MariaDB.is_mariadb());
        assert!(!DatabaseType::MySQL.is_mariadb());
        assert!(!DatabaseType::Aurora.is_mariadb());
    }
}

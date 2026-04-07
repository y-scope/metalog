use metalog_timeutil::epoch_nanos;
use sqlx::MySqlPool;

/// CRUD repository for `_kafka_source` table.
pub struct KafkaSourceStore {
    db: MySqlPool,
}

/// A Kafka source configuration row.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct KafkaSourceRow {
    pub table_name: String,
    pub source_name: String,
    pub topic: String,
    pub bootstrap_servers: String,
    pub record_transformer: String,
    pub consumer_group_id: Option<String>,
    pub required_env: Option<String>,
}

impl KafkaSourceStore {
    pub fn new(db: MySqlPool) -> Self {
        Self { db }
    }

    /// Registers a Kafka source (INSERT IGNORE for idempotency).
    /// Returns true if newly created.
    #[allow(clippy::too_many_arguments)]
    pub async fn register(
        &self,
        table_name: &str,
        source_name: &str,
        topic: &str,
        bootstrap_servers: &str,
        record_transformer: &str,
        consumer_group_id: &str,
        required_env: &str,
    ) -> Result<bool, sqlx::Error> {
        let now = epoch_nanos();
        let transformer = if record_transformer.is_empty() {
            "proto"
        } else {
            record_transformer
        };

        let result = sqlx::query(
            "INSERT IGNORE INTO _kafka_source (table_name, source_name, topic, bootstrap_servers, \
             record_transformer, consumer_group_id, required_env, created_at) VALUES (?, ?, ?, ?, \
             ?, ?, ?, ?)",
        )
        .bind(table_name)
        .bind(source_name)
        .bind(topic)
        .bind(bootstrap_servers)
        .bind(transformer)
        .bind(if consumer_group_id.is_empty() {
            None
        } else {
            Some(consumer_group_id)
        })
        .bind(if required_env.is_empty() {
            None
        } else {
            Some(required_env)
        })
        .bind(now)
        .execute(&self.db)
        .await?;

        if result.rows_affected() > 0 {
            // Also create assignment row.
            sqlx::query(
                "INSERT IGNORE INTO _kafka_assignment (table_name, source_name) VALUES (?, ?)",
            )
            .bind(table_name)
            .bind(source_name)
            .execute(&self.db)
            .await?;
        }

        Ok(result.rows_affected() > 0)
    }

    /// Deletes a Kafka source and its assignment.
    pub async fn delete(&self, table_name: &str, source_name: &str) -> Result<(), sqlx::Error> {
        // FK CASCADE handles _kafka_assignment deletion.
        sqlx::query("DELETE FROM _kafka_source WHERE table_name = ? AND source_name = ?")
            .bind(table_name)
            .bind(source_name)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    /// Lists all sources for a table.
    pub async fn list_sources(&self, table_name: &str) -> Result<Vec<KafkaSourceRow>, sqlx::Error> {
        sqlx::query_as::<_, KafkaSourceRow>(
            "SELECT table_name, source_name, topic, bootstrap_servers, record_transformer, \
             consumer_group_id, required_env FROM _kafka_source WHERE table_name = ? ORDER BY \
             source_name",
        )
        .bind(table_name)
        .fetch_all(&self.db)
        .await
    }

    /// Lists all sources across all tables.
    pub async fn list_all_sources(&self) -> Result<Vec<KafkaSourceRow>, sqlx::Error> {
        sqlx::query_as::<_, KafkaSourceRow>(
            "SELECT table_name, source_name, topic, bootstrap_servers, record_transformer, \
             consumer_group_id, required_env FROM _kafka_source ORDER BY table_name, source_name",
        )
        .fetch_all(&self.db)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_store_compiles() {
        assert!(std::mem::size_of::<KafkaSourceStore>() > 0);
    }
}

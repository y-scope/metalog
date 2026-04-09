mod source_store;

use std::sync::Arc;

use metalog_types::processors::KafkaProvider;
pub use source_store::KafkaSourceStore;
use sqlx::MySqlPool;

/// Premium Kafka ingestion module.
///
/// Manages Kafka source registration, consumer lifecycle, and
/// ingestion into the BatchingWriter.
pub struct KafkaModule {
    source_store: Arc<KafkaSourceStore>,
}

impl KafkaModule {
    pub fn new(db: MySqlPool) -> Self {
        Self {
            source_store: Arc::new(KafkaSourceStore::new(db)),
        }
    }

    pub fn source_store(&self) -> &Arc<KafkaSourceStore> {
        &self.source_store
    }

}

#[async_trait::async_trait]
impl KafkaProvider for KafkaModule {
    async fn register_source(
        &self,
        table_name: &str,
        source_name: &str,
        topic: &str,
        bootstrap_servers: &str,
        record_transformer: &str,
        consumer_group_id: &str,
        required_env: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.source_store
            .register(
                table_name,
                source_name,
                topic,
                bootstrap_servers,
                record_transformer,
                consumer_group_id,
                required_env,
            )
            .await
            .map_err(|e| Box::new(e) as _)
    }

    async fn delete_source(
        &self,
        table_name: &str,
        source_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.source_store
            .delete(table_name, source_name)
            .await
            .map_err(|e| Box::new(e) as _)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_module_is_object_safe() {
        fn _assert(_: &dyn KafkaProvider) {}
    }
}

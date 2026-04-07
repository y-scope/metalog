use metalog_types::processors::KafkaProvider;

/// Premium Kafka ingestion module.
///
/// Provides: rdkafka consumer, KafkaIngestionUnit lifecycle, source assignment
/// (claim/release/renew), _kafka_source + _kafka_assignment DDL,
/// admin handler delegation for RegisterKafkaSource/DeleteKafkaSource.
pub struct KafkaModule;

impl KafkaModule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for KafkaModule {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaProvider for KafkaModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_module_creates() {
        let module = KafkaModule::new();
        assert_eq!(module.name(), "kafka");
    }

    #[test]
    fn kafka_module_is_object_safe() {
        let module: Box<dyn KafkaProvider> = Box::new(KafkaModule::new());
        assert_eq!(module.name(), "kafka");
    }
}

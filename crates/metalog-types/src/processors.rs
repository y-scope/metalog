//! Premium provider/processor traits.
//!
//! Defined in the base crate so that community-edition components can hold
//! `Option<Arc<dyn XxxProvider>>` without depending on premium crates.
//! Premium crates implement these traits and are injected at startup.

/// Premium: processes aggregation columns during ingestion and queries.
///
/// Implemented by `metalog-aggs`.
pub trait AggProcessor: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "aggs"
    }
}

/// Premium: processes sketch/bloom filter columns during ingestion and queries.
///
/// Implemented by `metalog-sketches`.
pub trait SketchProcessor: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "sketches"
    }
}

/// Premium: provides Kafka ingestion capabilities.
///
/// Implemented by `metalog-kafka`.
#[async_trait::async_trait]
pub trait KafkaProvider: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "kafka"
    }

    /// Registers a Kafka source for a table (idempotent — returns true if newly created).
    async fn register_source(
        &self,
        table_name: &str,
        source_name: &str,
        topic: &str,
        bootstrap_servers: &str,
        record_transformer: &str,
        consumer_group_id: &str,
        required_env: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

    /// Deletes a Kafka source and its assignment row.
    async fn delete_source(
        &self,
        table_name: &str,
        source_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Premium: provides high-availability coordination.
///
/// Implemented by `metalog-ha`.
pub trait HAProvider: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "ha"
    }
}

/// Premium: provides the consolidation pipeline (planner + workers).
///
/// Implemented by `metalog-consolidation`.
pub trait ConsolidationProvider: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "consolidation"
    }
}

/// Premium: provides retention lifecycle management.
///
/// Implemented by `metalog-retention`.
pub trait RetentionProvider: Send + Sync {
    /// Name for logging.
    fn name(&self) -> &str {
        "retention"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verify traits are object-safe (can be used as dyn Trait).
    fn _assert_object_safe(
        _a: &dyn AggProcessor,
        _s: &dyn SketchProcessor,
        _k: &dyn KafkaProvider,
        _h: &dyn HAProvider,
        _c: &dyn ConsolidationProvider,
        _r: &dyn RetentionProvider,
    ) {
    }

    #[test]
    fn traits_are_object_safe() {
        // Compilation of _assert_object_safe proves object safety.
    }
}

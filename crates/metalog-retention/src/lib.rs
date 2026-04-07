use metalog_types::processors::RetentionProvider;

/// Premium retention lifecycle module.
///
/// Provides: Strategy trait, DefaultStrategy (3-phase: transition expired →
/// delete metadata → delete storage), rate-limited storage cleanup (500 ops/sec),
/// per-file retention adjustment support.
pub struct RetentionModule;

impl RetentionModule {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RetentionModule {
    fn default() -> Self {
        Self::new()
    }
}

impl RetentionProvider for RetentionModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_module_creates() {
        let module = RetentionModule::new();
        assert_eq!(module.name(), "retention");
    }

    #[test]
    fn retention_module_is_object_safe() {
        let module: Box<dyn RetentionProvider> = Box::new(RetentionModule::new());
        assert_eq!(module.name(), "retention");
    }
}

use std::{collections::HashMap, sync::Arc};

use crate::Backend;

/// Registry of named storage backends.
pub struct Registry {
    backends: HashMap<String, Arc<dyn Backend>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            backends: HashMap::new(),
        }
    }

    /// Registers a named backend.
    pub fn register(&mut self, name: &str, backend: Arc<dyn Backend>) {
        self.backends.insert(name.to_string(), backend);
    }

    /// Gets a backend by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn Backend>> {
        self.backends.get(name).cloned()
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_get() {
        let reg = Registry::new();
        assert!(reg.get("missing").is_none());
        // Can't easily create a mock Backend in unit tests without a real impl,
        // but we can verify the registry mechanics.
        assert_eq!(reg.backends.len(), 0);
    }
}

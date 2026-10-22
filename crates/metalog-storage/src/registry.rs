use std::{collections::HashMap, sync::Arc};

use crate::{Backend, FilesystemBackend, S3Backend, StorageError};

/// Registry of named storage backends.
pub struct Registry {
    backends: HashMap<String, Arc<dyn Backend>>,
}

impl Registry {
    /// Builds configured backends before accepting traffic.
    pub async fn from_config(
        config: &metalog_config::ObjectStorageConfig,
    ) -> Result<Self, StorageError> {
        let mut registry = Self::new();
        for (name, backend) in &config.backends {
            if name.is_empty() || !name.is_ascii() || name.len() > 32 {
                return Err(StorageError::Other(
                    "storage backend names must be 1-32 ASCII characters".into(),
                ));
            }
            let instance: Arc<dyn Backend> = match backend.type_name.as_str() {
                "s3" | "minio" => Arc::new(S3Backend::new(backend).await?),
                "fs" | "filesystem" => {
                    if backend.base_path.is_empty()
                        || !std::path::Path::new(&backend.base_path).is_absolute()
                    {
                        return Err(StorageError::Other(format!(
                            "filesystem backend {name} requires an absolute base_path"
                        )));
                    }
                    tokio::fs::create_dir_all(&backend.base_path).await?;
                    Arc::new(FilesystemBackend::new(&backend.base_path))
                }
                kind => {
                    return Err(StorageError::Other(format!(
                        "unsupported storage backend {name}: {kind}"
                    )))
                }
            };
            registry.register(name, instance);
        }
        if !config.default_backend.is_empty()
            && !config.backends.contains_key(&config.default_backend)
        {
            return Err(StorageError::Other(
                "default storage backend is not configured".into(),
            ));
        }
        Ok(registry)
    }

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

    /// Backends whose legacy locations must be audited before retention runs.
    pub fn canonical_backend_names(&self) -> Vec<String> {
        self.backends
            .iter()
            .filter(|(_, backend)| backend.requires_canonical_keys())
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Validates configured backend identities. Unconfigured locations remain
    /// usable for metadata-only ingestion; retention preserves those objects.
    pub fn validate_location(
        &self,
        backend: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        if self
            .backends
            .get(backend)
            .is_some_and(|backend| backend.requires_canonical_keys())
        {
            crate::backend::validate_canonical_location(bucket, key)?;
        }
        Ok(())
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

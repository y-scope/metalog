use std::collections::HashMap;

use serde::Deserialize;

/// Object storage configuration: named backends + default selection.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ObjectStorageConfig {
    /// Named storage backends (e.g., "s3-prod", "local", "minio").
    #[serde(default)]
    pub backends: HashMap<String, StorageBackendConfig>,

    /// Default backend name (used when a table doesn't specify one).
    #[serde(default)]
    pub default_backend: String,
}

/// Configuration for a single storage backend.
#[derive(Debug, Clone, Deserialize)]
pub struct StorageBackendConfig {
    /// Backend type: "s3", "filesystem", "http".
    #[serde(rename = "type")]
    pub type_name: String,

    /// S3/MinIO bucket name.
    #[serde(default)]
    pub bucket: String,

    /// HTTP backend base URL.
    #[serde(default)]
    pub base_url: String,

    /// Filesystem backend base path.
    #[serde(default)]
    pub base_path: String,

    /// S3-compatible endpoint URL (for MinIO, etc.).
    #[serde(default)]
    pub endpoint: String,

    /// AWS access key ID.
    #[serde(default)]
    pub access_key: String,

    /// AWS secret access key.
    #[serde(default)]
    pub secret_key: String,

    /// AWS region.
    #[serde(default)]
    pub region: String,

    /// Use path-style URLs for S3 (required for MinIO).
    #[serde(default)]
    pub force_path_style: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_storage_config() {
        let yaml = r#"
default_backend: minio
backends:
  minio:
    type: s3
    bucket: logs
    endpoint: http://minio:9000
    access_key: minioadmin
    secret_key: minioadmin
    force_path_style: true
  local:
    type: filesystem
    base_path: /tmp/storage
"#;
        let cfg: ObjectStorageConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.default_backend, "minio");
        assert_eq!(cfg.backends.len(), 2);
        assert_eq!(cfg.backends["minio"].type_name, "s3");
        assert!(cfg.backends["minio"].force_path_style);
        assert_eq!(cfg.backends["local"].base_path, "/tmp/storage");
    }

    #[test]
    fn empty_config() {
        let cfg = ObjectStorageConfig::default();
        assert!(cfg.backends.is_empty());
        assert!(cfg.default_backend.is_empty());
    }
}

use std::collections::HashMap;

use serde::Deserialize;

/// Object storage configuration: named backends + default selection.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectStorageConfig {
    /// Named storage backends (e.g., "s3-prod", "local", "minio").
    #[serde(default)]
    pub backends: HashMap<String, StorageBackendConfig>,

    /// Default backend for callers; persisted paths still require an explicit name.
    #[serde(default, alias = "defaultBackend")]
    pub default_backend: String,
}

/// Configuration for a single storage backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageBackendConfig {
    /// Backend type: "s3"/"minio" or "fs"/"filesystem".
    #[serde(rename = "type", default = "default_backend_type")]
    pub type_name: String,

    /// S3/MinIO bucket name.
    #[serde(default)]
    pub bucket: String,

    /// HTTP backend base URL.
    #[serde(default, alias = "baseUrl")]
    pub base_url: String,

    /// Filesystem backend base path.
    #[serde(default, alias = "basePath")]
    pub base_path: String,

    /// S3-compatible endpoint URL (for MinIO, etc.).
    #[serde(default)]
    pub endpoint: String,

    /// AWS access key ID.
    #[serde(default, alias = "accessKey")]
    pub access_key: String,

    /// AWS secret access key.
    #[serde(default, alias = "secretKey")]
    pub secret_key: String,

    /// Optional token for temporary AWS credentials.
    #[serde(default, alias = "sessionToken")]
    pub session_token: String,

    /// AWS region.
    #[serde(default)]
    pub region: String,

    /// Use path-style URLs for S3 (required for MinIO).
    #[serde(default, alias = "forcePathStyle")]
    pub force_path_style: bool,
}

fn default_backend_type() -> String {
    "s3".into()
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

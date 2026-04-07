/// Storage operation errors.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("access denied: {bucket}/{key}")]
    AccessDenied { bucket: String, key: String },

    #[error("storage io: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage: {0}")]
    Other(String),
}

impl StorageError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. })
    }
}

/// Contextual operation error (wraps operation name + bucket + key).
#[derive(Debug, thiserror::Error)]
#[error("{op} {bucket}/{key}: {source}")]
pub struct OpError {
    pub op: String,
    pub bucket: String,
    pub key: String,
    #[source]
    pub source: StorageError,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_display() {
        let err = StorageError::NotFound {
            bucket: "logs".into(),
            key: "/data/test.ir".into(),
        };
        assert!(err.to_string().contains("logs"));
        assert!(err.is_not_found());
    }

    #[test]
    fn op_error_display() {
        let err = OpError {
            op: "GET".into(),
            bucket: "logs".into(),
            key: "test.ir".into(),
            source: StorageError::NotFound {
                bucket: "logs".into(),
                key: "test.ir".into(),
            },
        };
        assert!(err.to_string().contains("GET"));
    }
}

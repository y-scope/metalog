use std::sync::Arc;

use metalog_metastore::validate_file_record;
use metalog_types::FileRecord;

use crate::batching_writer::BatchingWriter;

/// Validates and routes file records to the [`BatchingWriter`].
pub struct IngestionService {
    writer: Arc<BatchingWriter>,
    blocking: bool,
}

/// Outcome of an ingestion attempt.
#[derive(Debug)]
pub struct IngestionResult {
    pub accepted: bool,
    pub error: Option<String>,
}

impl IngestionService {
    pub fn new(writer: Arc<BatchingWriter>, blocking: bool) -> Self {
        Self { writer, blocking }
    }

    /// Ingests a record. Validates, then submits to the writer.
    ///
    /// In blocking mode, waits for channel space. In non-blocking mode,
    /// returns immediately with an error if the channel is full.
    pub async fn ingest(&self, table_name: &str, rec: FileRecord) -> IngestionResult {
        if let Err(e) = validate_file_record(&rec) {
            return IngestionResult {
                accepted: false,
                error: Some(e.to_string()),
            };
        }

        let result = if self.blocking {
            self.writer.submit_wait(table_name, rec).await
        } else {
            self.writer.submit(table_name, rec).await
        };

        match result {
            Ok(()) => IngestionResult {
                accepted: true,
                error: None,
            },
            Err(e) => IngestionResult {
                accepted: false,
                error: Some(e.to_string()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingestion_result_accepted() {
        let r = IngestionResult {
            accepted: true,
            error: None,
        };
        assert!(r.accepted);
    }

    #[test]
    fn ingestion_result_rejected() {
        let r = IngestionResult {
            accepted: false,
            error: Some("bad record".into()),
        };
        assert!(!r.accepted);
        assert!(r.error.unwrap().contains("bad"));
    }
}

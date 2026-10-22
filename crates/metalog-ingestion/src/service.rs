use std::sync::Arc;

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

/// A single record failure within a batch.
#[derive(Debug)]
pub struct RecordFailure {
    pub index: i32,
    pub error: String,
}

/// Outcome of a batch ingestion attempt.
#[derive(Debug)]
pub struct BatchIngestionResult {
    pub accepted_count: i32,
    pub rejected_count: i32,
    pub failures: Vec<RecordFailure>,
}

impl IngestionService {
    pub fn new(writer: Arc<BatchingWriter>, blocking: bool) -> Self {
        Self { writer, blocking }
    }

    /// Ingests a record and acknowledges only after its database statement commits.
    /// Non-blocking mode rejects a full queue immediately; accepted submissions in
    /// either mode wait for their durability receipt.
    pub async fn ingest(&self, table_name: &str, rec: FileRecord) -> IngestionResult {
        let result = match self
            .writer
            .submit_confirmed(table_name, rec, self.blocking)
            .await
        {
            Ok(receipt) => receipt.wait().await,
            Err(error) => Err(error),
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

    /// Enqueues all valid records with backpressure, then waits for their commits.
    /// Per-record failures preserve the input indices, including failures during flush.
    pub async fn batch_ingest(
        &self,
        table_name: &str,
        records: Vec<FileRecord>,
    ) -> BatchIngestionResult {
        let total = records.len() as i32;
        let mut failures = Vec::new();
        let mut receipts = Vec::new();
        for (i, rec) in records.into_iter().enumerate() {
            match self.writer.submit_confirmed(table_name, rec, true).await {
                Ok(receipt) => receipts.push((i as i32, receipt)),
                Err(error) => failures.push(RecordFailure {
                    index: i as i32,
                    error: error.to_string(),
                }),
            }
        }
        for (index, receipt) in receipts {
            if let Err(error) = receipt.wait().await {
                failures.push(RecordFailure {
                    index,
                    error: error.to_string(),
                });
            }
        }
        failures.sort_by_key(|failure| failure.index);

        let rejected_count = failures.len() as i32;
        BatchIngestionResult {
            accepted_count: total - rejected_count,
            rejected_count,
            failures,
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

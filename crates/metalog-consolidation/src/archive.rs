use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use metalog_storage::Registry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{ConsolidationPayload, TaskResult};

// Bound local scratch use and stay within S3's single-PutObject limit.
pub const MAX_INPUT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_ARCHIVE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("invalid consolidation task: {0}")]
    Invalid(String),
    #[error(transparent)]
    Storage(#[from] metalog_storage::StorageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("archive operation exceeded {0:?}")]
    Timeout(Duration),
    #[error("clp-s failed: {0}")]
    Compression(String),
}

impl ConsolidationPayload {
    pub fn validate(&self) -> Result<(), ArchiveError> {
        if self.ir_paths.is_empty()
            || self.ir_paths.len() > 10_000
            || !(self.ir_buckets.len() == 1 || self.ir_buckets.len() == self.ir_paths.len())
            || self.file_ids.len() != self.ir_paths.len()
            || self.ir_paths.iter().any(String::is_empty)
            || self.ir_buckets.iter().any(String::is_empty)
            || self.ir_backend.is_empty()
            || self.archive_backend.is_empty()
            || self.archive_bucket.is_empty()
            || self.archive_path.is_empty()
        {
            return Err(ArchiveError::Invalid(
                "expected matching nonempty paths, buckets and file IDs, and explicit storage \
                 identities"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn bucket(&self, index: usize) -> &str {
        &self.ir_buckets[if self.ir_buckets.len() == 1 { 0 } else { index }]
    }

    /// A distinct object per attempt prevents a stale worker overwriting a winner.
    pub fn attempt_path(&self, task_id: i64) -> String {
        format!("{}.task-{task_id}.clp", self.archive_path)
    }
}

pub struct ArchiveCreator {
    storage: Arc<Registry>,
    binary: PathBuf,
    timeout: Duration,
}

impl ArchiveCreator {
    pub fn new(storage: Arc<Registry>, binary: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            storage,
            binary: binary.into(),
            timeout,
        }
    }

    pub async fn create(
        &self,
        task_id: i64,
        input: &ConsolidationPayload,
    ) -> Result<TaskResult, ArchiveError> {
        tokio::time::timeout(self.timeout, self.create_inner(task_id, input))
            .await
            .map_err(|_| ArchiveError::Timeout(self.timeout))?
    }

    async fn create_inner(
        &self,
        task_id: i64,
        input: &ConsolidationPayload,
    ) -> Result<TaskResult, ArchiveError> {
        input.validate()?;
        let source = self.storage.get(&input.ir_backend).ok_or_else(|| {
            ArchiveError::Invalid(format!("unknown input backend {}", input.ir_backend))
        })?;
        let destination = self.storage.get(&input.archive_backend).ok_or_else(|| {
            ArchiveError::Invalid(format!("unknown archive backend {}", input.archive_backend))
        })?;
        let archive_path = input.attempt_path(task_id);
        self.storage.validate_location(
            &input.archive_backend,
            &input.archive_bucket,
            &archive_path,
        )?;
        let temp = tempfile::Builder::new()
            .prefix("metalog-archive-")
            .tempdir()?;
        let input_dir = temp.path().join("input");
        let output_dir = temp.path().join("output");
        tokio::fs::create_dir(&input_dir).await?;
        tokio::fs::create_dir(&output_dir).await?;
        let mut total = 0;
        let mut seen = std::collections::HashSet::new();
        for (index, path) in input.ir_paths.iter().enumerate() {
            let bucket = input.bucket(index);
            if !seen.insert((bucket, path)) {
                continue;
            }
            self.storage
                .validate_location(&input.ir_backend, bucket, path)?;
            let basename = path
                .rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .ok_or_else(|| ArchiveError::Invalid("missing input filename".into()))?;
            // S3 keys can have components longer than a filesystem's NAME_MAX.
            // Keep the suffix/extension and use the ordinal for uniqueness.
            let mut start = basename.len().saturating_sub(240);
            while !basename.is_char_boundary(start) {
                start += 1;
            }
            let basename = &basename[start..];
            let reader = source.get(bucket, path).await?;
            let mut reader = reader.take(MAX_INPUT_BYTES - total + 1);
            let mut output =
                tokio::fs::File::create(input_dir.join(format!("{index:04}_{basename}"))).await?;
            total += tokio::io::copy(&mut reader, &mut output).await?;
            if total > MAX_INPUT_BYTES {
                return Err(ArchiveError::Invalid(
                    "input group exceeds 8 GiB scratch limit; reduce policy max_files".into(),
                ));
            }
            output.flush().await?;
        }

        let mut command = tokio::process::Command::new(&self.binary);
        command
            .args(["c", "--single-file-archive", "--remove-path-prefix"])
            .arg(&input_dir)
            .args(["--remove-leading-slash", "--normalize-paths"])
            .arg(&output_dir)
            .arg(&input_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let mut stderr = child.stderr.take().expect("stderr is piped");
        // Drain diagnostics without allowing a noisy subprocess to consume memory.
        let read_stderr = async {
            let mut diagnostic = Vec::new();
            let mut buffer = [0u8; 4096];
            loop {
                let n = stderr.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                let keep = n.min(65536usize.saturating_sub(diagnostic.len()));
                diagnostic.extend_from_slice(&buffer[..keep]);
            }
            Ok::<_, std::io::Error>(diagnostic)
        };
        let (status, diagnostic) = tokio::try_join!(child.wait(), read_stderr)?;
        if !status.success() {
            return Err(ArchiveError::Compression(format!(
                "{status}: {}",
                String::from_utf8_lossy(&diagnostic)
            )));
        }
        let mut entries = tokio::fs::read_dir(&output_dir).await?;
        let archive = entries
            .next_entry()
            .await?
            .ok_or_else(|| ArchiveError::Compression("no archive produced".into()))?;
        if !archive.file_type().await?.is_file() || entries.next_entry().await?.is_some() {
            return Err(ArchiveError::Compression(
                "expected exactly one archive file".into(),
            ));
        }
        let size = archive.metadata().await?.len();
        if size == 0 || size > MAX_ARCHIVE_BYTES {
            return Err(ArchiveError::Compression(
                "archive must be nonempty and at most 5 GiB".into(),
            ));
        }
        destination
            .put_file(&input.archive_bucket, &archive_path, &archive.path())
            .await?;
        Ok(TaskResult {
            archive_path,
            archive_size_bytes: size as i64,
            error: String::new(),
            created_at: metalog_timeutil::epoch_nanos(),
        })
    }
}

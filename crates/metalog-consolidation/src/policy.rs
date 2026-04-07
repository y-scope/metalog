use std::time::Duration;

use metalog_types::FileRecord;

/// A group of files to consolidate into a single archive.
#[derive(Debug)]
pub struct FileGroup {
    pub records: Vec<FileRecord>,
    pub archive_path: String,
    pub archive_backend: String,
    pub archive_bucket: String,
}

/// Consolidation policy: decides how to group files for consolidation.
pub trait Policy: Send + Sync {
    /// Groups candidate files into consolidation groups.
    fn select_files(&self, candidates: &[FileRecord]) -> Vec<FileGroup>;

    /// Logical dimension keys needed by this policy.
    fn required_dims(&self) -> Vec<String> {
        vec![]
    }
}

/// Groups files by time window boundaries.
pub struct TimeWindowPolicy {
    window_nanos: i64,
    min_files: usize,
    max_files: usize,
}

impl TimeWindowPolicy {
    pub fn new(window_size: Duration, min_files: usize, max_files: usize) -> Self {
        Self {
            window_nanos: window_size.as_nanos() as i64,
            min_files,
            max_files,
        }
    }
}

impl Policy for TimeWindowPolicy {
    fn select_files(&self, candidates: &[FileRecord]) -> Vec<FileGroup> {
        use std::collections::HashMap;

        if candidates.is_empty() {
            return vec![];
        }

        // Bucket by time window.
        let mut buckets: HashMap<i64, Vec<&FileRecord>> = HashMap::new();
        for rec in candidates {
            let bucket_key = rec.min_timestamp / self.window_nanos;
            buckets.entry(bucket_key).or_default().push(rec);
        }

        let mut groups = Vec::new();
        for (_, bucket) in buckets {
            if bucket.len() < self.min_files {
                continue;
            }
            for chunk in bucket.chunks(self.max_files) {
                if chunk.len() >= self.min_files {
                    groups.push(FileGroup {
                        records: chunk.iter().map(|r| (*r).clone()).collect(),
                        archive_path: generate_archive_path(),
                        archive_backend: String::new(),
                        archive_bucket: String::new(),
                    });
                }
            }
        }
        groups
    }
}

/// Generates a UUIDv4-based archive path.
fn generate_archive_path() -> String {
    // Placeholder — real impl would use uuid v7 for time-sortable paths.
    format!("archives/{}", uuid_v4_placeholder())
}

fn uuid_v4_placeholder() -> String {
    use std::time::SystemTime;
    let t = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{t:032x}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metalog_types::file_state::FileState;

    use super::*;

    fn test_record(min_ts: i64) -> FileRecord {
        FileRecord {
            min_timestamp: min_ts,
            max_timestamp: min_ts + 1000,
            file_path: Some(format!("/data/{min_ts}.ir")),
            state: FileState::IrArchiveConsolidationPending,
            ..FileRecord::default()
        }
    }

    #[test]
    fn time_window_groups() {
        let window = Duration::from_secs(3600); // 1 hour
        let policy = TimeWindowPolicy::new(window, 2, 100);

        let hour_nanos = 3_600_000_000_000i64;
        let candidates: Vec<FileRecord> = (0..5)
            .map(|i| test_record(i * (hour_nanos / 10))) // All in same hour
            .collect();

        let groups = policy.select_files(&candidates);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].records.len(), 5);
    }

    #[test]
    fn time_window_min_files() {
        let policy = TimeWindowPolicy::new(Duration::from_secs(3600), 3, 100);
        let candidates = vec![test_record(1000), test_record(2000)]; // Only 2
        let groups = policy.select_files(&candidates);
        assert!(groups.is_empty()); // Below min_files
    }

    #[test]
    fn time_window_max_files_splits() {
        let policy = TimeWindowPolicy::new(Duration::from_secs(3600), 1, 3);
        let candidates: Vec<FileRecord> = (0..7).map(|i| test_record(i * 100)).collect();
        let groups = policy.select_files(&candidates);
        // 7 files / max 3 = at least 2 groups (7/3=2 full + 1 remainder of 1)
        assert!(groups.len() >= 2);
    }

    #[test]
    fn empty_candidates() {
        let policy = TimeWindowPolicy::new(Duration::from_secs(3600), 1, 100);
        assert!(policy.select_files(&[]).is_empty());
    }

    #[test]
    fn required_dims_empty() {
        let policy = TimeWindowPolicy::new(Duration::from_secs(3600), 1, 100);
        assert!(policy.required_dims().is_empty());
    }
}

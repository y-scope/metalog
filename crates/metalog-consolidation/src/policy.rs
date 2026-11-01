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
            window_nanos: window_size.as_nanos().clamp(1, i64::MAX as u128) as i64,
            min_files: min_files.max(1),
            max_files: max_files.max(1),
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

fn generate_archive_path() -> String {
    format!("archives/{}", uuid::Uuid::new_v4())
}

pub struct SparkJobPolicy {
    key: String,
    min_files: usize,
    max_files: usize,
    timeout: Duration,
}

impl SparkJobPolicy {
    pub fn new(key: String, min_files: usize, max_files: usize, timeout: Duration) -> Self {
        Self {
            key,
            min_files: min_files.max(1),
            max_files: max_files.max(1),
            timeout,
        }
    }
}

impl Policy for SparkJobPolicy {
    fn required_dims(&self) -> Vec<String> {
        vec![self.key.clone()]
    }

    fn select_files(&self, candidates: &[FileRecord]) -> Vec<FileGroup> {
        let mut buckets = std::collections::BTreeMap::<Option<&str>, Vec<&FileRecord>>::new();
        for record in candidates {
            let key = record
                .dims
                .get(&self.key)
                .and_then(|v| v.as_str())
                .filter(|v| !v.is_empty());
            buckets.entry(key).or_default().push(record);
        }
        let now = metalog_timeutil::epoch_nanos();
        let mut groups = Vec::new();
        for bucket in buckets.into_values() {
            let timed_out = !self.timeout.is_zero()
                && bucket.iter().any(|r| {
                    r.max_timestamp > 0
                        && now.saturating_sub(r.max_timestamp) as i128
                            >= self.timeout.as_nanos() as i128
                });
            for chunk in bucket.chunks(self.max_files) {
                if timed_out || chunk.len() >= self.min_files {
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

pub struct PolicyChain(pub Vec<std::sync::Arc<dyn Policy>>);
impl Policy for PolicyChain {
    fn required_dims(&self) -> Vec<String> {
        self.0
            .iter()
            .flat_map(|p| p.required_dims())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn select_files(&self, candidates: &[FileRecord]) -> Vec<FileGroup> {
        let mut remaining = candidates.to_vec();
        let mut result = Vec::new();
        for policy in &self.0 {
            let groups = policy.select_files(&remaining);
            let used: std::collections::HashSet<_> = groups
                .iter()
                .flat_map(|g| g.records.iter().map(|r| (r.min_timestamp, r.id)))
                .collect();
            remaining.retain(|r| !used.contains(&(r.min_timestamp, r.id)));
            result.extend(groups);
        }
        result
    }
}

#[derive(serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct PolicyOptions {
    window_size: Option<String>,
    grouping_dim_key: Option<String>,
    job_timeout: Option<String>,
    min_files: Option<usize>,
    max_files: Option<usize>,
}

/// Go-compatible defaults and waterfall policy order, with invalid configuration
/// rejected explicitly instead of silently replaced with defaults.
pub fn build_policy(
    config: &metalog_types::config::ConsolidationConfig,
) -> Result<std::sync::Arc<dyn Policy>, String> {
    use std::sync::Arc;
    if config.stale_buffering_mins > i64::MAX / 60_000_000_000 {
        return Err("stale_buffering_mins exceeds nanosecond range".into());
    }
    let duration = |value: Option<String>, default| -> Result<Duration, String> {
        let duration = match value {
            Some(v) => humantime::parse_duration(&v).map_err(|e| e.to_string())?,
            None => Duration::from_secs(default),
        };
        if duration.is_zero() || duration.as_nanos() > i64::MAX as u128 {
            return Err("policy duration must be positive and fit in nanoseconds".into());
        }
        Ok(duration)
    };
    let mut policies: Vec<Arc<dyn Policy>> = Vec::new();
    for policy in &config.policies {
        let options: PolicyOptions = if policy.config.is_null() {
            PolicyOptions::default()
        } else {
            serde_json::from_value(policy.config.clone()).map_err(|e| e.to_string())?
        };
        let min = options.min_files.unwrap_or(2);
        let max = options.max_files.unwrap_or(100);
        if min == 0 || min > max || max > 10_000 {
            return Err("policy requires 1 <= min_files <= max_files <= 10000".into());
        }
        match policy.type_name.as_str() {
            "" | "time_window" => {
                if options.grouping_dim_key.is_some() || options.job_timeout.is_some() {
                    return Err("time_window does not accept spark_job options".into());
                }
                policies.push(Arc::new(TimeWindowPolicy::new(
                    duration(options.window_size, 3600)?,
                    min,
                    max,
                )));
            }
            "spark_job" => {
                if options.window_size.is_some() {
                    return Err("spark_job does not accept window_size".into());
                }
                let key = options
                    .grouping_dim_key
                    .filter(|key| {
                        !key.is_empty() && key.chars().count() <= 1024 && !key.contains('\0')
                    })
                    .ok_or("spark_job requires grouping_dim_key")?;
                policies.push(Arc::new(SparkJobPolicy::new(
                    key,
                    min,
                    max,
                    duration(options.job_timeout, 86400)?,
                )));
            }
            name => return Err(format!("unknown consolidation policy: {name}")),
        }
    }
    if policies.is_empty() {
        policies.push(Arc::new(TimeWindowPolicy::new(
            Duration::from_secs(3600),
            2,
            100,
        )));
    }
    Ok(Arc::new(PolicyChain(policies)))
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
    fn spark_timeout_and_waterfall_preserve_remaining_records() {
        let now = metalog_timeutil::epoch_nanos();
        let mut records: Vec<_> = (0..5)
            .map(|id| FileRecord {
                id,
                min_timestamp: now + id,
                max_timestamp: now,
                ..test_record(now + id)
            })
            .collect();
        records[0].dims.insert("job".into(), serde_json::json!("a"));
        records[1].dims.insert("job".into(), serde_json::json!("a"));
        records[2]
            .dims
            .insert("job".into(), serde_json::json!("old"));
        records[2].max_timestamp = now - 100_000_000_000;
        records[3]
            .dims
            .insert("job".into(), serde_json::json!("future"));
        records[3].max_timestamp = now + 100_000_000_000;
        records[4].max_timestamp = 0;
        let spark = SparkJobPolicy::new("job".into(), 2, 2, Duration::from_secs(60));
        let groups = spark.select_files(&records);
        assert_eq!(groups.iter().map(|g| g.records.len()).sum::<usize>(), 3);
        let chain = PolicyChain(vec![
            std::sync::Arc::new(spark),
            std::sync::Arc::new(TimeWindowPolicy::new(Duration::from_secs(86400), 1, 100)),
        ]);
        let groups = chain.select_files(&records);
        let ids: Vec<_> = groups
            .iter()
            .flat_map(|g| g.records.iter().map(|r| r.id))
            .collect();
        assert_eq!(ids.len(), 5);
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            5
        );
        assert_eq!(chain.required_dims(), ["job"]);
    }

    #[test]
    fn malformed_policy_configuration_is_rejected() {
        for policy in [
            serde_json::json!({"type":"unknown"}),
            serde_json::json!({"type":"spark_job"}),
            serde_json::json!({"type":"time_window","config":{"window_size":"0s"}}),
            serde_json::json!({"type":"time_window","config":{"min_files":0}}),
            serde_json::json!({"type":"time_window","config":{"min_files":3,"max_files":2}}),
            serde_json::json!({"type":"time_window","config":{"window_size":"oops"}}),
            serde_json::json!({"type":"time_window","config":{"typo":1}}),
        ] {
            let config = serde_json::from_value(serde_json::json!({"policies":[policy]})).unwrap();
            assert!(build_policy(&config).is_err());
        }
        assert!(build_policy(&Default::default()).is_ok());
    }

    #[test]
    fn required_dims_empty() {
        let policy = TimeWindowPolicy::new(Duration::from_secs(3600), 1, 100);
        assert!(policy.required_dims().is_empty());
    }
}

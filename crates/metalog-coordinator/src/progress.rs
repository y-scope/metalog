use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::Duration,
};

/// Detects stalled coordinator loops by tracking last-progress timestamps.
///
/// Uses [`AtomicI64`] for lock-free timestamp storage. Thread-safe by design.
pub struct ProgressTracker {
    last_progress_nanos: AtomicI64,
    stall_timeout: Duration,
}

impl ProgressTracker {
    /// Creates a new tracker with the given stall timeout.
    /// Records initial progress at creation time.
    pub fn new(stall_timeout: Duration) -> Self {
        let tracker = Self {
            last_progress_nanos: AtomicI64::new(0),
            stall_timeout,
        };
        tracker.record_progress();
        tracker
    }

    /// Records progress at the current time.
    pub fn record_progress(&self) {
        let now = metalog_timeutil::epoch_nanos();
        self.last_progress_nanos.store(now, Ordering::Relaxed);
    }

    /// Returns true if no progress has been recorded within the stall timeout.
    pub fn is_stalled(&self) -> bool {
        let last = self.last_progress_nanos.load(Ordering::Relaxed);
        let now = metalog_timeutil::epoch_nanos();
        let elapsed_nanos = now - last;
        elapsed_nanos > self.stall_timeout.as_nanos() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_stalled_initially() {
        let tracker = ProgressTracker::new(Duration::from_secs(300));
        assert!(!tracker.is_stalled());
    }

    #[test]
    fn stalled_after_timeout() {
        let tracker = ProgressTracker::new(Duration::from_nanos(1));
        // Set last progress to a very old time.
        tracker.last_progress_nanos.store(0, Ordering::Relaxed);
        assert!(tracker.is_stalled());
    }

    #[test]
    fn record_progress_resets_stall() {
        let tracker = ProgressTracker::new(Duration::from_secs(300));
        // Simulate stall by setting last progress to epoch 0.
        tracker.last_progress_nanos.store(0, Ordering::Relaxed);
        assert!(tracker.is_stalled());

        // Recording progress should clear the stall.
        tracker.record_progress();
        assert!(!tracker.is_stalled());
    }
}

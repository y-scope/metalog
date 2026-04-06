use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

/// Throttles repeated failure logs in periodic loops.
///
/// On first failure: logs immediately at `warn` level. On subsequent failures
/// within the interval: suppressed. On recovery: logs once at `info` level.
///
/// Thread-safe via [`Mutex`].
pub struct FailureLogger {
    inner: Mutex<FailureState>,
    interval: Duration,
}

struct FailureState {
    last_log: Option<Instant>,
    failing: bool,
}

impl FailureLogger {
    /// Creates a new `FailureLogger` that throttles repeated failures to at most
    /// one log per `interval`.
    pub fn new(interval: Duration) -> Self {
        Self {
            inner: Mutex::new(FailureState {
                last_log: None,
                failing: false,
            }),
            interval,
        }
    }

    /// Records a failure. Logs at warn level on first failure, then at most once
    /// per interval while failures continue.
    pub fn fail(&self, msg: &str) {
        let mut state = self.inner.lock().unwrap();
        let now = Instant::now();

        if !state.failing {
            // First failure: log immediately.
            state.failing = true;
            state.last_log = Some(now);
            tracing::warn!("{msg}");
            return;
        }

        // Subsequent failure: throttle.
        let should_log = state
            .last_log
            .is_none_or(|last| now.duration_since(last) >= self.interval);
        if should_log {
            state.last_log = Some(now);
            tracing::warn!("{msg}");
        }
    }

    /// Records a recovery. If previously failing, logs once at info level.
    pub fn ok(&self) {
        let mut state = self.inner.lock().unwrap();
        if state.failing {
            state.failing = false;
            state.last_log = None;
            tracing::info!("recovered from failure");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn first_failure_sets_flag() {
        let fl = FailureLogger::new(Duration::from_secs(60));
        fl.fail("test error");
        let state = fl.inner.lock().unwrap();
        assert!(state.failing);
        assert!(state.last_log.is_some());
    }

    #[test]
    fn ok_clears_flag() {
        let fl = FailureLogger::new(Duration::from_secs(60));
        fl.fail("test error");
        fl.ok();
        let state = fl.inner.lock().unwrap();
        assert!(!state.failing);
        assert!(state.last_log.is_none());
    }

    #[test]
    fn ok_without_failure_is_noop() {
        let fl = FailureLogger::new(Duration::from_secs(60));
        fl.ok();
        let state = fl.inner.lock().unwrap();
        assert!(!state.failing);
    }

    #[test]
    fn repeated_failures_within_interval() {
        let fl = FailureLogger::new(Duration::from_secs(60));
        fl.fail("error 1");
        let first_log = fl.inner.lock().unwrap().last_log;
        fl.fail("error 2");
        let second_log = fl.inner.lock().unwrap().last_log;
        // last_log shouldn't change within interval (throttled).
        assert_eq!(first_log, second_log);
    }
}

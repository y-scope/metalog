use std::{collections::HashSet, sync::RwLock};

/// Tracks IR paths being consolidated (process-local, not persisted).
///
/// Prevents duplicate task creation within the planner lifecycle.
/// The database is the source of truth for persistence.
pub struct InFlightSet {
    paths: RwLock<HashSet<String>>,
}

impl InFlightSet {
    pub fn new() -> Self {
        Self {
            paths: RwLock::new(HashSet::new()),
        }
    }

    /// Atomically adds all paths if none are already present.
    /// Returns false (and adds nothing) if any path is already in-flight.
    #[must_use]
    pub fn try_add(&self, paths: &[String]) -> bool {
        let mut set = self.paths.write().unwrap_or_else(|e| e.into_inner());
        for path in paths {
            if set.contains(path) {
                return false;
            }
        }
        for path in paths {
            set.insert(path.clone());
        }
        true
    }

    /// Removes paths from the in-flight set.
    pub fn remove(&self, paths: &[String]) {
        let mut set = self.paths.write().unwrap_or_else(|e| e.into_inner());
        for path in paths {
            set.remove(path);
        }
    }

    /// Returns true if the path is currently in-flight.
    pub fn contains(&self, path: &str) -> bool {
        let set = self.paths.read().unwrap_or_else(|e| e.into_inner());
        set.contains(path)
    }

    /// Returns the number of in-flight paths.
    pub fn size(&self) -> usize {
        let set = self.paths.read().unwrap_or_else(|e| e.into_inner());
        set.len()
    }
}

impl Default for InFlightSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_add_success() {
        let set = InFlightSet::new();
        let paths = vec!["a.ir".into(), "b.ir".into()];
        assert!(set.try_add(&paths));
        assert_eq!(set.size(), 2);
        assert!(set.contains("a.ir"));
    }

    #[test]
    fn try_add_conflict() {
        let set = InFlightSet::new();
        let _ = set.try_add(&["a.ir".into()]);
        // Adding a set that overlaps should fail.
        assert!(!set.try_add(&["a.ir".into(), "c.ir".into()]));
        // Nothing from the failed set should be added.
        assert!(!set.contains("c.ir"));
        assert_eq!(set.size(), 1);
    }

    #[test]
    fn remove() {
        let set = InFlightSet::new();
        let _ = set.try_add(&["a.ir".into(), "b.ir".into()]);
        set.remove(&["a.ir".into()]);
        assert!(!set.contains("a.ir"));
        assert!(set.contains("b.ir"));
        assert_eq!(set.size(), 1);
    }

    #[test]
    fn empty_set() {
        let set = InFlightSet::new();
        assert_eq!(set.size(), 0);
        assert!(!set.contains("anything"));
        // Adding empty slice succeeds.
        assert!(set.try_add(&[]));
    }
}

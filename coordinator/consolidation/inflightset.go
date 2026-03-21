package consolidation

import "sync"

// InFlightSet tracks IR paths that are currently being consolidated,
// preventing the planner from creating duplicate tasks within a single
// planner lifecycle.
//
// By design, InFlightSet is process-local and starts empty on every restart.
// It is NOT persisted to the database. The database is the source of truth
// for task state — if the planner restarts, it may create duplicate tasks for
// files that already have pending tasks, but this is harmless: both workers
// produce the same archive output, and the MarkArchiveClosed state guard
// ensures only one succeeds. The cost of occasional duplicate work on restart
// is far lower than the complexity of persisting and synchronizing in-flight
// state across nodes.
type InFlightSet struct {
	mu    sync.RWMutex
	paths map[string]bool
}

// NewInFlightSet creates an InFlightSet.
func NewInFlightSet() *InFlightSet {
	return &InFlightSet{paths: make(map[string]bool)}
}

// TryAdd adds paths to the set if none are already present.
// Returns true if all were added, false if any were already in-flight.
func (s *InFlightSet) TryAdd(paths []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range paths {
		if s.paths[p] {
			return false
		}
	}
	for _, p := range paths {
		s.paths[p] = true
	}
	return true
}

// Remove removes paths from the set.
func (s *InFlightSet) Remove(paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range paths {
		delete(s.paths, p)
	}
}

// Contains returns true if the path is in-flight.
func (s *InFlightSet) Contains(path string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paths[path]
}

// Size returns the number of in-flight paths.
func (s *InFlightSet) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.paths)
}

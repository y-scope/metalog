package config

import (
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// FileWatcher polls a YAML file for changes and invokes a callback on modification.
// Uses stat-based polling (no fsnotify dependency) with a configurable interval.
type FileWatcher struct {
	path     string
	interval time.Duration
	log      *zap.Logger

	lastModTime time.Time
	lastSize    int64
}

// NewFileWatcher creates a FileWatcher for the given path.
func NewFileWatcher(path string, interval time.Duration, log *zap.Logger) *FileWatcher {
	return &FileWatcher{
		path:     path,
		interval: interval,
		log:      log.With(zap.String("watchPath", path)),
	}
}

// Watch polls the file until ctx is cancelled. Calls onChange when the file is modified.
// The callback receives the raw YAML bytes.
func (w *FileWatcher) Watch(ctx context.Context, onChange func(data []byte)) {
	// Capture initial state.
	if info, err := os.Stat(w.path); err == nil {
		w.lastModTime = info.ModTime()
		w.lastSize = info.Size()
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.check(onChange)
		}
	}
}

func (w *FileWatcher) check(onChange func(data []byte)) {
	info, err := os.Stat(w.path)
	if err != nil {
		return // file may be temporarily gone during atomic write
	}

	if info.ModTime().Equal(w.lastModTime) && info.Size() == w.lastSize {
		return
	}

	data, err := os.ReadFile(w.path)
	if err != nil {
		w.log.Warn("failed to read changed config", zap.Error(err))
		return
	}

	w.lastModTime = info.ModTime()
	w.lastSize = info.Size()
	w.log.Info("config file changed, reloading")
	onChange(data)
}

// HotReloadableConfig wraps a YAML config that can be atomically swapped via file watch.
type HotReloadableConfig[T any] struct {
	mu      sync.RWMutex
	current T
	log     *zap.Logger
}

// NewHotReloadableConfig creates a hot-reloadable config with an initial value.
func NewHotReloadableConfig[T any](initial T, log *zap.Logger) *HotReloadableConfig[T] {
	return &HotReloadableConfig[T]{current: initial, log: log}
}

// Get returns the current config value.
func (h *HotReloadableConfig[T]) Get() T {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.current
}

// Update atomically swaps the config value.
func (h *HotReloadableConfig[T]) Update(val T) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.current = val
}

// WatchAndReload starts a file watcher that parses YAML into T on change.
// Blocks until ctx is cancelled.
func (h *HotReloadableConfig[T]) WatchAndReload(ctx context.Context, path string, interval time.Duration) {
	watcher := NewFileWatcher(path, interval, h.log)
	watcher.Watch(ctx, func(data []byte) {
		var val T
		if err := yaml.Unmarshal(data, &val); err != nil {
			h.log.Warn("failed to parse reloaded config", zap.String("path", path), zap.Error(err))
			return
		}
		h.Update(val)
		h.log.Info("config reloaded", zap.String("path", path))
	})
}

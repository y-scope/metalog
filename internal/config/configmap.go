package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseConfigMap loads a NodeConfig from a Kubernetes ConfigMap mount directory.
// In a ConfigMap mount, each key is a file whose content is the value.
// Supports two layouts:
//  1. Single file: the directory contains a "node.yaml" file (same as LoadNodeConfig).
//  2. Multi-key: separate files for "node", "tables", "worker" are merged into one config.
func ParseConfigMap(dir string) (*NodeConfig, error) {
	// Check for single-file layout first.
	singleFile := filepath.Join(dir, "node.yaml")
	if _, err := os.Stat(singleFile); err == nil {
		return LoadNodeConfig(singleFile)
	}

	// Multi-key layout: merge individual YAML fragments.
	cfg := &NodeConfig{}

	if data, err := readConfigMapKey(dir, "node"); err == nil {
		if err := yaml.Unmarshal(data, &struct {
			Node *NodeSettings `yaml:"node"`
		}{Node: &cfg.Node}); err != nil {
			return nil, fmt.Errorf("parse configmap key 'node': %w", err)
		}
	}

	if data, err := readConfigMapKey(dir, "tables"); err == nil {
		if err := yaml.Unmarshal(data, &struct {
			Tables *[]TableConfig `yaml:"tables"`
		}{Tables: &cfg.Tables}); err != nil {
			return nil, fmt.Errorf("parse configmap key 'tables': %w", err)
		}
	}

	if data, err := readConfigMapKey(dir, "worker"); err == nil {
		if err := yaml.Unmarshal(data, &struct {
			Worker *WorkerConfig `yaml:"worker"`
		}{Worker: &cfg.Worker}); err != nil {
			return nil, fmt.Errorf("parse configmap key 'worker': %w", err)
		}
	}

	if err := cfg.validateRaw(); err != nil {
		return nil, fmt.Errorf("configmap: invalid config: %w", err)
	}
	applyDefaults(cfg)
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configmap: invalid config: %w", err)
	}
	return cfg, nil
}

// readConfigMapKey reads a ConfigMap key file, trying both with and without extension.
func readConfigMapKey(dir, key string) ([]byte, error) {
	// Try key.yaml first, then bare key name.
	for _, name := range []string{key + ".yaml", key + ".yml", key} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err == nil {
			return data, nil
		}
	}
	return nil, fmt.Errorf("configmap key %q not found in %s", key, dir)
}

// LoadConfigFromEnv loads a NodeConfig from the path specified by the CONFIG_PATH
// environment variable. Falls back to the given default path.
func LoadConfigFromEnv(defaultPath string) (*NodeConfig, error) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = defaultPath
	}

	// If path is a directory, treat it as a ConfigMap mount.
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("config path %s: %w", path, err)
	}
	if info.IsDir() {
		return ParseConfigMap(path)
	}

	return LoadNodeConfig(path)
}

// ExpandEnvVars replaces ${VAR} references in a YAML string with environment variables.
func ExpandEnvVars(data []byte) []byte {
	return []byte(os.Expand(string(data), func(key string) string {
		// Strip optional default: ${VAR:-default}
		if idx := strings.Index(key, ":-"); idx >= 0 {
			envKey := key[:idx]
			defaultVal := key[idx+2:]
			if v := os.Getenv(envKey); v != "" {
				return v
			}
			return defaultVal
		}
		return os.Getenv(key)
	}))
}

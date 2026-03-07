// Package config provides configuration types and loaders for metalog nodes.
//
// It supports YAML-based configuration with environment variable overrides,
// hot-reloadable config via file watching, and Kubernetes ConfigMap parsing.
// The central type is [NodeConfig], which defines database connections, storage
// backends, table definitions, gRPC settings, and HA strategy parameters.
package config

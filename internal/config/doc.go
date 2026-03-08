// Package config provides configuration types and loaders for metalog processes.
//
// It supports YAML-based configuration with environment variable overrides,
// hot-reloadable config via file watching, and Kubernetes ConfigMap parsing.
// The central type is [NodeConfig], which defines database connections, storage
// backends, server settings, coordinator HA parameters, and table definitions.
package config

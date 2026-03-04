package config

// StorageBackendConfig holds connection info for a single storage backend.
type StorageBackendConfig struct {
	Endpoint       string `yaml:"endpoint"`
	AccessKey      string `yaml:"accessKey"`
	SecretKey      string `yaml:"secretKey"`
	Region         string `yaml:"region"`
	ForcePathStyle bool   `yaml:"forcePathStyle"`
}

// ObjectStorageConfig holds storage settings for IR and archive files.
type ObjectStorageConfig struct {
	IRBucket                 string                          `yaml:"irBucket"`
	ArchiveBucket            string                          `yaml:"archiveBucket"`
	ClpBinaryPath            string                          `yaml:"clpBinaryPath"`
	ClpProcessTimeoutSeconds int                             `yaml:"clpProcessTimeoutSeconds"`
	DefaultBackend           string                          `yaml:"defaultBackend"`
	Backends                 map[string]StorageBackendConfig `yaml:"backends"`

	// TableCompression controls the compression clause for CREATE TABLE.
	// "lz4" (default for MySQL), "page_compressed" (auto for MariaDB), or "none".
	// When empty, auto-detected from the database type.
	TableCompression string `yaml:"tableCompression"`
}

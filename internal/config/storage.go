package config

// StorageBackendConfig holds connection info for a single storage backend.
type StorageBackendConfig struct {
	Type           string `yaml:"type"`     // "s3" (default), "tb", "http", "fs"
	Bucket         string `yaml:"bucket"`   // default bucket for this backend
	BaseURL        string `yaml:"baseUrl"`  // for http-based backends (tb, http)
	BasePath       string `yaml:"basePath"` // for filesystem backend
	Endpoint       string `yaml:"endpoint"`
	AccessKey      string `yaml:"accessKey"`
	SecretKey      string `yaml:"secretKey"`
	Region         string `yaml:"region"`
	ForcePathStyle bool   `yaml:"forcePathStyle"`
}

// ToMap returns config fields as a string map for factory consumption.
func (c StorageBackendConfig) ToMap() map[string]string {
	m := map[string]string{
		"endpoint":  c.Endpoint,
		"accessKey": c.AccessKey,
		"secretKey": c.SecretKey,
		"region":    c.Region,
		"baseUrl":   c.BaseURL,
		"basePath":  c.BasePath,
	}
	if c.ForcePathStyle {
		m["forcePathStyle"] = "true"
	}
	return m
}

// ObjectStorageConfig holds storage settings for IR and archive files.
type ObjectStorageConfig struct {
	DefaultBackend string                          `yaml:"defaultBackend"`
	Backends       map[string]StorageBackendConfig `yaml:"backends"`
}

package testutil

import (
	"context"
	"bytes"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/y-scope/metalog/storage"
)

const (
	minioImage    = "minio/minio:RELEASE.2025-02-18T16-25-55Z"
	minioUser     = "minioadmin"
	minioPassword = "minioadmin"
)

// MinIOContainer holds a running MinIO container, an S3 client, and a storage Backend.
type MinIOContainer struct {
	Container *minio.MinioContainer
	Client    *s3.Client
	Backend   storage.Backend
	Endpoint  string
}

// SetupMinIO starts a MinIO testcontainer and returns a connected client.
// The caller should defer Teardown().
func SetupMinIO(t *testing.T) *MinIOContainer {
	t.Helper()
	ctx := context.Background()

	container, err := minio.Run(ctx,
		minioImage,
		minio.WithUsername(minioUser),
		minio.WithPassword(minioPassword),
	)
	if err != nil {
		t.Fatalf("failed to start MinIO container: %v", err)
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		container.Terminate(ctx)
		t.Fatalf("failed to get MinIO connection string: %v", err)
	}

	// Ensure http:// prefix for the S3 SDK.
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "http://" + endpoint
	}

	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		})
	awsCfg := aws.Config{
		Region:                      "us-east-1",
		EndpointResolverWithOptions: resolver,
		Credentials: aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     minioUser,
					SecretAccessKey: minioPassword,
				}, nil
			}),
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return &MinIOContainer{
		Container: container,
		Client:    client,
		Backend:   storage.NewS3Backend(client),
		Endpoint:  endpoint,
	}
}

// CreateBucket creates an S3 bucket in the MinIO container.
func (mc *MinIOContainer) CreateBucket(t *testing.T, bucket string) {
	t.Helper()
	_, err := mc.Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("failed to create bucket %q: %v", bucket, err)
	}
}

// PutObject uploads a byte slice to MinIO.
func (mc *MinIOContainer) PutObject(t *testing.T, bucket, key string, data []byte) {
	t.Helper()
	err := mc.Backend.Put(context.Background(), bucket, key,
		bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("failed to put object %s/%s: %v", bucket, key, err)
	}
}

// ObjectExists returns true if the object exists in MinIO.
func (mc *MinIOContainer) ObjectExists(t *testing.T, bucket, key string) bool {
	t.Helper()
	exists, err := mc.Backend.Exists(context.Background(), bucket, key)
	if err != nil {
		t.Fatalf("failed to check object %s/%s: %v", bucket, key, err)
	}
	return exists
}

// Registry returns a storage.Registry with this MinIO backend registered under the given name.
func (mc *MinIOContainer) Registry(name string) *storage.Registry {
	reg := storage.NewRegistry()
	reg.Register(name, mc.Backend)
	return reg
}

// Teardown terminates the MinIO container.
func (mc *MinIOContainer) Teardown(t *testing.T) {
	t.Helper()
	if mc.Container != nil {
		if err := mc.Container.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate MinIO container: %v", err)
		}
	}
}

// ConnectionInfo returns the details needed to construct an S3Backend externally.
func (mc *MinIOContainer) ConnectionInfo() map[string]string {
	return map[string]string{
		"endpoint":       mc.Endpoint,
		"accessKey":      minioUser,
		"secretKey":      minioPassword,
		"region":         "us-east-1",
		"forcePathStyle": "true",
	}
}

// StorageBackendName is the default backend name used in tests.
const StorageBackendName = "minio"

// TestBucket is the default bucket name used in tests.
const TestBucket = "test-bucket"

// SetupMinIOWithBucket is a convenience that starts MinIO and creates a test bucket.
func SetupMinIOWithBucket(t *testing.T) *MinIOContainer {
	t.Helper()
	mc := SetupMinIO(t)
	mc.CreateBucket(t, TestBucket)
	return mc
}

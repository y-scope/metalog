package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func init() {
	RegisterType("s3", BackendMeta{
		RequiresBucket: true,
		Factory:        newS3BackendFromConfig,
	})
}

// newS3BackendFromConfig creates an S3 backend from a config map.
func newS3BackendFromConfig(cfg map[string]string) (StorageBackend, error) {
	endpoint := cfg["endpoint"]
	if endpoint == "" {
		return nil, fmt.Errorf("s3 backend requires endpoint")
	}
	resolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...any) (aws.Endpoint, error) {
			return aws.Endpoint{URL: endpoint}, nil
		})
	awsCfg := aws.Config{
		Region:                      cfg["region"],
		EndpointResolverWithOptions: resolver,
		Credentials: aws.CredentialsProviderFunc(
			func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     cfg["accessKey"],
					SecretAccessKey: cfg["secretKey"],
				}, nil
			}),
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg["forcePathStyle"] == "true"
	})
	return NewS3Backend(client), nil
}

// S3Backend implements StorageBackend using AWS S3.
type S3Backend struct {
	client *s3.Client
}

// NewS3Backend creates an S3 storage backend.
func NewS3Backend(client *s3.Client) *S3Backend {
	return &S3Backend{client: client}
}

func (b *S3Backend) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("s3 get: %w", err)
	}
	return out.Body, nil
}

func (b *S3Backend) Put(ctx context.Context, bucket, key string, body io.Reader, size int64) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("s3 put: %w", err)
	}
	return nil
}

func (b *S3Backend) Delete(ctx context.Context, bucket, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Treat not-found as success (idempotent delete).
		var nsk *types.NoSuchKey
		var nf *types.NotFound
		if errors.As(err, &nsk) || errors.As(err, &nf) {
			return nil
		}
		return fmt.Errorf("s3 delete: %w", err)
	}
	return nil
}

func (b *S3Backend) Exists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// HeadObject returns NotFound (HTTP 404), not NoSuchKey.
		var nf *types.NotFound
		var nsk *types.NoSuchKey
		if errors.As(err, &nf) || errors.As(err, &nsk) {
			return false, nil
		}
		return false, fmt.Errorf("s3 exists: %w", err)
	}
	return true, nil
}

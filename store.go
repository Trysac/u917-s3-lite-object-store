package s3liteobjectstore

import (
	"context"
	"time"
)

// BucketInfo describes an existing bucket.
type BucketInfo struct {
	Name         string
	CreationDate time.Time
}

// Object describes a stored S3 object and its metadata.
type Object struct {
	Key          string
	Body         []byte
	ETag         string
	ContentType  string
	Size         int64
	LastModified time.Time
}

// ObjectMeta describes an object's metadata in list responses without its body.
type ObjectMeta struct {
	Key          string
	ETag         string
	ContentType  string
	Size         int64
	LastModified time.Time
}

// ListOptions holds parameters for a ListObjectsV2 operation.
type ListOptions struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int
	ContinuationToken string
	StartAfter        string
}

// ListResult holds the result of a ListObjectsV2 operation.
type ListResult struct {
	Contents              []ObjectMeta
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
	KeyCount              int
}

// CompletePart represents a part specification in CompleteMultipartUpload.
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// Store is the storage backend interface used by [Server].
type Store interface {
	CreateBucket(ctx context.Context, bucket string, now time.Time) error
	DeleteBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	Put(ctx context.Context, bucket, key string, data []byte, contentType string, now time.Time) (ObjectMeta, error)
	Get(ctx context.Context, bucket, key string) (*Object, error)
	Delete(ctx context.Context, bucket, key string) error
	List(ctx context.Context, bucket string, opts ListOptions) (ListResult, error)

	InitMultipart(ctx context.Context, bucket, key, contentType string, now time.Time) (string, error)
	PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data []byte, now time.Time) (string, error)
	CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletePart, minPartSize, maxObjectBytes int64, now time.Time) (ObjectMeta, error)
	AbortMultipart(ctx context.Context, bucket, key, uploadID string) error
}

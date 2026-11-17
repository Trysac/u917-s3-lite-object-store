package s3liteobjectstore_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trysac/u917-s3-lite-object-store"
)

func TestDirStoreBucketLifecycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	ds, err := s3liteobjectstore.NewDirStore(root)
	if err != nil {
		t.Fatalf("NewDirStore: %v", err)
	}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	if err := ds.HeadBucket(ctx, "photos"); !errors.Is(err, s3liteobjectstore.ErrNoSuchBucket) {
		t.Fatalf("HeadBucket before create = %v, want NoSuchBucket", err)
	}
	if err := ds.CreateBucket(ctx, "photos", now); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := ds.CreateBucket(ctx, "photos", now); !errors.Is(err, s3liteobjectstore.ErrBucketAlreadyOwnedByYou) {
		t.Fatalf("second CreateBucket = %v, want BucketAlreadyOwnedByYou", err)
	}
	if err := ds.HeadBucket(ctx, "photos"); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	// A directory created outside the API is still listed, with no sidecar to read.
	if err := os.Mkdir(filepath.Join(root, "manual"), 0o750); err != nil {
		t.Fatal(err)
	}
	buckets, err := ds.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 2 || buckets[0].Name != "manual" || buckets[1].Name != "photos" {
		t.Fatalf("ListBuckets = %+v, want manual and photos", buckets)
	}
	if !buckets[1].CreationDate.Equal(now) {
		t.Fatalf("photos CreationDate = %v, want %v", buckets[1].CreationDate, now)
	}

	if _, err := ds.Put(ctx, "photos", "a/b.txt", []byte("hi"), "", now); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ds.DeleteBucket(ctx, "photos"); !errors.Is(err, s3liteobjectstore.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket non-empty = %v, want BucketNotEmpty", err)
	}
	obj, err := ds.Get(ctx, "photos", "a/b.txt")
	if err != nil || string(obj.Body) != "hi" || obj.ContentType != "application/octet-stream" {
		t.Fatalf("Get = %+v, %v", obj, err)
	}
	if err := ds.Delete(ctx, "photos", "a/b.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ds.Get(ctx, "photos", "a/b.txt"); !errors.Is(err, s3liteobjectstore.ErrNoSuchKey) {
		t.Fatalf("Get after Delete = %v, want NoSuchKey", err)
	}
	if err := ds.DeleteBucket(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "photos")); !os.IsNotExist(err) {
		t.Fatalf("bucket directory still present after DeleteBucket: %v", err)
	}
	if err := ds.DeleteBucket(ctx, "photos"); !errors.Is(err, s3liteobjectstore.ErrNoSuchBucket) {
		t.Fatalf("DeleteBucket twice = %v, want NoSuchBucket", err)
	}
}

func TestDirStoreRejectsUnsafeBucketNames(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "store")
	ds, err := s3liteobjectstore.NewDirStore(root)
	if err != nil {
		t.Fatalf("NewDirStore: %v", err)
	}
	now := time.Now()
	for _, name := range []string{"..", "../escape", "a/bc", `a\bc`, "ab", "UPPER", ""} {
		if err := ds.CreateBucket(ctx, name, now); !errors.Is(err, s3liteobjectstore.ErrInvalidBucketName) {
			t.Errorf("CreateBucket(%q) = %v, want InvalidBucketName", name, err)
		}
		if err := ds.HeadBucket(ctx, name); !errors.Is(err, s3liteobjectstore.ErrInvalidBucketName) {
			t.Errorf("HeadBucket(%q) = %v, want InvalidBucketName", name, err)
		}
		if _, err := ds.Put(ctx, name, "k", []byte("x"), "", now); !errors.Is(err, s3liteobjectstore.ErrInvalidBucketName) {
			t.Errorf("Put(%q) = %v, want InvalidBucketName", name, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(root))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("unexpected entries next to the store root: %v", entries)
	}
}

func TestDirStoreMissingBucketOperations(t *testing.T) {
	ctx := context.Background()
	ds, err := s3liteobjectstore.NewDirStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewDirStore: %v", err)
	}
	now := time.Now()
	missing := s3liteobjectstore.ErrNoSuchBucket
	if _, err := ds.Put(ctx, "nobucket", "k", []byte("x"), "", now); !errors.Is(err, missing) {
		t.Errorf("Put = %v", err)
	}
	if _, err := ds.Get(ctx, "nobucket", "k"); !errors.Is(err, missing) {
		t.Errorf("Get = %v", err)
	}
	if err := ds.Delete(ctx, "nobucket", "k"); !errors.Is(err, missing) {
		t.Errorf("Delete = %v", err)
	}
	if _, err := ds.List(ctx, "nobucket", s3liteobjectstore.ListOptions{}); !errors.Is(err, missing) {
		t.Errorf("List = %v", err)
	}
	if _, err := ds.InitMultipart(ctx, "nobucket", "k", "", now); !errors.Is(err, missing) {
		t.Errorf("InitMultipart = %v", err)
	}
	if _, err := ds.PutPart(ctx, "nobucket", "k", "id", 1, []byte("x"), now); !errors.Is(err, missing) {
		t.Errorf("PutPart = %v", err)
	}
	if _, err := ds.CompleteMultipart(ctx, "nobucket", "k", "id", nil, 0, 0, now); !errors.Is(err, missing) {
		t.Errorf("CompleteMultipart = %v", err)
	}
	if err := ds.AbortMultipart(ctx, "nobucket", "k", "id"); !errors.Is(err, missing) {
		t.Errorf("AbortMultipart = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := ds.HeadBucket(cancelled, "nobucket"); !errors.Is(err, context.Canceled) {
		t.Errorf("HeadBucket with cancelled context = %v", err)
	}
	if _, err := ds.ListBuckets(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("ListBuckets with cancelled context = %v", err)
	}
}

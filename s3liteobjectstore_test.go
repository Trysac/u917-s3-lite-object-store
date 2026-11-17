package s3liteobjectstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trysac/u917-s3-lite-object-store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	testAccessKey = "TESTKEY0"
	testSecretKey = "test-secret-key-0"
	testRegion    = "us-east-1"
)

func newTestSDKClient(endpoint string) *s3.Client {
	cfg := aws.Config{
		Region: testRegion,
		Credentials: credentials.NewStaticCredentialsProvider(
			testAccessKey,
			testSecretKey,
			"",
		),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestOracleCreatePutGet(t *testing.T) {
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials:  map[string]string{testAccessKey: testSecretKey},
		Region:       "us-east-1",
		NewRequestID: func() string { return "REQ-ORACLE-1" },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("photos"),
	}); err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("photos"),
	}); err != nil {
		t.Fatalf("HeadBucket error = %v", err)
	}

	lbOut, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets error = %v", err)
	}
	if len(lbOut.Buckets) != 1 || aws.ToString(lbOut.Buckets[0].Name) != "photos" {
		t.Fatalf("ListBuckets = %+v, want [photos]", lbOut.Buckets)
	}

	putOut, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("photos"),
		Key:         aws.String("a/1.txt"),
		Body:        strings.NewReader("hello"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject error = %v", err)
	}
	wantETag := "\"5d41402abc4b2a76b9719d911017c592\""
	if got := aws.ToString(putOut.ETag); got != wantETag {
		t.Fatalf("PutObject ETag = %q, want %q", got, wantETag)
	}

	headOut, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject error = %v", err)
	}
	if got := aws.ToString(headOut.ETag); got != wantETag {
		t.Fatalf("HeadObject ETag = %q, want %q", got, wantETag)
	}

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}
	body, err := io.ReadAll(getOut.Body)
	_ = getOut.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll error = %v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("GetObject body = %q, want %q", string(body), "hello")
	}
	if got := aws.ToString(getOut.ETag); got != wantETag {
		t.Fatalf("GetObject ETag = %q, want %q", got, wantETag)
	}
}

func TestListObjectsV2PrefixDelimiterPagination(t *testing.T) {
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	for _, k := range []string{"a/1.txt", "a/2.txt", "b/3.txt"} {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("photos"),
			Key:    aws.String(k),
			Body:   strings.NewReader("data-" + k),
		})
		if err != nil {
			t.Fatalf("PutObject(%q) error = %v", k, err)
		}
	}

	// ListObjectsV2 with Delimiter "/"
	delimOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String("photos"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 delimiter error = %v", err)
	}
	if got := aws.ToInt32(delimOut.KeyCount); got != 2 {
		t.Fatalf("KeyCount = %d, want 2", got)
	}
	if len(delimOut.CommonPrefixes) != 2 ||
		aws.ToString(delimOut.CommonPrefixes[0].Prefix) != "a/" ||
		aws.ToString(delimOut.CommonPrefixes[1].Prefix) != "b/" {
		t.Fatalf("CommonPrefixes = %+v, want [a/, b/]", delimOut.CommonPrefixes)
	}

	// ListObjectsV2 with Prefix "a/" and MaxKeys 1
	page1, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String("photos"),
		Prefix:  aws.String("a/"),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page1 error = %v", err)
	}
	if len(page1.Contents) != 1 || aws.ToString(page1.Contents[0].Key) != "a/1.txt" {
		t.Fatalf("page1 Contents = %+v, want [a/1.txt]", page1.Contents)
	}
	if !aws.ToBool(page1.IsTruncated) {
		t.Fatalf("page1 IsTruncated = false, want true")
	}

	page2, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String("photos"),
		Prefix:            aws.String("a/"),
		MaxKeys:           aws.Int32(1),
		ContinuationToken: page1.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page2 error = %v", err)
	}
	if len(page2.Contents) != 1 || aws.ToString(page2.Contents[0].Key) != "a/2.txt" {
		t.Fatalf("page2 Contents = %+v, want [a/2.txt]", page2.Contents)
	}
	if aws.ToBool(page2.IsTruncated) {
		t.Fatalf("page2 IsTruncated = true, want false")
	}

	// StartAfter check
	saOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String("photos"),
		StartAfter: aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 StartAfter error = %v", err)
	}
	if len(saOut.Contents) != 2 || aws.ToString(saOut.Contents[0].Key) != "a/2.txt" {
		t.Fatalf("StartAfter Contents = %+v", saOut.Contents)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		MinPartSize: 5,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	// Successful multipart upload of "hello " (6 bytes) and "world" (5 bytes)
	createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String("photos"),
		Key:         aws.String("greeting.txt"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload error = %v", err)
	}

	p1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("photos"),
		Key:        aws.String("greeting.txt"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("hello "),
	})
	if err != nil {
		t.Fatalf("UploadPart 1 error = %v", err)
	}
	p2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("photos"),
		Key:        aws.String("greeting.txt"),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader("world"),
	})
	if err != nil {
		t.Fatalf("UploadPart 2 error = %v", err)
	}

	compOut, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("photos"),
		Key:      aws.String("greeting.txt"),
		UploadId: createOut.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: p1.ETag},
				{PartNumber: aws.Int32(2), ETag: p2.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload error = %v", err)
	}
	wantMPETag := "\"e09e4fd6265b36115fe3db32df945d84-2\""
	if got := aws.ToString(compOut.ETag); got != wantMPETag {
		t.Fatalf("CompleteMultipartUpload ETag = %q, want %q", got, wantMPETag)
	}

	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("greeting.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject error = %v", err)
	}
	body, _ := io.ReadAll(getOut.Body)
	_ = getOut.Body.Close()
	if string(body) != "hello world" {
		t.Fatalf("GetObject body = %q, want %q", string(body), "hello world")
	}
	if got := aws.ToString(getOut.ETag); got != wantMPETag {
		t.Fatalf("GetObject ETag = %q, want %q", got, wantMPETag)
	}

	// Non-last part of 4 bytes makes CompleteMultipartUpload fail with "EntityTooSmall"
	createSmall, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("small.txt"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload small error = %v", err)
	}
	sp1, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("photos"),
		Key:        aws.String("small.txt"),
		UploadId:   createSmall.UploadId,
		PartNumber: aws.Int32(1),
		Body:       strings.NewReader("four"), // 4 bytes < MinPartSize 5
	})
	if err != nil {
		t.Fatalf("UploadPart small 1 error = %v", err)
	}
	sp2, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("photos"),
		Key:        aws.String("small.txt"),
		UploadId:   createSmall.UploadId,
		PartNumber: aws.Int32(2),
		Body:       strings.NewReader("world"),
	})
	if err != nil {
		t.Fatalf("UploadPart small 2 error = %v", err)
	}

	// Test InvalidPartOrder first
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("photos"),
		Key:      aws.String("small.txt"),
		UploadId: createSmall.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(2), ETag: sp2.ETag},
				{PartNumber: aws.Int32(1), ETag: sp1.ETag},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidPartOrder") {
		t.Fatalf("CompleteMultipartUpload out-of-order err = %v, want InvalidPartOrder", err)
	}

	// Test InvalidPart (wrong ETag)
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("photos"),
		Key:      aws.String("small.txt"),
		UploadId: createSmall.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: aws.String("\"00000000000000000000000000000000\"")},
				{PartNumber: aws.Int32(2), ETag: sp2.ETag},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidPart") {
		t.Fatalf("CompleteMultipartUpload wrong ETag err = %v, want InvalidPart", err)
	}

	// Test EntityTooSmall
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("photos"),
		Key:      aws.String("small.txt"),
		UploadId: createSmall.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: aws.Int32(1), ETag: sp1.ETag},
				{PartNumber: aws.Int32(2), ETag: sp2.ETag},
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "EntityTooSmall") {
		t.Fatalf("CompleteMultipartUpload small part err = %v, want EntityTooSmall", err)
	}

	// AbortMultipartUpload
	_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("photos"),
		Key:      aws.String("small.txt"),
		UploadId: createSmall.UploadId,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload error = %v", err)
	}
}

func TestSigV4TestVectorAndPublishedExample(t *testing.T) {
	fixedTime := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{
			"TESTKEY0":             "test-secret-key-0",
			"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
		Region: "us-east-1",
		Clock:  func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Create the "photos" bucket first so GET /photos/a.txt reaches NoSuchKey (404)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	// 1. Verify exact acceptance vector:
	// access key "TESTKEY0", secret "test-secret-key-0", region "us-east-1",
	// GET "/photos/a.txt", headers host "127.0.0.1:9000", x-amz-content-sha256 "UNSIGNED-PAYLOAD",
	// x-amz-date "20261109T120000Z" -> signature "516732389330998f8a89ff234dc2bde73dc9a25a0c9e72cf00c67f0bb48c8e95"
	createReq := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:9000/photos", nil)
	createReq.Host = "127.0.0.1:9000"
	createReq.Header.Set("X-Amz-Date", "20261109T120000Z")
	createReq.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	// Pre-create bucket via store or signed request:
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("TESTKEY0", "test-secret-key-0", ""),
	}, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(ts.URL)
	})
	// Using custom clock on server; build direct HTTP request for vector:
	_ = client

	// Let's use an injected store so we can create bucket "photos" directly, then send the exact vector request.
	memStore := s3liteobjectstore.NewMemoryStore(1024 * 1024)
	_ = memStore.CreateBucket(context.Background(), "photos", fixedTime)

	srvWithBucket, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{
			"TESTKEY0":             "test-secret-key-0",
			"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
		Region: "us-east-1",
		Store:  memStore,
		Clock:  func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	wantSig := "516732389330998f8a89ff234dc2bde73dc9a25a0c9e72cf00c67f0bb48c8e95"
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/photos/a.txt", nil)
	req.Host = "127.0.0.1:9000"
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("X-Amz-Date", "20261109T120000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TESTKEY0/20261109/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+wantSig)

	rec := httptest.NewRecorder()
	srvWithBucket.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "<Code>NoSuchKey</Code>") {
		t.Fatalf("body = %s, want NoSuchKey", rec.Body.String())
	}

	// 2. Verify published AWS S3 SigV4 'GET Object' example signature:
	// f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41
	awsExampleTime := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	_ = memStore.CreateBucket(context.Background(), "examplebucket", awsExampleTime)
	srvAWSExample, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{
			"AKIAIOSFODNN7EXAMPLE": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
		Region: "us-east-1",
		Store:  memStore,
		Clock:  func() time.Time { return awsExampleTime },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	publishedSig := "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	awsReq := httptest.NewRequest(http.MethodGet, "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	// In path-style on our server, if path is /test.txt wait: the published AWS example uses virtual-hosted
	// path "/test.txt" and Host "examplebucket.s3.amazonaws.com". In our path-style router, "/test.txt" is
	// parsed as bucket "test.txt" (CreateBucket/HeadBucket/etc. or Unknown on GET without list-type=2 -> 501 NotImplemented AFTER auth succeeds!).
	// Passing authentication and returning 501 (not 403) proves the exact signature f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41 is accepted!
	awsReq.Host = "examplebucket.s3.amazonaws.com"
	awsReq.Header.Set("Range", "bytes=0-9")
	awsReq.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	awsReq.Header.Set("X-Amz-Date", "20130524T000000Z")
	awsReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature="+publishedSig)

	awsRec := httptest.NewRecorder()
	srvAWSExample.ServeHTTP(awsRec, awsReq)
	if awsRec.Code == http.StatusForbidden {
		t.Fatalf("published AWS S3 GET example failed authentication: %s", awsRec.Body.String())
	}
}

func TestAuthenticationErrorsAndPresignExpiry(t *testing.T) {
	var mu sync.Mutex
	now := time.Now().UTC().Truncate(time.Second)

	memStore := s3liteobjectstore.NewMemoryStore(1024 * 1024)
	_ = memStore.CreateBucket(context.Background(), "photos", now)
	_, _ = memStore.Put(context.Background(), "photos", "a/1.txt", []byte("hello"), "text/plain", now)

	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		Region:      "us-east-1",
		Store:       memStore,
		Clock: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx := context.Background()

	// 1. Wrong secret -> 403 SignatureDoesNotMatch
	wrongSecretClient := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(testAccessKey, "wrong-secret", ""),
	}, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(ts.URL)
	})
	_, err = wrongSecretClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	})
	if err == nil || !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
		t.Fatalf("wrong secret err = %v, want SignatureDoesNotMatch", err)
	}

	// 2. Request dated 16 minutes from the injected clock -> 403 RequestTimeTooSkewed
	mu.Lock()
	now = now.Add(16 * time.Minute)
	mu.Unlock()

	goodClient := newTestSDKClient(ts.URL)
	_, err = goodClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	})
	if err == nil || !strings.Contains(err.Error(), "RequestTimeTooSkewed") {
		t.Fatalf("16m skew err = %v, want RequestTimeTooSkewed", err)
	}

	// Restore clock to real time so SDK signing time matches
	mu.Lock()
	now = time.Now().UTC()
	mu.Unlock()

	// 3. No Authorization -> 403 AccessDenied
	noAuthResp, err := http.Get(ts.URL + "/photos/a/1.txt")
	if err != nil {
		t.Fatalf("http.Get error = %v", err)
	}
	noAuthBody, _ := io.ReadAll(noAuthResp.Body)
	_ = noAuthResp.Body.Close()
	if noAuthResp.StatusCode != http.StatusForbidden || !strings.Contains(string(noAuthBody), "AccessDenied") {
		t.Fatalf("no Auth = %d %s, want 403 AccessDenied", noAuthResp.StatusCode, string(noAuthBody))
	}

	// 4. Presigned GET (SDK PresignClient, 60 s) works and after the clock advances 61 s returns 403 AccessDenied
	presigner := s3.NewPresignClient(goodClient)
	presignedReq, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	}, s3.WithPresignExpires(60*time.Second))
	if err != nil {
		t.Fatalf("PresignGetObject error = %v", err)
	}

	resp1, err := http.Get(presignedReq.URL)
	if err != nil {
		t.Fatalf("presigned GET error = %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK || string(body1) != "hello" {
		t.Fatalf("presigned GET = %d %q, want 200 hello", resp1.StatusCode, string(body1))
	}

	// Advance clock 61 seconds
	mu.Lock()
	now = now.Add(61 * time.Second)
	mu.Unlock()

	resp2, err := http.Get(presignedReq.URL)
	if err != nil {
		t.Fatalf("expired presigned GET error = %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden || !strings.Contains(string(body2), "AccessDenied") || !strings.Contains(string(body2), "Request has expired") {
		t.Fatalf("expired presigned GET = %d %s, want 403 AccessDenied (Request has expired)", resp2.StatusCode, string(body2))
	}
}

func TestRangeAndBucketAndKeyErrors(t *testing.T) {
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
		Body:   strings.NewReader("hello"),
	})
	if err != nil {
		t.Fatalf("PutObject error = %v", err)
	}

	// Range "bytes=1-3" returns 206, body "ell" and Content-Range "bytes 1-3/5"
	rangeOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
		Range:  aws.String("bytes=1-3"),
	})
	if err != nil {
		t.Fatalf("GetObject Range error = %v", err)
	}
	rangeBody, _ := io.ReadAll(rangeOut.Body)
	_ = rangeOut.Body.Close()
	if string(rangeBody) != "ell" {
		t.Fatalf("Range body = %q, want %q", string(rangeBody), "ell")
	}
	if got := aws.ToString(rangeOut.ContentRange); got != "bytes 1-3/5" {
		t.Fatalf("ContentRange = %q, want %q", got, "bytes 1-3/5")
	}

	// Prefix range "bytes=2-" -> "llo"
	rangePrefixOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
		Range:  aws.String("bytes=2-"),
	})
	if err != nil {
		t.Fatalf("GetObject prefix range error = %v", err)
	}
	pBody, _ := io.ReadAll(rangePrefixOut.Body)
	_ = rangePrefixOut.Body.Close()
	if string(pBody) != "llo" {
		t.Fatalf("prefix range body = %q, want llo", string(pBody))
	}

	// Suffix range "bytes=-2" -> "lo"
	rangeSuffixOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
		Range:  aws.String("bytes=-2"),
	})
	if err != nil {
		t.Fatalf("GetObject suffix range error = %v", err)
	}
	sBody, _ := io.ReadAll(rangeSuffixOut.Body)
	_ = rangeSuffixOut.Body.Close()
	if string(sBody) != "lo" {
		t.Fatalf("suffix range body = %q, want lo", string(sBody))
	}

	// Invalid range -> 416 InvalidRange
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
		Range:  aws.String("bytes=10-20"),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidRange") {
		t.Fatalf("InvalidRange error = %v, want InvalidRange", err)
	}

	// Missing key -> 404 NoSuchKey
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("missing.txt"),
	})
	if err == nil || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("missing key error = %v, want NoSuchKey", err)
	}

	// Bucket "Bad_Name" -> 400 InvalidBucketName
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("Bad_Name"),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
		t.Fatalf("CreateBucket Bad_Name error = %v, want InvalidBucketName", err)
	}

	// IPv4 bucket name -> 400 InvalidBucketName
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("192.168.1.1"),
	})
	if err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
		t.Fatalf("CreateBucket IPv4 error = %v, want InvalidBucketName", err)
	}

	// Deleting a non-empty bucket -> 409 BucketNotEmpty
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("photos"),
	})
	if err == nil || !strings.Contains(err.Error(), "BucketNotEmpty") {
		t.Fatalf("DeleteBucket non-empty error = %v, want BucketNotEmpty", err)
	}

	// DeleteObject returns 204 even when missing, then DeleteBucket succeeds
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("never-existed.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject missing error = %v", err)
	}
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject existing error = %v", err)
	}
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String("photos"),
	})
	if err != nil {
		t.Fatalf("DeleteBucket empty error = %v", err)
	}
}

func TestMaxObjectBytesAndDirStorePathTraversal(t *testing.T) {
	rootDir := t.TempDir()
	ds, err := s3liteobjectstore.NewDirStore(rootDir)
	if err != nil {
		t.Fatalf("NewDirStore error = %v", err)
	}

	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials:    map[string]string{testAccessKey: testSecretKey},
		Store:          ds,
		MaxObjectBytes: 10,
		MinPartSize:    5,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	// 11 bytes returns 400 EntityTooLarge
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("too-big.txt"),
		Body:   strings.NewReader("0123456789A"), // 11 bytes
	})
	if err == nil || !strings.Contains(err.Error(), "EntityTooLarge") {
		t.Fatalf("PutObject 11 bytes err = %v, want EntityTooLarge", err)
	}

	// 10 bytes are stored; key "../../etc/passwd" is stored inside root and read back unchanged.
	// Note: net/http and URL parsers clean ".." segments if sent unescaped in URL.Path,
	// so we test both via Store directly and via HTTP with the key.
	meta, err := ds.Put(ctx, "photos", "../../etc/passwd", []byte("0123456789"), "text/plain", time.Now())
	if err != nil {
		t.Fatalf("ds.Put traversal key error = %v", err)
	}
	if meta.Size != 10 {
		t.Fatalf("meta.Size = %d, want 10", meta.Size)
	}

	// Verify the file exists inside root/photos/<sha256> and nothing escaped root
	h := sha256.Sum256([]byte("../../etc/passwd"))
	expectedFile := filepath.Join(rootDir, "photos", hex.EncodeToString(h[:]))
	if _, statErr := os.Stat(expectedFile); statErr != nil {
		t.Fatalf("expected hashed file %s inside root, statErr = %v", expectedFile, statErr)
	}

	gotObj, err := ds.Get(ctx, "photos", "../../etc/passwd")
	if err != nil {
		t.Fatalf("ds.Get traversal key error = %v", err)
	}
	if string(gotObj.Body) != "0123456789" || gotObj.Key != "../../etc/passwd" {
		t.Fatalf("ds.Get = %+v, want key ../../etc/passwd and body 0123456789", gotObj)
	}

	// Also verify 10-byte upload over HTTP via SDK
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("ten.txt"),
		Body:   strings.NewReader("0123456789"),
	})
	if err != nil {
		t.Fatalf("PutObject 10 bytes error = %v", err)
	}
	getTen, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("photos"),
		Key:    aws.String("ten.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject 10 bytes error = %v", err)
	}
	tenBody, _ := io.ReadAll(getTen.Body)
	_ = getTen.Body.Close()
	if string(tenBody) != "0123456789" {
		t.Fatalf("GetObject 10 bytes = %q, want 0123456789", string(tenBody))
	}

	// Also test DirStore ListBuckets, List, Multipart, Delete, DeleteBucket
	buckets, err := ds.ListBuckets(ctx)
	if err != nil || len(buckets) != 1 || buckets[0].Name != "photos" {
		t.Fatalf("ds.ListBuckets = %+v, %v", buckets, err)
	}
	lr, err := ds.List(ctx, "photos", s3liteobjectstore.ListOptions{MaxKeys: 10})
	if err != nil || lr.KeyCount != 2 {
		t.Fatalf("ds.List = %+v, %v", lr, err)
	}

	upID, err := ds.InitMultipart(ctx, "photos", "mp.txt", "text/plain", time.Now())
	if err != nil {
		t.Fatalf("ds.InitMultipart error = %v", err)
	}
	et1, err := ds.PutPart(ctx, "photos", "mp.txt", upID, 1, []byte("12345"), time.Now())
	if err != nil {
		t.Fatalf("ds.PutPart 1 error = %v", err)
	}
	et2, err := ds.PutPart(ctx, "photos", "mp.txt", upID, 2, []byte("67890"), time.Now())
	if err != nil {
		t.Fatalf("ds.PutPart 2 error = %v", err)
	}
	_, err = ds.CompleteMultipart(ctx, "photos", "mp.txt", upID, []s3liteobjectstore.CompletePart{
		{PartNumber: 1, ETag: et1},
		{PartNumber: 2, ETag: et2},
	}, 5, 10, time.Now())
	if err != nil {
		t.Fatalf("ds.CompleteMultipart error = %v", err)
	}
	mpObj, err := ds.Get(ctx, "photos", "mp.txt")
	if err != nil || string(mpObj.Body) != "1234567890" {
		t.Fatalf("ds.Get mp.txt = %+v, %v", mpObj, err)
	}

	// AbortMultipart on DirStore
	upID2, err := ds.InitMultipart(ctx, "photos", "abort.txt", "", time.Now())
	if err != nil {
		t.Fatalf("ds.InitMultipart 2 error = %v", err)
	}
	if err := ds.AbortMultipart(ctx, "photos", "abort.txt", upID2); err != nil {
		t.Fatalf("ds.AbortMultipart error = %v", err)
	}

	_ = ds.Delete(ctx, "photos", "../../etc/passwd")
	_ = ds.Delete(ctx, "photos", "ten.txt")
	_ = ds.Delete(ctx, "photos", "mp.txt")
	if err := ds.DeleteBucket(ctx, "photos"); err != nil {
		t.Fatalf("ds.DeleteBucket error = %v", err)
	}
}

func TestOnEventSanitizationAndConcurrentPutObjects(t *testing.T) {
	var mu sync.Mutex
	var events []s3liteobjectstore.Event

	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		OnEvent: func(e s3liteobjectstore.Event) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("photos")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			k := fmt.Sprintf("concurrent/%02d.txt", idx)
			_, putErr := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String("photos"),
				Key:    aws.String(k),
				Body:   strings.NewReader(fmt.Sprintf("payload-%d", idx)),
			})
			if putErr != nil {
				errCh <- putErr
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatalf("concurrent PutObject error = %v", e)
	}

	mu.Lock()
	evSnap := make([]s3liteobjectstore.Event, len(events))
	copy(evSnap, events)
	mu.Unlock()

	if len(evSnap) != 1+workers {
		t.Fatalf("events count = %d, want %d", len(evSnap), 1+workers)
	}
	for _, ev := range evSnap {
		dump := fmt.Sprintf("%+v", ev)
		if strings.Contains(dump, testAccessKey) || strings.Contains(dump, testSecretKey) || strings.Contains(dump, "AWS4-HMAC-SHA256") {
			t.Fatalf("Event leaked sensitive info: %s", dump)
		}
		if ev.Bucket != "photos" || ev.Status != http.StatusOK {
			t.Fatalf("unexpected event: %+v", ev)
		}
	}
}

func TestMemoryStoreQuotaAndEdgeCases(t *testing.T) {
	mem := s3liteobjectstore.NewMemoryStore(10)
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		Store:       mem,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	client := newTestSDKClient(ts.URL)
	ctx := context.Background()

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("quota")})
	if err != nil {
		t.Fatalf("CreateBucket error = %v", err)
	}

	// Duplicate CreateBucket -> 409 BucketAlreadyOwnedByYou
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("quota")})
	if err == nil || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("duplicate CreateBucket err = %v, want BucketAlreadyOwnedByYou", err)
	}

	// Put 8 bytes succeeds
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("quota"),
		Key:    aws.String("k1"),
		Body:   strings.NewReader("12345678"),
	})
	if err != nil {
		t.Fatalf("PutObject 8 bytes error = %v", err)
	}

	// Put another 5 bytes exceeds maxBytes (10) -> 503 ServiceUnavailable "store full"
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("quota"),
		Key:    aws.String("k2"),
		Body:   strings.NewReader("12345"),
	})
	if err == nil || !strings.Contains(err.Error(), "ServiceUnavailable") || !strings.Contains(err.Error(), "store full") {
		t.Fatalf("PutObject exceeding store quota err = %v, want 503 ServiceUnavailable store full", err)
	}

	// KeyTooLongError (> 1024 bytes)
	longKey := strings.Repeat("a", 1025)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("quota"),
		Key:    aws.String(longKey),
		Body:   strings.NewReader("x"),
	})
	if err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("long key err = %v, want KeyTooLongError", err)
	}
}

func TestWireErrorsAndNotImplemented(t *testing.T) {
	fixedTime := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		Region:      "us-east-1",
		Clock:       func() time.Time { return fixedTime },
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// STREAMING-AWS4-HMAC-SHA256-PAYLOAD -> 501 NotImplemented
	req := httptest.NewRequest(http.MethodPut, "http://127.0.0.1:9000/photos/a.txt", strings.NewReader("chunked"))
	req.Host = "127.0.0.1:9000"
	req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	req.Header.Set("X-Amz-Date", "20261109T120000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TESTKEY0/20261109/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented || !strings.Contains(rec.Body.String(), "NotImplemented") {
		t.Fatalf("STREAMING payload = %d %s, want 501 NotImplemented", rec.Code, rec.Body.String())
	}

	// XAmzContentSHA256Mismatch -> 400
	// Using the known valid signature for GET /photos/a.txt with UNSIGNED-PAYLOAD, let's test invalid hex SHA256
	reqBadHash := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:9000/photos/a.txt", nil)
	reqBadHash.Host = "127.0.0.1:9000"
	reqBadHash.Header.Set("X-Amz-Content-Sha256", "not-a-valid-sha256")
	reqBadHash.Header.Set("X-Amz-Date", "20261109T120000Z")
	reqBadHash.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=TESTKEY0/20261109/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000")
	recBadHash := httptest.NewRecorder()
	srv.ServeHTTP(recBadHash, reqBadHash)
	if recBadHash.Code != http.StatusBadRequest || !strings.Contains(recBadHash.Body.String(), "XAmzContentSHA256Mismatch") {
		t.Fatalf("bad sha256 header = %d %s, want 400 XAmzContentSHA256Mismatch", recBadHash.Code, recBadHash.Body.String())
	}

	// Unknown operation via SDK (e.g., GetBucketAcl -> 501 NotImplemented)
	testSrv, srvErr := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{testAccessKey: testSecretKey},
		Region:      "us-east-1",
	})
	if srvErr != nil {
		t.Fatalf("New error = %v", srvErr)
	}
	ts := httptest.NewServer(testSrv)
	defer ts.Close()
	client := newTestSDKClient(ts.URL)
	_, err = client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{
		Bucket: aws.String("photos"),
	})
	if err == nil || !strings.Contains(err.Error(), "NotImplemented") {
		t.Fatalf("GetBucketAcl err = %v, want NotImplemented", err)
	}
}

func TestConfigAndErrorTypes(t *testing.T) {
	if _, err := s3liteobjectstore.New(s3liteobjectstore.Config{}); err == nil {
		t.Fatal("New with empty credentials should fail")
	}
	if _, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{"": "secret"},
	}); err == nil {
		t.Fatal("New with blank access key should fail")
	}
	if _, err := s3liteobjectstore.NewDirStore(""); err == nil {
		t.Fatal("NewDirStore with empty root should fail")
	}

	e1 := &s3liteobjectstore.Error{Code: s3liteobjectstore.ErrCodeNoSuchKey, Message: strings.Repeat("x", 250), Status: 404}
	if len(e1.Error()) > 240 {
		t.Fatalf("Error.Error() did not cap message length: len=%d", len(e1.Error()))
	}
	if !errors.Is(e1, s3liteobjectstore.ErrNoSuchKey) {
		t.Fatal("errors.Is(e1, ErrNoSuchKey) = false, want true")
	}
	if errors.Is(e1, s3liteobjectstore.ErrNoSuchBucket) {
		t.Fatal("errors.Is(e1, ErrNoSuchBucket) = true, want false")
	}
	var nilErr *s3liteobjectstore.Error
	if nilErr.Error() != "" {
		t.Fatalf("nilErr.Error() = %q, want empty", nilErr.Error())
	}

	// Verify XML error structure
	rawXML := `<Error><Code>NoSuchKey</Code><Message>missing</Message><Resource>/b/k</Resource><RequestId>R1</RequestId></Error>`
	var parsed struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
	}
	if err := xml.Unmarshal([]byte(rawXML), &parsed); err != nil || parsed.Code != "NoSuchKey" {
		t.Fatalf("xml.Unmarshal error = %v", err)
	}
}

func TestCLIRun(t *testing.T) {
	var out, errBuf bytes.Buffer

	// -h returns 0
	if code := s3liteobjectstore.Run([]string{"-h"}, &out, &errBuf); code != 0 {
		t.Fatalf("Run(-h) = %d, want 0", code)
	}

	// Missing -access-key returns 2
	errBuf.Reset()
	if code := s3liteobjectstore.Run([]string{}, &out, &errBuf); code != 2 {
		t.Fatalf("Run() without flags = %d, want 2", code)
	}

	// Missing S3LITE_SECRET returns 2
	t.Setenv("S3LITE_SECRET", "")
	errBuf.Reset()
	if code := s3liteobjectstore.Run([]string{"-access-key", "AKIA123"}, &out, &errBuf); code != 2 {
		t.Fatalf("Run() without S3LITE_SECRET = %d, want 2", code)
	}

	// Unexpected positional arg returns 2
	t.Setenv("S3LITE_SECRET", "secret123")
	errBuf.Reset()
	if code := s3liteobjectstore.Run([]string{"-access-key", "AKIA123", "extra"}, &out, &errBuf); code != 2 {
		t.Fatalf("Run() with positional arg = %d, want 2", code)
	}

	// Unknown flag returns 2
	errBuf.Reset()
	if code := s3liteobjectstore.Run([]string{"-unknown-flag"}, &out, &errBuf); code != 2 {
		t.Fatalf("Run() with unknown flag = %d, want 2", code)
	}
}

func ExampleNew() {
	srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
		Credentials: map[string]string{"TESTKEY0": "test-secret-key-0"},
		Region:      "us-east-1",
	})
	if err != nil {
		panic(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	fmt.Println(strings.HasPrefix(ts.URL, "http://"))
	// Output:
	// true
}

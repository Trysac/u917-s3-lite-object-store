package s3liteobjectstore

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxMultipartUploads = 1000

type memPart struct {
	partNumber int
	data       []byte
	rawMD5     [16]byte
	etag       string
}

type memUpload struct {
	bucket      string
	key         string
	uploadID    string
	contentType string
	createdAt   time.Time
	parts       map[int]memPart
	totalBytes  int64
}

type memBucket struct {
	info    BucketInfo
	objects map[string]*Object
}

type memoryStore struct {
	mu        sync.RWMutex
	maxBytes  int64
	usedBytes int64
	buckets   map[string]*memBucket
	uploads   map[string]*memUpload
}

// NewMemoryStore creates an in-memory [Store] capped at maxBytes of stored object
// and multipart part data. If maxBytes <= 0, DefaultMaxObjectBytes (64 MiB) is used.
// Writes that would exceed maxBytes are refused with HTTP 503 ServiceUnavailable
// and message "store full".
func NewMemoryStore(maxBytes int64) Store {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxObjectBytes
	}
	return &memoryStore{
		maxBytes: maxBytes,
		buckets:  make(map[string]*memBucket),
		uploads:  make(map[string]*memUpload),
	}
}

func (m *memoryStore) CreateBucket(ctx context.Context, bucket string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; exists {
		return ErrBucketAlreadyOwnedByYou
	}
	m.buckets[bucket] = &memBucket{
		info:    BucketInfo{Name: bucket, CreationDate: now.UTC()},
		objects: make(map[string]*Object),
	}
	return nil
}

func (m *memoryStore) DeleteBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, exists := m.buckets[bucket]
	if !exists {
		return ErrNoSuchBucket
	}
	if len(b.objects) > 0 {
		return ErrBucketNotEmpty
	}
	for id, u := range m.uploads {
		if u.bucket == bucket {
			m.usedBytes -= u.totalBytes
			delete(m.uploads, id)
		}
	}
	delete(m.buckets, bucket)
	return nil
}

func (m *memoryStore) HeadBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, exists := m.buckets[bucket]; !exists {
		return ErrNoSuchBucket
	}
	return nil
}

func (m *memoryStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]BucketInfo, 0, len(m.buckets))
	for _, b := range m.buckets {
		out = append(out, b.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (m *memoryStore) Put(ctx context.Context, bucket, key string, data []byte, contentType string, now time.Time) (ObjectMeta, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMeta{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, exists := m.buckets[bucket]
	if !exists {
		return ObjectMeta{}, ErrNoSuchBucket
	}
	var oldSize int64
	if prev, ok := b.objects[key]; ok {
		oldSize = prev.Size
	}
	newSize := int64(len(data))
	if m.usedBytes-oldSize+newSize > m.maxBytes {
		return ObjectMeta{}, newError(ErrServiceUnavailable, "store full")
	}
	sum := md5.Sum(data)
	etag := fmt.Sprintf("\"%x\"", sum)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	bodyCopy := make([]byte, len(data))
	copy(bodyCopy, data)
	obj := &Object{
		Key:          key,
		Body:         bodyCopy,
		ETag:         etag,
		ContentType:  contentType,
		Size:         newSize,
		LastModified: now.UTC(),
	}
	b.objects[key] = obj
	m.usedBytes = m.usedBytes - oldSize + newSize
	return ObjectMeta{
		Key:          obj.Key,
		ETag:         obj.ETag,
		ContentType:  obj.ContentType,
		Size:         obj.Size,
		LastModified: obj.LastModified,
	}, nil
}

func (m *memoryStore) Get(ctx context.Context, bucket, key string) (*Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, exists := m.buckets[bucket]
	if !exists {
		return nil, ErrNoSuchBucket
	}
	obj, ok := b.objects[key]
	if !ok {
		return nil, ErrNoSuchKey
	}
	bodyCopy := make([]byte, len(obj.Body))
	copy(bodyCopy, obj.Body)
	return &Object{
		Key:          obj.Key,
		Body:         bodyCopy,
		ETag:         obj.ETag,
		ContentType:  obj.ContentType,
		Size:         obj.Size,
		LastModified: obj.LastModified,
	}, nil
}

func (m *memoryStore) Delete(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, exists := m.buckets[bucket]
	if !exists {
		return ErrNoSuchBucket
	}
	if prev, ok := b.objects[key]; ok {
		m.usedBytes -= prev.Size
		delete(b.objects, key)
	}
	return nil
}

func (m *memoryStore) List(ctx context.Context, bucket string, opts ListOptions) (ListResult, error) {
	if err := ctx.Err(); err != nil {
		return ListResult{}, err
	}
	m.mu.RLock()
	b, exists := m.buckets[bucket]
	if !exists {
		m.mu.RUnlock()
		return ListResult{}, ErrNoSuchBucket
	}
	all := make([]ObjectMeta, 0, len(b.objects))
	for _, obj := range b.objects {
		all = append(all, ObjectMeta{
			Key:          obj.Key,
			ETag:         obj.ETag,
			ContentType:  obj.ContentType,
			Size:         obj.Size,
			LastModified: obj.LastModified,
		})
	}
	m.mu.RUnlock()
	return listObjectsFromSlice(all, opts)
}

func (m *memoryStore) InitMultipart(ctx context.Context, bucket, key, contentType string, now time.Time) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; !exists {
		return "", ErrNoSuchBucket
	}
	if len(m.uploads) >= maxMultipartUploads {
		return "", newError(ErrServiceUnavailable, "too many in-progress multipart uploads")
	}
	id := newUploadID()
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	m.uploads[id] = &memUpload{
		bucket:      bucket,
		key:         key,
		uploadID:    id,
		contentType: contentType,
		createdAt:   now.UTC(),
		parts:       make(map[int]memPart),
	}
	return id, nil
}

func (m *memoryStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data []byte, now time.Time) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; !exists {
		return "", ErrNoSuchBucket
	}
	u, ok := m.uploads[uploadID]
	if !ok || u.bucket != bucket || u.key != key {
		return "", ErrNoSuchUpload
	}
	var prevSize int64
	if prev, exists := u.parts[partNumber]; exists {
		prevSize = int64(len(prev.data))
	}
	newSize := int64(len(data))
	if m.usedBytes-prevSize+newSize > m.maxBytes {
		return "", newError(ErrServiceUnavailable, "store full")
	}
	rawSum := md5.Sum(data)
	etag := fmt.Sprintf("\"%x\"", rawSum)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	u.parts[partNumber] = memPart{
		partNumber: partNumber,
		data:       dataCopy,
		rawMD5:     rawSum,
		etag:       etag,
	}
	u.totalBytes = u.totalBytes - prevSize + newSize
	m.usedBytes = m.usedBytes - prevSize + newSize
	return etag, nil
}

func (m *memoryStore) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletePart, minPartSize, maxObjectBytes int64, now time.Time) (ObjectMeta, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMeta{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, exists := m.buckets[bucket]
	if !exists {
		return ObjectMeta{}, ErrNoSuchBucket
	}
	u, ok := m.uploads[uploadID]
	if !ok || u.bucket != bucket || u.key != key {
		return ObjectMeta{}, ErrNoSuchUpload
	}
	if len(parts) == 0 {
		return ObjectMeta{}, ErrInvalidPart
	}

	var totalLen int64
	lastNum := 0
	selected := make([]memPart, 0, len(parts))
	for i, cp := range parts {
		if cp.PartNumber <= lastNum {
			return ObjectMeta{}, ErrInvalidPartOrder
		}
		lastNum = cp.PartNumber
		p, found := u.parts[cp.PartNumber]
		if !found || !etagMatches(p.etag, cp.ETag) {
			return ObjectMeta{}, ErrInvalidPart
		}
		if i < len(parts)-1 && int64(len(p.data)) < minPartSize {
			return ObjectMeta{}, ErrEntityTooSmall
		}
		totalLen += int64(len(p.data))
		if maxObjectBytes > 0 && totalLen > maxObjectBytes {
			return ObjectMeta{}, ErrEntityTooLarge
		}
		selected = append(selected, p)
	}

	var oldObjSize int64
	if prev, exists := b.objects[key]; exists {
		oldObjSize = prev.Size
	}
	if m.usedBytes-u.totalBytes-oldObjSize+totalLen > m.maxBytes {
		return ObjectMeta{}, newError(ErrServiceUnavailable, "store full")
	}

	combined := make([]byte, 0, totalLen)
	md5Concat := make([]byte, 0, len(selected)*16)
	for _, p := range selected {
		combined = append(combined, p.data...)
		md5Concat = append(md5Concat, p.rawMD5[:]...)
	}
	finalMD5 := md5.Sum(md5Concat)
	etag := fmt.Sprintf("\"%x-%d\"", finalMD5, len(selected))

	obj := &Object{
		Key:          key,
		Body:         combined,
		ETag:         etag,
		ContentType:  u.contentType,
		Size:         totalLen,
		LastModified: now.UTC(),
	}
	b.objects[key] = obj
	m.usedBytes = m.usedBytes - u.totalBytes - oldObjSize + totalLen
	delete(m.uploads, uploadID)

	return ObjectMeta{
		Key:          obj.Key,
		ETag:         obj.ETag,
		ContentType:  obj.ContentType,
		Size:         obj.Size,
		LastModified: obj.LastModified,
	}, nil
}

func (m *memoryStore) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.buckets[bucket]; !exists {
		return ErrNoSuchBucket
	}
	u, ok := m.uploads[uploadID]
	if !ok || u.bucket != bucket || u.key != key {
		return ErrNoSuchUpload
	}
	m.usedBytes -= u.totalBytes
	delete(m.uploads, uploadID)
	return nil
}

func newUploadID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}

func etagMatches(stored, provided string) bool {
	s := strings.Trim(strings.TrimSpace(stored), "\"")
	p := strings.Trim(strings.TrimSpace(provided), "\"")
	return s != "" && s == p
}

// listObjectsFromSlice executes the S3 ListObjectsV2 sorting, filtering, delimiter
// collapsing, and continuation token logic over a slice of ObjectMeta.
func listObjectsFromSlice(all []ObjectMeta, opts ListOptions) (ListResult, error) {
	if opts.MaxKeys < 0 {
		return ListResult{}, ErrInvalidArgument
	}
	if opts.MaxKeys == 0 {
		return ListResult{IsTruncated: false, KeyCount: 0}, nil
	}
	var afterKey string
	if opts.ContinuationToken != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(opts.ContinuationToken)
		if err != nil {
			return ListResult{}, ErrInvalidArgument
		}
		afterKey = string(decoded)
	}
	if opts.StartAfter != "" && opts.StartAfter > afterKey {
		afterKey = opts.StartAfter
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })

	type entry struct {
		sortKey  string
		isPrefix bool
		meta     ObjectMeta
	}

	seenPrefixes := make(map[string]struct{})
	var entries []entry

	for _, obj := range all {
		if opts.Prefix != "" && !strings.HasPrefix(obj.Key, opts.Prefix) {
			continue
		}
		if opts.Delimiter != "" {
			rest := obj.Key[len(opts.Prefix):]
			if idx := strings.Index(rest, opts.Delimiter); idx >= 0 {
				cp := opts.Prefix + rest[:idx+len(opts.Delimiter)]
				if _, seen := seenPrefixes[cp]; !seen {
					seenPrefixes[cp] = struct{}{}
					entries = append(entries, entry{
						sortKey:  cp,
						isPrefix: true,
					})
				}
				continue
			}
		}
		entries = append(entries, entry{
			sortKey:  obj.Key,
			isPrefix: false,
			meta:     obj,
		})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].sortKey < entries[j].sortKey })

	filtered := entries[:0]
	for _, e := range entries {
		if afterKey != "" && e.sortKey <= afterKey {
			continue
		}
		filtered = append(filtered, e)
	}

	var res ListResult
	if len(filtered) > opts.MaxKeys {
		res.IsTruncated = true
		filtered = filtered[:opts.MaxKeys]
		lastKey := filtered[len(filtered)-1].sortKey
		res.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(lastKey))
	}

	for _, e := range filtered {
		if e.isPrefix {
			res.CommonPrefixes = append(res.CommonPrefixes, e.sortKey)
		} else {
			res.Contents = append(res.Contents, e.meta)
		}
	}
	res.KeyCount = len(res.Contents) + len(res.CommonPrefixes)
	return res, nil
}

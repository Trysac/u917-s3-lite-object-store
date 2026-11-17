package s3liteobjectstore

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type sidecarMeta struct {
	Key          string    `json:"key"`
	ETag         string    `json:"etag"`
	ContentType  string    `json:"content_type"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

type bucketSidecar struct {
	Name         string    `json:"name"`
	CreationDate time.Time `json:"creation_date"`
}

type dirStore struct {
	mu      sync.RWMutex
	root    string
	uploads map[string]*memUpload
}

// NewDirStore creates a directory-backed [Store] rooted at root.
// Object data is stored under root/<bucket>/<hex sha256 of key> alongside a
// <hex sha256 of key>.json metadata sidecar. Writes are staged to a temporary
// file in the bucket directory and atomically renamed into place.
func NewDirStore(root string) (Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, newError(ErrInvalidArgument, "root directory must not be empty")
	}
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cleanRoot, 0o750); err != nil {
		return nil, err
	}
	return &dirStore{
		root:    cleanRoot,
		uploads: make(map[string]*memUpload),
	}, nil
}

// checkBucketName validates bucket and rejects any name with a directory component, so callers
// can safely use filepath.Base(bucket) (identical for a valid name) as a direct child of the root.
func checkBucketName(bucket string) error {
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	if filepath.Base(bucket) != bucket {
		return ErrInvalidBucketName
	}
	return nil
}

// bucketPath joins root with name. Callers pass filepath.Base of a checked bucket name or of a
// directory entry, so every path the store touches stays a direct child of root.
func bucketPath(root, name string) string {
	return filepath.Join(root, name)
}

// keyHash returns the hex SHA-256 of key, the file name an object is stored under.
func keyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// objectPaths returns the data and metadata sidecar paths for the object file name in bDir.
// Callers pass filepath.Base(keyHash(key)): object files are named by the key hash, never the raw key.
func objectPaths(bDir, name string) (dataPath, metaPath string) {
	return filepath.Join(bDir, name), filepath.Join(bDir, name+".json")
}

// requireBucket returns ErrNoSuchBucket unless bDir is an existing directory.
func requireBucket(bDir string) error {
	info, err := os.Stat(bDir)
	if err != nil || !info.IsDir() {
		return ErrNoSuchBucket
	}
	return nil
}

// createBucketDir creates bDir and its .bucket.json sidecar; it fails if bDir already exists.
func createBucketDir(bDir string, sidecar bucketSidecar) error {
	if _, statErr := os.Stat(bDir); statErr == nil {
		return ErrBucketAlreadyOwnedByYou
	}
	if err := os.MkdirAll(bDir, 0o750); err != nil {
		return err
	}
	meta, err := json.Marshal(sidecar)
	if err != nil {
		return err
	}
	return writeAtomic(bDir, filepath.Join(bDir, ".bucket.json"), meta)
}

// checkBucketEmpty returns ErrBucketNotEmpty if bDir holds anything besides its sidecar and
// leftover temporary files, and ErrNoSuchBucket if it does not exist.
func checkBucketEmpty(bDir string) error {
	entries, err := os.ReadDir(bDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNoSuchBucket
		}
		return err
	}
	for _, e := range entries {
		if e.Name() != ".bucket.json" && !strings.HasPrefix(e.Name(), ".tmp-") {
			return ErrBucketNotEmpty
		}
	}
	return nil
}

// removeBucketDir deletes bDir and everything left in it.
func removeBucketDir(bDir string) error {
	return os.RemoveAll(bDir)
}

// readBucketSidecar reads the .bucket.json sidecar of bDir.
func readBucketSidecar(bDir string) (bucketSidecar, error) {
	var bs bucketSidecar
	raw, err := os.ReadFile(filepath.Join(bDir, ".bucket.json"))
	if err != nil {
		return bs, err
	}
	err = json.Unmarshal(raw, &bs)
	return bs, err
}

func writeAtomic(dir, targetPath string, data []byte) error {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		_ = os.Remove(tmpName)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpName)
		return closeErr
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func (d *dirStore) CreateBucket(ctx context.Context, bucket string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	return createBucketDir(bDir, bucketSidecar{Name: bucket, CreationDate: now.UTC()})
}

func (d *dirStore) DeleteBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := checkBucketEmpty(bDir); err != nil {
		return err
	}
	for id, u := range d.uploads {
		if u.bucket == bucket {
			delete(d.uploads, id)
		}
	}
	return removeBucketDir(bDir)
}

func (d *dirStore) HeadBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.RLock()
	defer d.mu.RUnlock()
	return requireBucket(bDir)
}

func (d *dirStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	entries, err := os.ReadDir(d.root)
	if err != nil {
		return nil, err
	}
	var out []BucketInfo
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		bDir := bucketPath(d.root, filepath.Base(e.Name()))
		if bs, err := readBucketSidecar(bDir); err == nil && bs.Name != "" {
			out = append(out, BucketInfo{Name: bs.Name, CreationDate: bs.CreationDate})
			continue
		}
		info, statErr := e.Info()
		var ctime time.Time
		if statErr == nil {
			ctime = info.ModTime().UTC()
		}
		out = append(out, BucketInfo{Name: e.Name(), CreationDate: ctime})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (d *dirStore) Put(ctx context.Context, bucket, key string, data []byte, contentType string, now time.Time) (ObjectMeta, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMeta{}, err
	}
	if err := checkBucketName(bucket); err != nil {
		return ObjectMeta{}, err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	return putObject(bDir, key, data, contentType, "", now)
}

func putObject(bDir, key string, data []byte, contentType, overrideETag string, now time.Time) (ObjectMeta, error) {
	if err := requireBucket(bDir); err != nil {
		return ObjectMeta{}, err
	}
	dataPath, metaPath := objectPaths(bDir, filepath.Base(keyHash(key)))

	etag := overrideETag
	if etag == "" {
		sum := md5.Sum(data)
		etag = fmt.Sprintf("\"%x\"", sum)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	sc := sidecarMeta{
		Key:          key,
		ETag:         etag,
		ContentType:  contentType,
		Size:         int64(len(data)),
		LastModified: now.UTC(),
	}
	metaBytes, err := json.Marshal(sc)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := writeAtomic(bDir, dataPath, data); err != nil {
		return ObjectMeta{}, err
	}
	if err := writeAtomic(bDir, metaPath, metaBytes); err != nil {
		_ = os.Remove(dataPath)
		return ObjectMeta{}, err
	}
	return ObjectMeta{
		Key:          sc.Key,
		ETag:         sc.ETag,
		ContentType:  sc.ContentType,
		Size:         sc.Size,
		LastModified: sc.LastModified,
	}, nil
}

func (d *dirStore) Get(ctx context.Context, bucket, key string) (*Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkBucketName(bucket); err != nil {
		return nil, err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := requireBucket(bDir); err != nil {
		return nil, err
	}
	dataPath, metaPath := objectPaths(bDir, filepath.Base(keyHash(key)))

	rawMeta, err := os.ReadFile(metaPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	var sc sidecarMeta
	if err := json.Unmarshal(rawMeta, &sc); err != nil {
		return nil, err
	}
	body, err := os.ReadFile(dataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoSuchKey
		}
		return nil, err
	}
	return &Object{
		Key:          sc.Key,
		Body:         body,
		ETag:         sc.ETag,
		ContentType:  sc.ContentType,
		Size:         sc.Size,
		LastModified: sc.LastModified,
	}, nil
}

func (d *dirStore) Delete(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := requireBucket(bDir); err != nil {
		return err
	}
	dataPath, metaPath := objectPaths(bDir, filepath.Base(keyHash(key)))
	_ = os.Remove(metaPath)
	_ = os.Remove(dataPath)
	return nil
}

func (d *dirStore) List(ctx context.Context, bucket string, opts ListOptions) (ListResult, error) {
	if err := ctx.Err(); err != nil {
		return ListResult{}, err
	}
	if err := checkBucketName(bucket); err != nil {
		return ListResult{}, err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.RLock()
	entries, err := os.ReadDir(bDir)
	if err != nil {
		d.mu.RUnlock()
		if errors.Is(err, os.ErrNotExist) {
			return ListResult{}, ErrNoSuchBucket
		}
		return ListResult{}, err
	}
	var all []ObjectMeta
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || name == ".bucket.json" || !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(bDir, filepath.Base(name)))
		if err != nil {
			continue
		}
		var sc sidecarMeta
		if json.Unmarshal(raw, &sc) == nil {
			all = append(all, ObjectMeta{
				Key:          sc.Key,
				ETag:         sc.ETag,
				ContentType:  sc.ContentType,
				Size:         sc.Size,
				LastModified: sc.LastModified,
			})
		}
	}
	d.mu.RUnlock()
	return listObjectsFromSlice(all, opts)
}

func (d *dirStore) InitMultipart(ctx context.Context, bucket, key, contentType string, now time.Time) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := checkBucketName(bucket); err != nil {
		return "", err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := requireBucket(bDir); err != nil {
		return "", err
	}
	if len(d.uploads) >= maxMultipartUploads {
		return "", newError(ErrServiceUnavailable, "too many in-progress multipart uploads")
	}
	id := newUploadID()
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	d.uploads[id] = &memUpload{
		bucket:      bucket,
		key:         key,
		uploadID:    id,
		contentType: contentType,
		createdAt:   now.UTC(),
		parts:       make(map[int]memPart),
	}
	return id, nil
}

func (d *dirStore) PutPart(ctx context.Context, bucket, key, uploadID string, partNumber int, data []byte, now time.Time) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := checkBucketName(bucket); err != nil {
		return "", err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := requireBucket(bDir); err != nil {
		return "", err
	}
	u, ok := d.uploads[uploadID]
	if !ok || u.bucket != bucket || u.key != key {
		return "", ErrNoSuchUpload
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
	return etag, nil
}

func (d *dirStore) CompleteMultipart(ctx context.Context, bucket, key, uploadID string, parts []CompletePart, minPartSize, maxObjectBytes int64, now time.Time) (ObjectMeta, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMeta{}, err
	}
	if err := checkBucketName(bucket); err != nil {
		return ObjectMeta{}, err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := requireBucket(bDir); err != nil {
		return ObjectMeta{}, err
	}
	u, ok := d.uploads[uploadID]
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

	combined := make([]byte, 0, totalLen)
	md5Concat := make([]byte, 0, len(selected)*16)
	for _, p := range selected {
		combined = append(combined, p.data...)
		md5Concat = append(md5Concat, p.rawMD5[:]...)
	}
	finalMD5 := md5.Sum(md5Concat)
	etag := fmt.Sprintf("\"%x-%d\"", finalMD5, len(selected))

	meta, err := putObject(bDir, key, combined, u.contentType, etag, now)
	if err != nil {
		return ObjectMeta{}, err
	}
	delete(d.uploads, uploadID)
	return meta, nil
}

func (d *dirStore) AbortMultipart(ctx context.Context, bucket, key, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkBucketName(bucket); err != nil {
		return err
	}
	bDir := bucketPath(d.root, filepath.Base(bucket))
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := requireBucket(bDir); err != nil {
		return err
	}
	u, ok := d.uploads[uploadID]
	if !ok || u.bucket != bucket || u.key != key {
		return ErrNoSuchUpload
	}
	delete(d.uploads, uploadID)
	return nil
}

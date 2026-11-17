package s3liteobjectstore

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Default configuration values.
const (
	DefaultRegion         = "us-east-1"
	DefaultMaxObjectBytes = int64(64 << 20) // 64 MiB
	DefaultMinPartSize    = int64(5 << 20)  // 5 MiB
	DefaultMaxKeysPerList = 1000
	maxKeyBytes           = 1024
	s3XMLNamespace        = "http://s3.amazonaws.com/doc/2006-03-01/"
)

// Event describes a completed S3 request for telemetry callbacks.
// It intentionally excludes credentials, signatures, headers, and bodies.
type Event struct {
	Op     string
	Bucket string
	Status int
}

// Config configures a [Server] instance.
type Config struct {
	// Credentials maps AWS access key IDs to secret access keys. At least one
	// non-empty access key and secret pair is required.
	Credentials map[string]string
	// Region is the expected SigV4 region (defaults to "us-east-1").
	Region string
	// Store is the backing object store (defaults to NewMemoryStore(MaxObjectBytes)).
	Store Store
	// Clock returns the current time for signature verification and timestamps
	// (defaults to time.Now).
	Clock func() time.Time
	// NewRequestID returns a request identifier for S3 headers and error responses.
	NewRequestID func() string
	// MaxObjectBytes is the maximum allowed object or part body size in bytes
	// (defaults to 64 MiB).
	MaxObjectBytes int64
	// MinPartSize is the minimum allowed size for non-final multipart upload parts
	// in bytes (defaults to 5 MiB).
	MinPartSize int64
	// MaxKeysPerList is the default and maximum number of keys returned by
	// ListObjectsV2 (defaults to 1000).
	MaxKeysPerList int
	// OnEvent is an optional hook called after each request completes.
	OnEvent func(Event)
}

// Server is an [http.Handler] implementing a subset of the Amazon S3 REST API.
type Server struct {
	credentials    map[string]string
	region         string
	store          Store
	clock          func() time.Time
	newRequestID   func() string
	maxObjectBytes int64
	minPartSize    int64
	maxKeysPerList int
	onEvent        func(Event)
}

// New validates cfg and returns a configured [*Server].
func New(cfg Config) (*Server, error) {
	if len(cfg.Credentials) == 0 {
		return nil, newError(ErrInvalidArgument, "at least one credential pair is required")
	}
	creds := make(map[string]string, len(cfg.Credentials))
	for k, v := range cfg.Credentials {
		if strings.TrimSpace(k) == "" || v == "" {
			return nil, newError(ErrInvalidArgument, "access key and secret must not be empty")
		}
		creds[k] = v
	}
	region := cfg.Region
	if region == "" {
		region = DefaultRegion
	}
	maxObj := cfg.MaxObjectBytes
	if maxObj <= 0 {
		maxObj = DefaultMaxObjectBytes
	}
	minPart := cfg.MinPartSize
	if minPart <= 0 {
		minPart = DefaultMinPartSize
	}
	maxKeys := cfg.MaxKeysPerList
	if maxKeys <= 0 {
		maxKeys = DefaultMaxKeysPerList
	}
	st := cfg.Store
	if st == nil {
		st = NewMemoryStore(maxObj)
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	reqID := cfg.NewRequestID
	if reqID == nil {
		reqID = defaultRequestID
	}
	return &Server{
		credentials:    creds,
		region:         region,
		store:          st,
		clock:          clock,
		newRequestID:   reqID,
		maxObjectBytes: maxObj,
		minPartSize:    minPart,
		maxKeysPerList: maxKeys,
		onEvent:        cfg.OnEvent,
	}, nil
}

func defaultRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// ServeHTTP implements [http.Handler].
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := s.newRequestID()
	w.Header().Set("x-amz-request-id", reqID)

	now := s.clock().UTC()
	bucket, key := parsePathStyle(r.URL.Path)
	op := classifyOp(r, bucket, key)

	auth, authErr := s.authenticateRequest(r, now)
	if authErr != nil {
		s.writeError(w, r, bucket, op, reqID, authErr)
		return
	}

	if op == "Unknown" {
		s.writeError(w, r, bucket, op, reqID, ErrNotImplemented)
		return
	}

	if bucket == "" {
		s.handleListBuckets(w, r, op, reqID)
		return
	}

	if err := validateBucketName(bucket); err != nil {
		s.writeError(w, r, bucket, op, reqID, err)
		return
	}

	if key == "" {
		s.handleBucketOp(w, r, bucket, op, reqID, auth, now)
		return
	}

	if err := validateKey(key); err != nil {
		s.writeError(w, r, bucket, op, reqID, err)
		return
	}

	s.handleObjectOp(w, r, bucket, key, op, reqID, auth, now)
}

func parsePathStyle(p string) (bucket, key string) {
	trimmed := strings.TrimPrefix(p, "/")
	if trimmed == "" {
		return "", ""
	}
	b, k, ok := strings.Cut(trimmed, "/")
	if !ok {
		return b, ""
	}
	return b, k
}

var unsupportedSubresources = []string{
	"acl", "policy", "versioning", "versions", "tagging", "cors",
	"lifecycle", "encryption", "replication", "website", "logging",
	"notification", "accelerate", "requestPayment", "object-lock", "legal-hold",
	"retention", "restore", "select", "inventory", "metrics", "analytics",
}

func hasUnsupportedQuery(r *http.Request) bool {
	q := r.URL.Query()
	for _, sub := range unsupportedSubresources {
		if q.Has(sub) {
			return true
		}
	}
	if r.Header.Get("x-amz-copy-source") != "" {
		return true
	}
	return false
}

func classifyOp(r *http.Request, bucket, key string) string {
	if hasUnsupportedQuery(r) {
		return "Unknown"
	}
	q := r.URL.Query()
	if bucket == "" {
		if r.Method == http.MethodGet {
			return "ListBuckets"
		}
		return "Unknown"
	}
	if key == "" {
		if q.Has("delete") {
			return "Unknown"
		}
		switch r.Method {
		case http.MethodPut:
			return "CreateBucket"
		case http.MethodDelete:
			return "DeleteBucket"
		case http.MethodHead:
			return "HeadBucket"
		case http.MethodGet:
			if q.Get("list-type") == "2" {
				return "ListObjectsV2"
			}
			return "Unknown"
		default:
			return "Unknown"
		}
	}

	if q.Has("uploads") && r.Method == http.MethodPost {
		return "CreateMultipartUpload"
	}
	if q.Get("uploadId") != "" {
		switch r.Method {
		case http.MethodPut:
			if q.Get("partNumber") != "" {
				return "UploadPart"
			}
			return "Unknown"
		case http.MethodPost:
			return "CompleteMultipartUpload"
		case http.MethodDelete:
			return "AbortMultipartUpload"
		default:
			return "Unknown"
		}
	}

	switch r.Method {
	case http.MethodPut:
		return "PutObject"
	case http.MethodGet:
		return "GetObject"
	case http.MethodHead:
		return "HeadObject"
	case http.MethodDelete:
		return "DeleteObject"
	default:
		return "Unknown"
	}
}

func validateBucketName(name string) *Error {
	if len(name) < 3 || len(name) > 63 {
		return ErrInvalidBucketName
	}
	if !isLowerAlnum(name[0]) || !isLowerAlnum(name[len(name)-1]) {
		return ErrInvalidBucketName
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !isLowerAlnum(c) && c != '.' && c != '-' {
			return ErrInvalidBucketName
		}
		if i > 0 && c == '.' && name[i-1] == '.' {
			return ErrInvalidBucketName
		}
	}
	if ip := net.ParseIP(name); ip != nil && ip.To4() != nil {
		return ErrInvalidBucketName
	}
	return nil
}

func isLowerAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func validateKey(key string) *Error {
	if len(key) < 1 || len(key) > maxKeyBytes || !utf8.ValidString(key) {
		return ErrKeyTooLongError
	}
	return nil
}

func (s *Server) readVerifiedBody(w http.ResponseWriter, r *http.Request, auth authResult) ([]byte, *Error) {
	if r.Body == nil {
		if auth.payloadHash != unsignedPayload && !strings.EqualFold(auth.payloadHash, sha256Hex(nil)) {
			return nil, ErrXAmzContentSHA256Mismatch
		}
		return nil, nil
	}
	reader := http.MaxBytesReader(w, r.Body, s.maxObjectBytes)
	data, err := io.ReadAll(reader)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, ErrEntityTooLarge
		}
		return nil, ErrInternalError
	}
	if auth.payloadHash != unsignedPayload {
		actual := sha256Hex(data)
		if !strings.EqualFold(actual, auth.payloadHash) {
			return nil, ErrXAmzContentSHA256Mismatch
		}
	}
	return data, nil
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request, op, reqID string) {
	buckets, err := s.store.ListBuckets(r.Context())
	if err != nil {
		s.writeStoreError(w, r, "", op, reqID, err)
		return
	}
	type xmlBucket struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	type listAllMyBucketsResult struct {
		XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
		Xmlns   string      `xml:"xmlns,attr"`
		Buckets []xmlBucket `xml:"Buckets>Bucket"`
	}
	res := listAllMyBucketsResult{
		Xmlns:   s3XMLNamespace,
		Buckets: make([]xmlBucket, len(buckets)),
	}
	for i, b := range buckets {
		res.Buckets[i] = xmlBucket{
			Name:         b.Name,
			CreationDate: b.CreationDate.UTC().Format(time.RFC3339),
		}
	}
	s.writeXML(w, "", op, http.StatusOK, res)
}

func (s *Server) handleBucketOp(w http.ResponseWriter, r *http.Request, bucket, op, reqID string, auth authResult, now time.Time) {
	switch op {
	case "CreateBucket":
		if _, err := s.readVerifiedBody(w, r, auth); err != nil {
			s.writeError(w, r, bucket, op, reqID, err)
			return
		}
		if err := s.store.CreateBucket(r.Context(), bucket, now); err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		s.emitEvent(op, bucket, http.StatusOK)

	case "DeleteBucket":
		if err := s.store.DeleteBucket(r.Context(), bucket); err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		s.emitEvent(op, bucket, http.StatusNoContent)

	case "HeadBucket":
		if err := s.store.HeadBucket(r.Context(), bucket); err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		s.emitEvent(op, bucket, http.StatusOK)

	case "ListObjectsV2":
		q := r.URL.Query()
		maxKeys := s.maxKeysPerList
		if mkStr := q.Get("max-keys"); mkStr != "" {
			n, err := strconv.Atoi(mkStr)
			if err != nil || n < 0 {
				s.writeError(w, r, bucket, op, reqID, ErrInvalidArgument)
				return
			}
			if n < maxKeys {
				maxKeys = n
			}
		}
		opts := ListOptions{
			Prefix:            q.Get("prefix"),
			Delimiter:         q.Get("delimiter"),
			MaxKeys:           maxKeys,
			ContinuationToken: q.Get("continuation-token"),
			StartAfter:        q.Get("start-after"),
		}
		res, err := s.store.List(r.Context(), bucket, opts)
		if err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		type xmlContent struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			StorageClass string `xml:"StorageClass"`
		}
		type xmlCommonPrefix struct {
			Prefix string `xml:"Prefix"`
		}
		type listBucketResult struct {
			XMLName               xml.Name          `xml:"ListBucketResult"`
			Xmlns                 string            `xml:"xmlns,attr"`
			Name                  string            `xml:"Name"`
			Prefix                string            `xml:"Prefix"`
			Delimiter             string            `xml:"Delimiter,omitempty"`
			KeyCount              int               `xml:"KeyCount"`
			MaxKeys               int               `xml:"MaxKeys"`
			IsTruncated           bool              `xml:"IsTruncated"`
			ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
			NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
			StartAfter            string            `xml:"StartAfter,omitempty"`
			Contents              []xmlContent      `xml:"Contents"`
			CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes"`
		}
		out := listBucketResult{
			Xmlns:                 s3XMLNamespace,
			Name:                  bucket,
			Prefix:                opts.Prefix,
			Delimiter:             opts.Delimiter,
			KeyCount:              res.KeyCount,
			MaxKeys:               maxKeys,
			IsTruncated:           res.IsTruncated,
			ContinuationToken:     opts.ContinuationToken,
			NextContinuationToken: res.NextContinuationToken,
			StartAfter:            opts.StartAfter,
			Contents:              make([]xmlContent, len(res.Contents)),
			CommonPrefixes:        make([]xmlCommonPrefix, len(res.CommonPrefixes)),
		}
		for i, c := range res.Contents {
			out.Contents[i] = xmlContent{
				Key:          c.Key,
				LastModified: c.LastModified.UTC().Format(time.RFC3339),
				ETag:         c.ETag,
				Size:         c.Size,
				StorageClass: "STANDARD",
			}
		}
		for i, cp := range res.CommonPrefixes {
			out.CommonPrefixes[i] = xmlCommonPrefix{Prefix: cp}
		}
		s.writeXML(w, bucket, op, http.StatusOK, out)
	}
}

func (s *Server) handleObjectOp(w http.ResponseWriter, r *http.Request, bucket, key, op, reqID string, auth authResult, now time.Time) {
	switch op {
	case "PutObject":
		data, readErr := s.readVerifiedBody(w, r, auth)
		if readErr != nil {
			s.writeError(w, r, bucket, op, reqID, readErr)
			return
		}
		ct := r.Header.Get("Content-Type")
		meta, err := s.store.Put(r.Context(), bucket, key, data, ct, now)
		if err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.Header().Set("ETag", meta.ETag)
		w.WriteHeader(http.StatusOK)
		s.emitEvent(op, bucket, http.StatusOK)

	case "GetObject", "HeadObject":
		obj, err := s.store.Get(r.Context(), bucket, key)
		if err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.Header().Set("ETag", obj.ETag)
		w.Header().Set("Content-Type", obj.ContentType)
		w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")

		rangeHdr := r.Header.Get("Range")
		if rangeHdr != "" && op == "GetObject" {
			start, end, ok := parseByteRange(rangeHdr, obj.Size)
			if !ok {
				s.writeError(w, r, bucket, op, reqID, ErrInvalidRange)
				return
			}
			part := obj.Body[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, obj.Size))
			w.Header().Set("Content-Length", strconv.Itoa(len(part)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(part)
			s.emitEvent(op, bucket, http.StatusPartialContent)
			return
		}

		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
		w.WriteHeader(http.StatusOK)
		if op == "GetObject" {
			_, _ = w.Write(obj.Body)
		}
		s.emitEvent(op, bucket, http.StatusOK)

	case "DeleteObject":
		if err := s.store.Delete(r.Context(), bucket, key); err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		s.emitEvent(op, bucket, http.StatusNoContent)

	case "CreateMultipartUpload":
		if _, readErr := s.readVerifiedBody(w, r, auth); readErr != nil {
			s.writeError(w, r, bucket, op, reqID, readErr)
			return
		}
		ct := r.Header.Get("Content-Type")
		uploadID, err := s.store.InitMultipart(r.Context(), bucket, key, ct, now)
		if err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		type initiateMultipartUploadResult struct {
			XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
			Xmlns    string   `xml:"xmlns,attr"`
			Bucket   string   `xml:"Bucket"`
			Key      string   `xml:"Key"`
			UploadId string   `xml:"UploadId"`
		}
		s.writeXML(w, bucket, op, http.StatusOK, initiateMultipartUploadResult{
			Xmlns:    s3XMLNamespace,
			Bucket:   bucket,
			Key:      key,
			UploadId: uploadID,
		})

	case "UploadPart":
		q := r.URL.Query()
		uploadID := q.Get("uploadId")
		partNum, err := strconv.Atoi(q.Get("partNumber"))
		if err != nil || partNum < 1 || partNum > 10000 {
			s.writeError(w, r, bucket, op, reqID, ErrInvalidArgument)
			return
		}
		data, readErr := s.readVerifiedBody(w, r, auth)
		if readErr != nil {
			s.writeError(w, r, bucket, op, reqID, readErr)
			return
		}
		etag, storeErr := s.store.PutPart(r.Context(), bucket, key, uploadID, partNum, data, now)
		if storeErr != nil {
			s.writeStoreError(w, r, bucket, op, reqID, storeErr)
			return
		}
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		s.emitEvent(op, bucket, http.StatusOK)

	case "CompleteMultipartUpload":
		uploadID := r.URL.Query().Get("uploadId")
		body, readErr := s.readVerifiedBody(w, r, auth)
		if readErr != nil {
			s.writeError(w, r, bucket, op, reqID, readErr)
			return
		}
		type completeMultipartUploadReq struct {
			Parts []CompletePart `xml:"Part"`
		}
		var reqXML completeMultipartUploadReq
		if err := xml.Unmarshal(body, &reqXML); err != nil {
			s.writeError(w, r, bucket, op, reqID, ErrMalformedXML)
			return
		}
		meta, err := s.store.CompleteMultipart(r.Context(), bucket, key, uploadID, reqXML.Parts, s.minPartSize, s.maxObjectBytes, now)
		if err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		type completeMultipartUploadResult struct {
			XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
			Xmlns    string   `xml:"xmlns,attr"`
			Location string   `xml:"Location"`
			Bucket   string   `xml:"Bucket"`
			Key      string   `xml:"Key"`
			ETag     string   `xml:"ETag"`
		}
		s.writeXML(w, bucket, op, http.StatusOK, completeMultipartUploadResult{
			Xmlns:    s3XMLNamespace,
			Location: "/" + bucket + "/" + key,
			Bucket:   bucket,
			Key:      key,
			ETag:     meta.ETag,
		})

	case "AbortMultipartUpload":
		uploadID := r.URL.Query().Get("uploadId")
		if err := s.store.AbortMultipart(r.Context(), bucket, key, uploadID); err != nil {
			s.writeStoreError(w, r, bucket, op, reqID, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		s.emitEvent(op, bucket, http.StatusNoContent)
	}
}

func parseByteRange(hdr string, size int64) (int64, int64, bool) {
	if size <= 0 || !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}
	if startStr == "" {
		// Suffix range: bytes=-n
		if endStr == "" {
			return 0, 0, false
		}
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if endStr == "" {
		// Prefix range: bytes=a-
		return start, size - 1, true
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

func (s *Server) writeXML(w http.ResponseWriter, bucket, op string, status int, payload any) {
	raw, err := xml.Marshal(payload)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		s.emitEvent(op, bucket, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(raw)
	s.emitEvent(op, bucket, status)
}

func (s *Server) writeStoreError(w http.ResponseWriter, r *http.Request, bucket, op, reqID string, err error) {
	var s3Err *Error
	if errors.As(err, &s3Err) {
		s.writeError(w, r, bucket, op, reqID, s3Err)
		return
	}
	s.writeError(w, r, bucket, op, reqID, ErrInternalError)
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, bucket, op, reqID string, s3Err *Error) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(s3Err.Status)
	if r.Method != http.MethodHead {
		resp := xmlErrorResponse{
			Code:      s3Err.Code,
			Message:   s3Err.Message,
			Resource:  r.URL.Path,
			RequestID: reqID,
		}
		if raw, err := xml.Marshal(resp); err == nil {
			_, _ = w.Write([]byte(xml.Header))
			_, _ = w.Write(raw)
		}
	}
	s.emitEvent(op, bucket, s3Err.Status)
}

func (s *Server) emitEvent(op, bucket string, status int) {
	if s.onEvent != nil {
		s.onEvent(Event{
			Op:     op,
			Bucket: bucket,
			Status: status,
		})
	}
}

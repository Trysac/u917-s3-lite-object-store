// Package s3liteobjectstore provides an embeddable HTTP server handler speaking
// a subset of the Amazon S3 REST API with full AWS Signature Version 4
// authentication, presigned URLs, ListObjectsV2, and multipart uploads.
//
// # Overview
//
// Full S3 clones are heavy external processes, while naive test doubles often
// skip signature verification and hide authentication bugs until production.
// [Server] implements [net/http.Handler] using only the Go standard library,
// enforcing real SigV4 header and presigned URL verification against an
// in-memory ([NewMemoryStore]) or atomic directory-backed ([NewDirStore])
// storage backend.
//
// # Supported Operations
//
// The server supports path-style addressing (/bucket and /bucket/key) for:
//   - CreateBucket (PUT /bucket)
//   - DeleteBucket (DELETE /bucket)
//   - HeadBucket (HEAD /bucket)
//   - ListBuckets (GET /)
//   - PutObject (PUT /bucket/key)
//   - GetObject (GET /bucket/key, including single byte-range requests)
//   - HeadObject (HEAD /bucket/key)
//   - DeleteObject (DELETE /bucket/key)
//   - ListObjectsV2 (GET /bucket?list-type=2)
//   - CreateMultipartUpload (POST /bucket/key?uploads)
//   - UploadPart (PUT /bucket/key?partNumber=N&uploadId=ID)
//   - CompleteMultipartUpload (POST /bucket/key?uploadId=ID)
//   - AbortMultipartUpload (DELETE /bucket/key?uploadId=ID)
//
// # Threat Model
//
// Defended:
//   - Forged or tampered requests (AWS Signature Version 4 HMAC-SHA256 verification
//     and SHA-256 payload hash validation when provided).
//   - Timing attacks on signature comparison (constant-time [crypto/hmac.Equal]).
//   - Expired presigned URLs and requests skewed more than 15 minutes from the
//     configured clock.
//   - Path traversal in object keys on disk ([NewDirStore] stores objects by the
//     hex SHA-256 digest of their key plus a JSON metadata sidecar).
//   - Oversized payloads and resource exhaustion (enforced via [net/http.MaxBytesReader],
//     bounded in-progress multipart uploads, and memory store byte caps).
//   - Secret leakage in telemetry ([Event] contains only operation, bucket, and
//     HTTP status; error messages are sanitized).
//
// Not defended:
//   - Replay of a valid signed request within the 15-minute clock skew window
//     or before a presigned URL expires.
//   - Transport layer security (deploy behind a TLS termination proxy or
//     use [net/http.Server.ListenAndServeTLS]).
//   - Fine-grained bucket policies, IAM roles, or ACLs (any configured access
//     key has full access to all buckets in the server instance).
package s3liteobjectstore

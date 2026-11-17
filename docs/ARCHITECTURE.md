# Architecture

`s3liteobjectstore` is organized into focused components within the root package:

1. **HTTP Handler & Router (`s3liteobjectstore.go`)**:
   - Parses path-style routes (`/`, `/bucket`, `/bucket/key`).
   - Classifies the S3 operation from the HTTP method and query parameters (`list-type=2`, `uploads`, `uploadId`, `partNumber`).
   - Validates bucket names (3-63 chars, lowercase alphanumeric, `.`, `-`, non-IPv4) and object keys (1-1024 bytes of valid UTF-8).
   - Bounds request bodies with `http.MaxBytesReader` and verifies the SHA-256 payload digest unless `UNSIGNED-PAYLOAD` is specified.
   - Formats responses and errors in the `http://s3.amazonaws.com/doc/2006-03-01/` XML namespace.

2. **Signature Version 4 Verifier (`sigv4.go`)**:
   - Supports both `Authorization: AWS4-HMAC-SHA256 ...` headers and query-string presigned URLs (`X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`, `X-Amz-SignedHeaders`, `X-Amz-Signature`).
   - Builds the canonical request (method, single-encoded path keeping `/`, RFC 3986 sorted query string, lowercase trimmed canonical headers, signed headers, payload hash).
   - Derives the signing key via the 4-step HMAC-SHA256 chain (`AWS4`+secret -> date -> region -> `s3` -> `aws4_request`) and compares signatures in constant time with `hmac.Equal`.

3. **Storage Backends (`store.go`, `memory_store.go`, `dir_store.go`)**:
   - `Store` defines concurrency-safe bucket, object, listing, and multipart operations taking `context.Context`.
   - `NewMemoryStore(maxBytes)` tracks total byte usage across objects and in-progress multipart uploads, refusing writes beyond `maxBytes` with `503 ServiceUnavailable` (`store full`).
   - `NewDirStore(root)` persists each object at `root/<bucket>/<hex-sha256-of-key>` with a `<hex-sha256-of-key>.json` sidecar, writing via a temporary file and `os.Rename` to guarantee atomic updates and prevent path traversal.

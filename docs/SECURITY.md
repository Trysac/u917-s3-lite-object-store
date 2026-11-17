# Security & Threat Model

## Defended Threats

- **Forged or Tampered Requests**: Verified using AWS Signature Version 4. Requests with `x-amz-content-sha256` digest must match body hash; otherwise 400 `XAmzContentSHA256Mismatch` is returned.
- **Signature Timing Attacks**: Signatures are compared using constant-time `crypto/hmac.Equal`.
- **Expired Presigned URLs & Clock Skew**: Requests are strictly validated against the server clock (±15 minutes skew). Presigned URLs enforce `X-Amz-Expires` (1-604800s); expired URLs return 403 `AccessDenied`.
- **Filesystem Path Traversal**: `DirStore` uses hashed object keys (SHA-256) for storage paths, effectively neutralizing directory traversal attempts like `../../etc/passwd`.
- **Denial of Service**: Request bodies are capped via `http.MaxBytesReader` (default 64 MiB). Multipart uploads are limited to 1000 in-progress operations. Memory stores enforce hard byte limits.
- **Secret Leakage**: The CLI reads the secret key from `S3LITE_SECRET`, preventing leakage via process lists. Event hooks never expose sensitive headers or credentials.

## Out of Scope / Not Defended

- **Replay Attacks**: Valid requests replayed within the 15-minute skew window or before presigned URL expiration are accepted.
- **Transport Layer Security (TLS)**: Handlers communicate via HTTP; users must deploy behind a TLS termination proxy.
- **IAM and ACLs**: All valid credentials possess full, unrestricted access to all buckets and objects within the instance.

# s3-lite-object-store

Object store server speaking a subset of the S3 REST API with SigV4 authentication, presigned URLs, multipart uploads and ListObjectsV2.

## Why `s3-lite-object-store`?

Tests and small deployments that talk to S3 need a local server, and full S3 clones are heavy services with many features nobody uses. A fake that skips signature checks hides authentication bugs until production. This package is an embeddable `http.Handler` that verifies AWS Signature Version 4 like S3 does and implements the bucket, object, byte-range, `ListObjectsV2`, and multipart upload calls that the official AWS SDK uses most.

## Installation

```bash
go get github.com/Trysac/u917-s3-lite-object-store
```

## Usage

```go
srv, err := s3liteobjectstore.New(s3liteobjectstore.Config{
	Credentials: map[string]string{"TESTKEY0": "test-secret-key-0"},
	Region:      "us-east-1",
})
if err != nil {
	log.Fatal(err)
}
ts := httptest.NewServer(srv)
defer ts.Close()
```

## Public API Summary

- `func New(cfg Config) (*Server, error)` where `*Server` implements `http.Handler`.
- `Config`: `Credentials` map, `Region`, `Store` interface, `Clock` function, `NewRequestID` function, `MaxObjectBytes` (64 MiB default), `MinPartSize` (5 MiB default), `MaxKeysPerList` (1000 default), and `OnEvent` callback.
- Stores: `NewMemoryStore(maxBytes)` and `NewDirStore(root)` providing atomic persistence.
- Errors: Returns 400, 403, 404, 409, 416, 501, 503 errors compatible with S3 XML format.
- Limits: Bucket names 3-63 chars (lowercase, alphanumeric, dots, hyphens), keys 1-1024 bytes (UTF-8), multipart uploads capped at 1000 per server.

## Why not MinIO or LocalStack?

These are heavy external services. `s3-lite-object-store` is an embeddable handler with no external process that still enforces real SigV4 authentication.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the complete threat model and limits.

## License

MIT

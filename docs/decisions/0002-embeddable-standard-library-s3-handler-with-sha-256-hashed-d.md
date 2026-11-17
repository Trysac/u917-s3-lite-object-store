# Embeddable Standard-Library S3 Handler with SHA-256 Hashed Directory Storage and AWS SDK v2 Test Oracle

Status: accepted (generated, pending human review)

## Context

Applications and tests that interact with Amazon S3 need a fast, embeddable server that verifies real AWS Signature Version 4 requests without requiring external processes like MinIO or LocalStack, while keeping the library free of third-party runtime dependencies.

## Decision

Implement the S3 REST protocol subset, SigV4 header and presigned URL verifier, in-memory store, and directory-backed store using only the Go standard library. Map keys on disk in DirStore to their hex SHA-256 digest plus a JSON sidecar written atomically via temp-file and os.Rename. Use the official github.com/aws/aws-sdk-go-v2/service/s3 client exclusively in _test.go files as the wire-compatibility oracle.

## Consequences

Zero external runtime dependencies in library and CLI binaries, immunity to filesystem path traversal regardless of object key characters, and verified wire compatibility with the official AWS SDK for Go v2.

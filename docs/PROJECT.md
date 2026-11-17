# Project record

## Objective

To provide an embeddable S3-compatible object store server that enforces real AWS Signature Version 4 (SigV4) authentication, enabling accurate testing of authentication, region handling, and clock-skew logic without the overhead of heavy external S3 clones.

## Scope

- Implementing S3 REST API subset: path-style addressing (`/bucket/key`), `CreateBucket`, `DeleteBucket`, `ListBuckets`, `HeadBucket`, `PutObject`, `GetObject` (single range), `HeadObject`, `DeleteObject`, `ListObjectsV2` (prefix, delimiter, max-keys, continuation token, start-after), `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`.
- SigV4 Authentication: Authorization headers and query-string presigned URLs, SHA-256 payload verification.
- Storage backends: `MemoryStore` and `DirStore`.
- CLI for standalone operation via environment variables.

## Out of Scope

- Virtual-hosted addressing.
- Versioning, ACLs, and Bucket Policies.
- `DeleteObjects`, `CopyObject`.
- Object tagging.
- Checksums other than MD5 (ETags) and SHA-256 (payload hash).
- `aws-chunked` uploads and SSE (Server Side Encryption).

## Factory run u917 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-10 | generation | google/gemini-4-argon | yes | 6172 | 46675 | 0 | 0 | 0.9582 |
| 2026-11-10 | fix-1 | google/gemini-3.8-flash | no | 617323 | 2276 | 0 | 678232 | 0.5224 |
| 2026-11-10 | generation | google/gemini-4-argon | yes | 0 | 0 | 0 | 0 | 0.0000 |
| 2026-11-10 | generation | google/gemini-4-argon | yes | 6172 | 48980 | 0 | 0 | 1.0043 |
| 2026-11-10 | fix-1 | google/gemini-3.8-flash | no | 758490 | 2454 | 0 | 1332564 | 0.6780 |
| 2026-11-10 | fix-2 | google/gemini-3.8-flash | no | 385715 | 791 | 0 | 620924 | 0.3388 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 0 | 0 | 0 | 0 | 0.0000 |
| 2026-11-17 | threat-model | google/gemini-3.8-flash | no | 0 | 0 | 0 | 0 | 0.0000 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 6250 | 1711 | 0 | 0 | 0.0041 |

Total: $3.5058. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).

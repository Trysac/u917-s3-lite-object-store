# Testing

## Running

```sh
go test -cover .          # tests with coverage summary
go run ./scripts/check    # every gate, as CI and the factory run it
```

## Coverage & Policy

- Statement coverage must stay at or above 80%.
- Tests reside in `s3liteobjectstore_test` package, asserting observable effects via public API calls only.
- Table-driven tests using `t.Run` are preferred.
- Tests are deterministic, relying on `httptest.NewServer`, injected clocks, and injected request IDs.
- Races are detected via the built-in `-race` detector.
- Every bug fix must include a regression test that fails without the fix.

## Test Files

- `store_test.go`: Exercises `MemoryStore` and `DirStore` persistence logic.
- `sigv4_test.go`: Verifies SigV4 canonicalization and HMAC chaining using independent vectors.
- `s3liteobjectstore_test.go`: Uses `aws-sdk-go-v2` as a conformance oracle to verify S3 protocol compliance (buckets, objects, multipart, ranges).
- `example_test.go`: Contains runnable `Example` functions verified by `go test` output comments.

// Package s3liteobjectstore is a small, dependency-free Go library: Object store server speaking a subset of the S3 REST API with SigV4 authentication, presigned URLs, multipart uploads and ListObjectsV2
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package s3liteobjectstore

# s3-lite-object-store

[![Go Reference](https://pkg.go.dev/badge/github.com/Trysac/u917-s3-lite-object-store.svg)](https://pkg.go.dev/github.com/Trysac/u917-s3-lite-object-store)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u917-s3-lite-object-store/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u917-s3-lite-object-store/actions/workflows/ci.yml)

Object store server speaking a subset of the S3 REST API with SigV4 authentication, presigned URLs, multipart uploads and ListObjectsV2

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
go get github.com/Trysac/u917-s3-lite-object-store
```

Requires Go 1.23 or newer. No dependencies outside the standard library.

## Usage

```go
package main

import (
	"errors"
	"fmt"

	"github.com/Trysac/u917-s3-lite-object-store"
)

func main() {
	// Normalize trims surrounding whitespace and lower-cases the text.
	out, _ := s3liteobjectstore.Normalize("  Hello World ")
	fmt.Println(out) // -> "hello world"

	// Blank input is rejected explicitly instead of returning an empty string.
	if _, err := s3liteobjectstore.Normalize("   "); errors.Is(err, s3liteobjectstore.ErrBlank) {
		fmt.Println("rejected:", err)
	}
}
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez

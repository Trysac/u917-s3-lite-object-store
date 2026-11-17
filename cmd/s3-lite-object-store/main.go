// Command s3-lite-object-store starts a lightweight S3-compatible HTTP object
// store server.
package main

import (
	"os"

	"github.com/Trysac/u917-s3-lite-object-store"
)

func main() {
	os.Exit(s3liteobjectstore.Run(os.Args[1:], os.Stdout, os.Stderr))
}

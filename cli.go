package s3liteobjectstore

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// Run executes the s3-lite-object-store command-line interface.
// It parses args, reads the secret key from the S3LITE_SECRET environment
// variable (never a flag), starts the HTTP server on -addr (default 127.0.0.1:9000),
// and returns an exit code (0 on clean shutdown or -h, 2 on usage or configuration error).
func Run(args []string, stdout, stderr io.Writer) int {
	return runWithContext(context.Background(), args, stdout, stderr, nil)
}

func runWithContext(ctx context.Context, args []string, stdout, stderr io.Writer, ready chan<- string) int {
	fs := flag.NewFlagSet("s3-lite-object-store", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "127.0.0.1:9000", "TCP listen address")
	dir := fs.String("dir", "", "optional directory path for persistent storage (default in-memory)")
	accessKey := fs.String("access-key", "", "AWS access key ID (required; secret read from S3LITE_SECRET)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintln(stderr, "error: unexpected positional arguments")
		return 2
	}
	if strings.TrimSpace(*accessKey) == "" {
		_, _ = fmt.Fprintln(stderr, "error: -access-key is required")
		return 2
	}
	secret := os.Getenv("S3LITE_SECRET")
	if secret == "" {
		_, _ = fmt.Fprintln(stderr, "error: S3LITE_SECRET environment variable must be set")
		return 2
	}

	var store Store
	if *dir != "" {
		ds, err := NewDirStore(*dir)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "error initializing directory store: %v\n", err)
			return 2
		}
		store = ds
	}

	h, err := New(Config{
		Credentials: map[string]string{*accessKey: secret},
		Store:       store,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "error configuring server: %v\n", err)
		return 2
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "error listening on %s: %v\n", *addr, err)
		return 1
	}

	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
	}

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, _ = fmt.Fprintf(stdout, "listening on %s\n", ln.Addr().String())
	if ready != nil {
		ready <- ln.Addr().String()
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-sigCtx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return 0
	case serveErr := <-errCh:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			_, _ = fmt.Fprintf(stderr, "server error: %v\n", serveErr)
			return 1
		}
		return 0
	}
}

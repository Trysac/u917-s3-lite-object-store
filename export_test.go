package s3liteobjectstore

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func mustNewForTest(t *testing.T) *Server {
	t.Helper()
	srv, err := New(Config{
		Credentials: map[string]string{"TESTKEY0": "test-secret-key-0"},
		Region:      "us-east-1",
	})
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	return srv
}

func MustNewForTest(t *testing.T) *Server {
	return mustNewForTest(t)
}

func TestCLIServerLifecycleInternal(t *testing.T) {
	t.Setenv("S3LITE_SECRET", "test-secret-key-0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan string, 1)
	done := make(chan int, 1)
	var out, errBuf bytes.Buffer
	d := t.TempDir()

	go func() {
		done <- runWithContext(ctx, []string{"-addr", "127.0.0.1:0", "-dir", d, "-access-key", "TESTKEY0"}, &out, &errBuf, ready)
	}()

	select {
	case <-ready:
		cancel()
	case <-time.After(3 * time.Second):
		t.Fatal("CLI server did not become ready in time")
	}

	if code := <-done; code != 0 {
		t.Fatalf("runWithContext exit code = %d, stderr = %s", code, errBuf.String())
	}
}

package sdk

import (
	"bytes"
	"log/slog"
	"testing"
)

// testWriter forwards structured log output into the test log so the key
// decision basis (classification, wait, attempt) is visible in test output.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Logf("[sdk-log] %s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

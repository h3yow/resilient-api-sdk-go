package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestRateLimitBounded verifies that persistent 429s are retried only up to
// MaxAttempts, keep their distinct KindRateLimit classification, and are
// never reported as business errors.
func TestRateLimitBounded(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Retry-After", "0")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"code":"throttled","message":"too many requests"}}`)
	}))
	defer srv.Close()

	policy := RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Retry:      policy,
		Logger:     testLogger(t),
	})
	err := c.DoJSON(context.Background(), http.MethodGet, "/r", nil, nil)

	mu.Lock()
	got := hits
	mu.Unlock()
	if got != 4 {
		t.Fatalf("upstream hits = %d, want exactly MaxAttempts=4 (bounded, not infinite)", got)
	}
	var ae *APIError
	if !errors.As(err, &ae) {
		t.Fatalf("want *APIError, got %T: %v", err, err)
	}
	if ae.Kind != KindRateLimit {
		t.Fatalf("error kind = %s, want rate_limit (must not masquerade as business error)", ae.Kind)
	}
	if !ae.Retryable {
		t.Fatalf("rate-limit error should be classified retryable")
	}
	t.Logf("判定依据: 持续 429 时上游命中 %d 次即停止（有界），最终错误 Kind=%s，未伪装为业务错误", got, ae.Kind)
}

// TestRateLimitHonoursRetryAfter verifies the server hint is used (and
// clamped to MaxDelay), and that one eventual success resolves the call.
func TestRateLimitHonoursRetryAfter(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		if hits < 3 {
			w.Header().Set("Retry-After", "1") // 1 second; must be clamped
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// Pure unit checks on delay math with two distinct policies.
	clamped := RetryPolicy{MaxAttempts: 5, BaseDelay: 500 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	if d := clamped.NextDelay(0, time.Second); d != 20*time.Millisecond {
		t.Fatalf("server hint not clamped: got %s, want 20ms", d)
	}
	t.Logf("判定依据: Retry-After=1s 被钳制到 MaxDelay=20ms（实际 %s）", clamped.NextDelay(0, time.Second))
	backoff := RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 300 * time.Millisecond}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
	for i, w := range want {
		if d := backoff.NextDelay(i, 0); d != w {
			t.Fatalf("backoff attempt %d = %s, want %s", i, d, w)
		}
	}
	t.Log("判定依据: 无提示时退避为 100ms,200ms,400ms→钳至300ms（指数增长且有上限）")

	clientPolicy := RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond}
	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Retry:      clientPolicy,
		Logger:     testLogger(t),
	})
	start := time.Now()
	if err := c.DoJSON(context.Background(), http.MethodGet, "/r", nil, nil); err != nil {
		t.Fatalf("expected success after throttling window, got %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("waited %s, server hint was not clamped to MaxDelay", elapsed)
	}
	mu.Lock()
	got := hits
	mu.Unlock()
	if got != 3 {
		t.Fatalf("upstream hits = %d, want 3 (2 throttled + 1 success)", got)
	}
	t.Logf("判定依据: 两次 429 后第 %d 次成功，总等待 %s（提示已钳制，未无限等待）", got, elapsed)
}

// TestServerErrorsRetried verifies transient 5xx with Retry-After is
// retried, while a 4xx client error fails immediately (non-retryable).
func TestServerErrorsRetried(t *testing.T) {
	var mu sync.Mutex
	hits503 := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits503++
		n := hits503
		mu.Unlock()
		switch r.URL.Path {
		case "/transient":
			if n < 2 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/bad":
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Retry:      RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Logger:     testLogger(t),
	})
	if err := c.DoJSON(context.Background(), http.MethodGet, "/transient", nil, nil); err != nil {
		t.Fatalf("5xx should be retried then succeed: %v", err)
	}
	mu.Lock()
	gotTransient := hits503
	mu.Unlock()
	if gotTransient != 2 {
		t.Fatalf("transient endpoint hits = %d, want 2", gotTransient)
	}
	t.Logf("判定依据: 503 可重试，第 %d 次成功", gotTransient)

	hitsBefore := gotTransient
	err := c.DoJSON(context.Background(), http.MethodGet, "/bad", nil, nil)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Retryable || ae.StatusCode != http.StatusBadRequest {
		t.Fatalf("400 must be a terminal non-retryable error, got %v", err)
	}
	mu.Lock()
	gotBad := hits503 - hitsBefore
	mu.Unlock()
	if gotBad != 1 {
		t.Fatalf("non-retryable 400 produced %d requests, want exactly 1", gotBad)
	}
	t.Logf("判定依据: 400 归类为不可重试，仅请求 %d 次；错误=%v", gotBad, err)
}

// TestRetryDisabled checks MaxAttempts<=1 means no retries at all.
func TestRetryDisabled(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Retry:      RetryPolicy{MaxAttempts: 1},
		Logger:     testLogger(t),
	})
	if err := c.DoJSON(context.Background(), http.MethodGet, "/r", nil, nil); err == nil {
		t.Fatal("expected rate-limit error")
	}
	if hits != 1 {
		t.Fatalf("hits = %d, want 1 with retries disabled", hits)
	}
	t.Logf("判定依据: MaxAttempts=1 时限流也不重试，命中 %d 次", hits)
}

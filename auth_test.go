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

// TestConcurrentRenewalSingleFlight fires N concurrent ValidToken calls
// against a cold manager and asserts the renewer runs exactly once.
func TestConcurrentRenewalSingleFlight(t *testing.T) {
	var (
		mu       sync.Mutex
		renewals int
		started  = make(chan struct{})
		release  = make(chan struct{})
	)
	renewer := func(ctx context.Context) (Token, error) {
		mu.Lock()
		renewals++
		mu.Unlock()
		close(started)
		<-release // hold the renewal so contenders must coalesce
		return Token{AccessToken: "tok-A", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}
	tm := newTokenManager(renewer)

	const n = 25
	var wg sync.WaitGroup
	tokens := make([]string, n)
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			tok, err := tm.ValidToken(context.Background())
			tokens[i], errs[i] = tok, err
		}(i)
	}
	<-started
	time.Sleep(50 * time.Millisecond) // give contenders time to enqueue
	close(release)
	wg.Wait()

	mu.Lock()
	got := renewals
	mu.Unlock()
	if got != 1 {
		t.Fatalf("renewer invocations = %d, want exactly 1 (single-flight)", got)
	}
	t.Logf("判定依据: 25 个并发 ValidToken，renewer 实际调用 %d 次", got)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d got unexpected error: %v", i, err)
		}
		if tokens[i] != "tok-A" {
			t.Fatalf("caller %d got token %q", i, tokens[i])
		}
	}

	// A cached, unexpired token needs no renewal.
	if _, err := tm.ValidToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got = renewals
	mu.Unlock()
	if got != 1 {
		t.Fatalf("cached token triggered extra renewal: invocations = %d", got)
	}
	t.Logf("判定依据: 缓存有效期内再次取令牌，renewer 仍为 %d 次", got)
}

// TestConcurrentRenewalAfterInvalidation ensures that invalidation followed
// by concurrent demand also coalesces to a single renewal.
func TestConcurrentRenewalAfterInvalidation(t *testing.T) {
	var (
		mu       sync.Mutex
		renewals int
	)
	tm := newTokenManager(func(ctx context.Context) (Token, error) {
		mu.Lock()
		renewals++
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		return Token{AccessToken: "tok-B", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	if _, err := tm.ValidToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	tm.Invalidate()

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() { defer wg.Done(); _, _ = tm.ValidToken(context.Background()) }()
	}
	wg.Wait()

	mu.Lock()
	got := renewals
	mu.Unlock()
	if got != 2 { // 1 initial + 1 coalesced
		t.Fatalf("renewer invocations = %d, want 2 (initial + one after invalidation)", got)
	}
	t.Logf("判定依据: 失效后 10 并发取令牌，累计 renewer 调用 %d 次（首轮1 + 单飞1）", got)
}

// TestRenewalFailureNotSuccess asserts that a failing renewal surfaces as
// an auth error wrapping ErrTokenRenewal, and waiters share the failure.
func TestRenewalFailureNotSuccess(t *testing.T) {
	boom := errors.New("idp down")
	var (
		mu      sync.Mutex
		started int
	)
	gate := make(chan struct{})
	release := make(chan struct{})
	tm := newTokenManager(func(ctx context.Context) (Token, error) {
		mu.Lock()
		started++
		mu.Unlock()
		close(gate)
		<-release // hold so concurrent contenders coalesce
		return Token{}, boom
	})

	errs := make([]error, 8)
	ready := make(chan struct{}, 8)
	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			_, errs[i] = tm.ValidToken(context.Background())
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-ready
	}
	// All 8 goroutines are past scheduling gates; release the blocked leader.
	<-gate
	close(release)
	wg.Wait()

	mu.Lock()
	renewals := started
	mu.Unlock()
	if renewals != 1 {
		t.Fatalf("failed renewal invocations = %d, want 1", renewals)
	}
	for i, err := range errs {
		if err == nil {
			t.Fatalf("caller %d: renewal failure was reported as success", i)
		}
		if !errors.Is(err, boom) {
			t.Fatalf("caller %d: error %v does not wrap renewer error", i, err)
		}
	}
	t.Logf("判定依据: 续期失败时 %d 个等待者全部收到失败（非成功），renewer 调用 %d 次", len(errs), renewals)

	// End-to-end through the client: failed renewal => typed auth error and
	// zero requests reach the upstream.
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Renewer:    func(ctx context.Context) (Token, error) { return Token{}, boom },
		Retry:      RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		Logger:     testLogger(t),
	})
	err := c.DoJSON(context.Background(), http.MethodGet, "/x", nil, nil)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Kind != KindAuth {
		t.Fatalf("want *APIError KindAuth, got %v", err)
	}
	if !errors.Is(err, ErrTokenRenewal) {
		t.Fatalf("error %v must wrap ErrTokenRenewal", err)
	}
	if hits != 0 {
		t.Fatalf("upstream hits = %d, want 0 (request must not be sent without token)", hits)
	}
	t.Logf("判定依据: 续期失败 → KindAuth 且包裹 ErrTokenRenewal，上游命中 %d 次；错误=%v", hits, err)
}

// TestOneRenewalBackedRetry verifies a 401 triggers one renewal-backed
// retry that then succeeds, and a persistent 401 does not loop forever.
func TestOneRenewalBackedRetry(t *testing.T) {
	const good = "good-token"
	var (
		mu       sync.Mutex
		hits     int
		badToken = "bad-token"
		renewals int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		auth := r.Header.Get("Authorization")
		mu.Unlock()
		if auth != "Bearer "+good {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":{"code":"invalid_token","message":"token expired"}}`)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Renewer: func(ctx context.Context) (Token, error) {
			mu.Lock()
			renewals++
			mu.Unlock()
			return Token{AccessToken: good, ExpiresAt: time.Now().Add(time.Hour)}, nil
		},
		Retry:  RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Logger: testLogger(t),
	})
	// Seed the manager with the bad token via reflection-free path: send one
	// failed request is not possible pre-construction, so seed directly.
	c.tm.mu.Lock()
	c.tm.token = Token{AccessToken: badToken, ExpiresAt: time.Now().Add(time.Hour)}
	c.tm.mu.Unlock()

	if err := c.DoJSON(context.Background(), http.MethodGet, "/x", nil, nil); err != nil {
		t.Fatalf("expected renewal-backed retry to succeed, got %v", err)
	}
	mu.Lock()
	gotHits, gotRenewals := hits, renewals
	mu.Unlock()
	if gotHits != 2 {
		t.Fatalf("upstream hits = %d, want 2 (reject + retry)", gotHits)
	}
	if gotRenewals != 1 {
		t.Fatalf("renewals = %d, want 1", gotRenewals)
	}
	t.Logf("判定依据: 401 后续期 1 次、上游命中 %d 次后成功", gotHits)
}

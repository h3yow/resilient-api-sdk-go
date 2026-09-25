package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fastRetry 让测试里的退避足够快，同时保留上限语义。
func fastRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

// 限流后恢复：前 2 次 429（带 Retry-After），第 3 次成功；请求次数必须精确。
func TestClient_RateLimitThenSuccess(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n <= 2 {
			w.Header().Set("Retry-After", "0.003")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"code":"rate_limited","message":"slow down"}}`)
			return
		}
		fmt.Fprint(w, `{"value":"ok","server_new_field":"ignored"}`)
	}))
	defer srv.Close()

	c := New(srv.URL, nil, WithRetryPolicy(fastRetry()))
	var out struct {
		Value string `json:"value"`
	}
	if err := c.Get(context.Background(), "/items", &out); err != nil {
		t.Fatal(err)
	}
	if out.Value != "ok" {
		t.Fatalf("响应解析 = %+v", out)
	}
	if got := hits.Load(); got != 3 {
		t.Fatalf("请求次数 = %d, 期望 3（2 次 429 + 1 次成功）", got)
	}
	t.Logf("判定依据: 2 次 429 按 Retry-After 退避后第 3 次成功, 总请求 %d 次, 未知字段被忽略", hits.Load())
}

// 持续限流：重试预算耗尽后必须报 ErrRateLimited，不得无限重试，也不得伪装成业务错误。
func TestClient_RateLimitExhausted(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "0.001")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	policy := fastRetry()
	c := New(srv.URL, nil, WithRetryPolicy(policy))
	err := c.Get(context.Background(), "/items", nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("错误 = %v, 期望 ErrRateLimited", err)
	}
	var ae *APIError
	if !errors.As(err, &ae) || ae.StatusCode != 429 {
		t.Fatalf("应保留 APIError 细节: %v", err)
	}
	if errors.Is(err, ErrNotRetryable) || errors.Is(err, ErrServer) {
		t.Fatalf("限流不得被归类为其他错误: %v", err)
	}
	if got := hits.Load(); int(got) != policy.MaxAttempts {
		t.Fatalf("请求次数 = %d, 期望 %d（预算耗尽即停）", got, policy.MaxAttempts)
	}
	t.Logf("判定依据: 请求 %d 次后停止, 归类 ErrRateLimited 且保留 429 细节", hits.Load())
}

// 401 触发一次续期并重放原请求；并发请求共享同一次续期。
func TestClient_UnauthorizedRenewsAndReplays(t *testing.T) {
	var hits, renewals atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fresh" {
			hits.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		hits.Add(1)
		fmt.Fprint(w, `{"value":"secret"}`)
	}))
	defer srv.Close()

	tm := NewTokenManager(TokenProviderFunc(func(ctx context.Context) (Token, error) {
		renewals.Add(1)
		return Token{Value: "fresh", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}), 0)
	// 预置一个客户端看来未过期、但服务端已吊销的令牌：
	// 首请求带它发出 -> 401 -> 续期 -> 重放。
	tm.token = Token{Value: "stale", ExpiresAt: time.Now().Add(time.Hour)}
	tm.hasTok = true
	c := New(srv.URL, nil, WithRetryPolicy(fastRetry()))
	c.tokens = tm

	var out struct {
		Value string `json:"value"`
	}
	if err := c.Get(context.Background(), "/me", &out); err != nil {
		t.Fatal(err)
	}
	if out.Value != "secret" {
		t.Fatalf("响应 = %+v", out)
	}
	if got := renewals.Load(); got != 1 {
		t.Fatalf("续期次数 = %d, 期望 1", got)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("服务端请求次数 = %d, 期望 2（401 + 重放）", got)
	}
	t.Logf("判定依据: 过期令牌触发 401, 仅续期 %d 次, 重放成功, 服务端共收到 %d 次请求", renewals.Load(), hits.Load())
}

// 续期失败：原请求必须失败且归类为 ErrAuth，不得被当作成功。
func TestClient_RenewalFailureNotSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL, TokenProviderFunc(func(ctx context.Context) (Token, error) {
		return Token{}, errors.New("idp down")
	}), WithRetryPolicy(fastRetry()))

	err := c.Get(context.Background(), "/me", nil)
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("错误 = %v, 期望 ErrAuth", err)
	}
	t.Logf("判定依据: 续期失败, 原请求归类为 ErrAuth: %v", err)
}

// 已移除的能力：410 与显式 deprecated code 都必须归类为 ErrRemoved，不静默降级。
func TestClient_RemovedCapability(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gone":
			w.WriteHeader(http.StatusGone)
		case "/deprecated":
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"error":{"code":"deprecated","message":"use /v2/items"}}`)
		}
	}))
	defer srv.Close()

	c := New(srv.URL, nil, WithRetryPolicy(fastRetry()))
	for _, p := range []string{"/gone", "/deprecated"} {
		err := c.Get(context.Background(), p, nil)
		if !errors.Is(err, ErrRemoved) {
			t.Fatalf("%s 错误 = %v, 期望 ErrRemoved", p, err)
		}
		t.Logf("判定依据: %s 归类为 ErrRemoved: %v", p, err)
	}
}

// 不可重试的 4xx：只请求一次，归类为 ErrNotRetryable。
func TestClient_NotRetryable(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"code":"bad_param","message":"limit must be > 0"}}`)
	}))
	defer srv.Close()

	c := New(srv.URL, nil, WithRetryPolicy(fastRetry()))
	err := c.Get(context.Background(), "/items", nil)
	if !errors.Is(err, ErrNotRetryable) {
		t.Fatalf("错误 = %v, 期望 ErrNotRetryable", err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("请求次数 = %d, 期望 1（不可重试）", got)
	}
	t.Logf("判定依据: 400 仅请求 %d 次, 归类 ErrNotRetryable", hits.Load())
}

// 5xx 可重试但预算有限：耗尽后归类 ErrServer。
func TestClient_ServerErrorExhausted(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	policy := fastRetry()
	c := New(srv.URL, nil, WithRetryPolicy(policy))
	err := c.Get(context.Background(), "/items", nil)
	if !errors.Is(err, ErrServer) {
		t.Fatalf("错误 = %v, 期望 ErrServer", err)
	}
	if got := hits.Load(); int(got) != policy.MaxAttempts {
		t.Fatalf("请求次数 = %d, 期望 %d", got, policy.MaxAttempts)
	}
	t.Logf("判定依据: 502 重试 %d 次后归类 ErrServer", hits.Load())
}

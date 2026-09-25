package sdk

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 并发触发续期：N 个 goroutine 同时拿令牌，provider 只能被调用一次。
func TestTokenManager_ConcurrentRenewal(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	tm := NewTokenManager(TokenProviderFunc(func(ctx context.Context) (Token, error) {
		n := calls.Add(1)
		t.Logf("判定依据: provider 第 %d 次被调用", n)
		<-release // 阻塞续期，迫使所有 goroutine 汇入同一次 flight
		return Token{Value: "tok-1", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}), 0)

	const n = 32
	var wg sync.WaitGroup
	toks := make([]Token, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			toks[i], errs[i] = tm.Valid(context.Background())
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // 让所有 goroutine 都进入等待
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("续期次数 = %d, 期望恰好 1 次", got)
	}
	t.Logf("判定依据: %d 个并发调用共享 1 次续期", n)
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("goroutine %d 出错: %v", i, errs[i])
		}
		if toks[i].Value != "tok-1" {
			t.Fatalf("goroutine %d 拿到令牌 %q, 期望 tok-1", i, toks[i].Value)
		}
	}
}

// 续期失败：所有并发调用方都必须拿到错误，而不是成功。
func TestTokenManager_RenewalFailurePropagates(t *testing.T) {
	renewErr := errors.New("idp unavailable")
	var calls atomic.Int32
	release := make(chan struct{})
	tm := NewTokenManager(TokenProviderFunc(func(ctx context.Context) (Token, error) {
		calls.Add(1)
		<-release // 阻塞续期，确保所有 goroutine 汇入同一次 flight
		return Token{}, renewErr
	}), 0)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = tm.Valid(context.Background())
		}(i)
	}
	time.Sleep(50 * time.Millisecond) // 让所有 goroutine 都进入等待
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("续期次数 = %d, 期望恰好 1 次", got)
	}
	for i, err := range errs {
		if !errors.Is(err, renewErr) {
			t.Fatalf("goroutine %d 错误 = %v, 期望续期失败被传播", i, err)
		}
	}
	t.Logf("判定依据: 续期失败已传播给全部 %d 个等待方", n)
}

// 缓存命中：未过期令牌不应触发续期。
func TestTokenManager_CacheHit(t *testing.T) {
	var calls atomic.Int32
	tm := NewTokenManager(TokenProviderFunc(func(ctx context.Context) (Token, error) {
		calls.Add(1)
		return Token{Value: "cached", ExpiresAt: time.Now().Add(time.Hour)}, nil
	}), 0)
	for i := 0; i < 5; i++ {
		if _, err := tm.Valid(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("provider 调用 %d 次, 期望 1 次（缓存命中）", got)
	}
	tm.Invalidate()
	if _, err := tm.Valid(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Invalidate 后 provider 调用 %d 次, 期望 2 次", got)
	}
	t.Logf("判定依据: 缓存命中 1 次续期, Invalidate 后再续期 1 次")
}

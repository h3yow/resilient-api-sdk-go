package sdk

import (
	"context"
	"sync"
	"time"
)

// Token 是一份带过期时间的访问令牌。
type Token struct {
	Value     string
	ExpiresAt time.Time
}

// Expired 报告令牌是否已过期（含提前量）。
func (t Token) Expired(now time.Time, skew time.Duration) bool {
	return !now.Add(skew).Before(t.ExpiresAt)
}

// TokenProvider 由调用方实现，用于获取/续期令牌。
type TokenProvider interface {
	FetchToken(ctx context.Context) (Token, error)
}

// TokenProviderFunc 让普通函数可作为 TokenProvider 使用。
type TokenProviderFunc func(ctx context.Context) (Token, error)

// FetchToken 实现 TokenProvider。
func (f TokenProviderFunc) FetchToken(ctx context.Context) (Token, error) {
	return f(ctx)
}

// renewal 记录一次进行中的续期及其结果，等待方持有各自指针，避免被下一轮覆盖。
type renewal struct {
	done chan struct{}
	tok  Token
	err  error
}

// TokenManager 缓存令牌并保证并发下只续期一次（singleflight）。
type TokenManager struct {
	provider TokenProvider
	skew     time.Duration

	mu     sync.Mutex
	token  Token
	hasTok bool
	flight *renewal // 非 nil 表示有续期正在进行
}

// NewTokenManager 创建令牌管理器；skew 为过期提前量。
func NewTokenManager(p TokenProvider, skew time.Duration) *TokenManager {
	return &TokenManager{provider: p, skew: skew}
}

// Valid 返回当前可用令牌；失效时触发一次续期，并发调用共享同一次续期结果。
func (m *TokenManager) Valid(ctx context.Context) (Token, error) {
	m.mu.Lock()
	if m.hasTok && !m.token.Expired(time.Now(), m.skew) {
		t := m.token
		m.mu.Unlock()
		return t, nil
	}
	if m.flight != nil {
		// 已有续期在进行：等待其完成并共享结果，保证只续期一次。
		r := m.flight
		m.mu.Unlock()
		select {
		case <-r.done:
			return r.tok, r.err
		case <-ctx.Done():
			return Token{}, ctx.Err()
		}
	}
	// 由当前调用者执行续期。
	r := &renewal{done: make(chan struct{})}
	m.flight = r
	m.mu.Unlock()

	t, err := m.provider.FetchToken(ctx)

	m.mu.Lock()
	if err == nil {
		m.token, m.hasTok = t, true
	}
	r.tok, r.err = t, err
	close(r.done)
	m.flight = nil
	m.mu.Unlock()
	return t, err
}

// Invalidate 使当前缓存令牌失效（例如收到 401 后调用）。
func (m *TokenManager) Invalidate() {
	m.mu.Lock()
	m.hasTok = false
	m.token = Token{}
	m.mu.Unlock()
}

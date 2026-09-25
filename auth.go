package sdk

import (
	"context"
	"sync"
	"time"
)

// expirySkew renews a token slightly early to absorb clock skew and
// in-flight request latency.
const expirySkew = 30 * time.Second

// Token is an access token with an optional absolute expiry time. A zero
// ExpiresAt means the token never expires.
type Token struct {
	AccessToken string
	ExpiresAt   time.Time
}

// TokenRenewer obtains a fresh token. It is called by the SDK only and must
// be safe to call concurrently, although the SDK guarantees that concurrent
// invalidations trigger at most one invocation at a time.
type TokenRenewer func(ctx context.Context) (Token, error)

// renewResult is delivered once to every caller waiting on a shared renewal.
type renewResult struct {
	token Token
	err   error
}

// tokenManager hands out valid tokens and coalesces concurrent renewals
// (single-flight) so that a token invalidation triggers at most one
// renewal at a time.
type tokenManager struct {
	renew TokenRenewer
	now   func() time.Time

	mu       sync.Mutex
	token    Token
	invalid  bool
	renewing bool
	waiters  []chan renewResult
}

func newTokenManager(r TokenRenewer) *tokenManager {
	return &tokenManager{
		renew: r,
		now:   time.Now,
	}
}

// validLocked reports whether the cached token may still be used.
func (m *tokenManager) validLocked() bool {
	if m.invalid || m.token.AccessToken == "" {
		return false
	}
	if m.token.ExpiresAt.IsZero() {
		return true
	}
	return m.now().Add(expirySkew).Before(m.token.ExpiresAt)
}

// ValidToken returns a non-expired access token, renewing it if necessary.
// Concurrent callers observing an invalid token share one renewal call.
func (m *tokenManager) ValidToken(ctx context.Context) (string, error) {
	m.mu.Lock()
	if m.validLocked() {
		t := m.token.AccessToken
		m.mu.Unlock()
		return t, nil
	}
	if m.renewing {
		// Join the in-flight renewal. Buffered so the publisher never blocks
		// on a caller whose context gave up early.
		ch := make(chan renewResult, 1)
		m.waiters = append(m.waiters, ch)
		m.mu.Unlock()
		select {
		case r := <-ch:
			if r.err != nil {
				return "", r.err
			}
			return r.token.AccessToken, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	// Leader performs the renewal.
	m.renewing = true
	m.mu.Unlock()

	token, err := m.renew(ctx)

	m.mu.Lock()
	if err == nil {
		m.token = token
		m.invalid = false
	}
	m.renewing = false
	waiters := m.waiters
	m.waiters = nil
	res := renewResult{token: token, err: err}
	for _, ch := range waiters {
		ch <- res
	}
	m.mu.Unlock()

	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

// Invalidate marks the current token as invalid so the next ValidToken call
// renews it. It is safe for concurrent use.
func (m *tokenManager) Invalidate() {
	m.mu.Lock()
	m.invalid = true
	m.mu.Unlock()
}

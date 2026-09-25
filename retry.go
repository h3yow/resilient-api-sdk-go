package sdk

import "time"

// Default retry bounds. They keep retries finite even when a server keeps
// issuing 429 responses.
const (
	defaultMaxAttempts = 4 // 1 initial try + 3 retries
	defaultBaseDelay   = 500 * time.Millisecond
	defaultMaxDelay    = 10 * time.Second
)

// RetryPolicy configures bounded exponential backoff.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts including the first one.
	// Values <= 1 disable retries.
	MaxAttempts int
	// BaseDelay is the wait before the first retry; subsequent waits grow
	// exponentially from it.
	BaseDelay time.Duration
	// MaxDelay is the upper cap for any single wait. A server Retry-After
	// hint is also clamped to this value so a hostile server cannot force
	// unbounded sleeping.
	MaxDelay time.Duration
}

// DefaultRetryPolicy returns a conservative bounded policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: defaultMaxAttempts,
		BaseDelay:   defaultBaseDelay,
		MaxDelay:    defaultMaxDelay,
	}
}

// NextDelay computes the wait before the next attempt (attempt is the
// zero-based index of the attempt that just failed). When the server gave
// a Retry-After hint it wins over exponential backoff, but the result is
// always clamped to MaxDelay.
func (p RetryPolicy) NextDelay(attempt int, serverHint time.Duration) time.Duration {
	maxDelay := p.MaxDelay
	if maxDelay <= 0 {
		maxDelay = defaultMaxDelay
	}
	clamp := func(d time.Duration) time.Duration {
		if d > maxDelay {
			return maxDelay
		}
		if d < 0 {
			return 0
		}
		return d
	}
	if serverHint > 0 {
		return clamp(serverHint)
	}
	base := p.BaseDelay
	if base <= 0 {
		base = defaultBaseDelay
	}
	if attempt < 0 {
		attempt = 0
	}
	d := base * (1 << attempt) // base * 2^attempt
	return clamp(d)
}

// classifyError reports whether err is retryable and, when the server
// mandated a wait, how long to wait. Only API errors that explicitly
// advertise retryability (transient 5xx / 429) are retried; auth,
// capability and payload failures are terminal.
func classifyError(err error) (retryable bool, retryAfter time.Duration) {
	if err == nil {
		return false, 0
	}
	if ae, ok := isAPIError(err); ok {
		return ae.Retryable, ae.RetryAfter
	}
	return false, 0
}

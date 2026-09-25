// Package sdk provides a reusable API client SDK with unified pagination,
// concurrent-safe token renewal, bounded retries and rate-limit handling.
package sdk

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrorKind classifies failures returned by the SDK so callers can branch
// on the failure category without parsing error strings.
type ErrorKind int

const (
	KindUnknown    ErrorKind = iota
	KindAuth                 // authentication / token failure
	KindRateLimit            // server rejected the request due to rate limiting
	KindCapability           // removed or deprecated server capability
	KindPayload              // response payload / version incompatibility
)

// String returns a stable, human-readable name for the error kind.
func (k ErrorKind) String() string {
	switch k {
	case KindAuth:
		return "auth"
	case KindRateLimit:
		return "rate_limit"
	case KindCapability:
		return "capability"
	case KindPayload:
		return "payload"
	default:
		return "unknown"
	}
}

// APIError is the structured error produced for a failed API call.
type APIError struct {
	Kind       ErrorKind
	StatusCode int
	// Code is the server-provided machine-readable error code, if any.
	Code string
	// Message is the human-readable message.
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Cause      error
}

func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "sdk: %s error", e.Kind)
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, " (http %d)", e.StatusCode)
	}
	if e.Code != "" {
		fmt.Fprintf(&b, " code=%s", e.Code)
	}
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	if e.RetryAfter > 0 {
		fmt.Fprintf(&b, " [retry_after=%s]", e.RetryAfter)
	}
	if e.Cause != nil {
		fmt.Fprintf(&b, ": %v", e.Cause)
	}
	return b.String()
}

// Unwrap exposes the wrapped cause for errors.Is / errors.As.
func (e *APIError) Unwrap() error { return e.Cause }

// CapabilityError reports a server capability that has been removed or
// deprecated. It is deliberately a distinct type so callers cannot mistake
// it for a successful silent downgrade.
type CapabilityError struct {
	Capability string
	Removed    bool
	Deprecated bool
	Cause      error
}

func (e *CapabilityError) Error() string {
	state := "deprecated"
	if e.Removed {
		state = "removed"
	}
	if e.Cause != nil {
		return fmt.Sprintf("sdk: capability %q is %s: %v", e.Capability, state, e.Cause)
	}
	return fmt.Sprintf("sdk: capability %q is %s", e.Capability, state)
}

// Unwrap exposes the wrapped cause.
func (e *CapabilityError) Unwrap() error { return e.Cause }

// Sentinel errors for pagination anomalies and renewal failures.
var (
	// ErrCursorRegression means a next-page cursor pointed at a page that
	// had already been consumed (repeated page) or moved backwards.
	ErrCursorRegression = errors.New("sdk: pagination cursor regressed or repeated a previously seen page")
	// ErrPageSkipped means an observed page ordinal jumped forward, so data
	// could have been omitted.
	ErrPageSkipped = errors.New("sdk: pagination skipped a page number, data may be missing")
	// ErrTokenRenewal wraps a failed token renewal attempt.
	ErrTokenRenewal = errors.New("sdk: token renewal failed")
)

// isAPIError extracts the *APIError carried by err, if any.
func isAPIError(err error) (*APIError, bool) {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}

// parseRetryAfter parses an RFC 7231 Retry-After value, either an integer
// number of seconds or an HTTP-date. A zero duration means absent/invalid.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := time.Parse(time.RFC1123, v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

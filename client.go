package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Config configures a Client.
type Config struct {
	// BaseURL is prepended to request paths.
	BaseURL string
	// HTTPClient defaults to http.DefaultClient.
	HTTPClient *http.Client
	// Renewer obtains access tokens. Required only for endpoints that need
	// authentication.
	Renewer TokenRenewer
	// Retry defaults to DefaultRetryPolicy.
	Retry RetryPolicy
	// APIVersion is sent on every request via the Api-Version header and is
	// also embedded in Accept.
	APIVersion string
	// Logger, when set, receives retry / renewal / classification decisions.
	Logger *slog.Logger
}

// Option mutates a Config.
type Option func(*Config)

// WithHTTPClient overrides the underlying *http.Client.
func WithHTTPClient(h *http.Client) Option { return func(c *Config) { c.HTTPClient = h } }

// WithLogger attaches a structured logger.
func WithLogger(l *slog.Logger) Option { return func(c *Config) { c.Logger = l } }

// Client is the reusable API client.
type Client struct {
	cfg Config
	tm  *tokenManager
}

// New constructs a Client with the given configuration and options.
func New(cfg Config, opts ...Option) *Client {
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry = DefaultRetryPolicy()
	}
	c := &Client{cfg: cfg}
	if cfg.Renewer != nil {
		c.tm = newTokenManager(cfg.Renewer)
	}
	return c
}

// wireEnvelope is the documented server error envelope. It is decoded
// permissively: any unknown fields added by a newer server are ignored.
type wireEnvelope struct {
	Error *struct {
		Code       string `json:"code"`
		Message    string `json:"message"`
		Capability string `json:"capability"`
		Removed    bool   `json:"removed"`
		Deprecated bool   `json:"deprecated"`
	} `json:"error"`
}

// DoJSON performs a JSON request and decodes the JSON response into out.
// It attaches the current bearer token, renews it once on auth failure,
// retries only classified-retryable failures with bounded backoff, and
// returns typed errors for auth / rate-limit / capability / payload
// failures. A successful renewal that is followed by a failing request is
// never reported as success, and a failed renewal is wrapped with
// ErrTokenRenewal.
func (c *Client) DoJSON(ctx context.Context, method, path string, in, out any) error {
	var bodyBytes []byte
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return &APIError{Kind: KindPayload, Message: "encode request body", Cause: err}
		}
		bodyBytes = buf
	}

	maxAttempts := c.cfg.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	authRetried := false
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, method, c.cfg.BaseURL+path, nil)
		if err != nil {
			return &APIError{Kind: KindUnknown, Message: "build request", Cause: err}
		}
		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			req.ContentLength = int64(len(bodyBytes))
			req.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bodyBytes)), nil
			}
		}
		req.Header.Set("Accept", jsonAccept(c.cfg.APIVersion))
		if c.cfg.APIVersion != "" {
			req.Header.Set("Api-Version", c.cfg.APIVersion)
		}
		req.Header.Set("Content-Type", "application/json")
		if c.tm != nil {
			tok, terr := c.tm.ValidToken(ctx)
			if terr != nil {
				return &APIError{
					Kind:      KindAuth,
					Message:   "cannot obtain access token; original request was not sent",
					Retryable: false,
					Cause:     fmt.Errorf("%w: %v", ErrTokenRenewal, terr),
				}
			}
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, doErr := c.cfg.HTTPClient.Do(req)
		if doErr != nil {
			// A cancelled/deadline-exceeded context is terminal.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = &APIError{
				Kind:      KindUnknown,
				Message:   "transport error",
				Retryable: true,
				Cause:     doErr,
			}
		} else {
			lastErr = c.handleResponse(resp, out, attempt)
			resp.Body.Close()
		}

		retryable, retryAfter := classifyError(lastErr)
		if !retryable || attempt == maxAttempts-1 {
			return lastErr
		}

		// Auth failures trigger exactly one renewal-backed retry; the
		// single-flight manager coalesces concurrent renewals. A second
		// auth failure is terminal even though it is marked retryable.
		if ae, ok := isAPIError(lastErr); ok && ae.Kind == KindAuth {
			if c.tm == nil || authRetried {
				return lastErr
			}
			authRetried = true
			c.tm.Invalidate()
		}

		wait := c.cfg.Retry.NextDelay(attempt, retryAfter)
		c.log("sdk retry scheduled",
			"attempt", attempt+1, "max_attempts", maxAttempts,
			"wait", wait.String(), "server_hint", retryAfter.String(),
			"error", safeErrString(lastErr))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

// handleResponse converts one HTTP response into nil (success) or an
// *APIError / *CapabilityError.
func (c *Client) handleResponse(resp *http.Response, out any, attempt int) error {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return &APIError{StatusCode: resp.StatusCode, Kind: KindPayload,
			Message: "read response body", Cause: err}
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(raw) > 0 {
			// Decoder without DisallowUnknownFields: newer servers adding
			// fields must not break older clients.
			if err := json.Unmarshal(raw, out); err != nil {
				return &APIError{StatusCode: resp.StatusCode, Kind: KindPayload,
					Message: "decode success payload", Retryable: false, Cause: err}
			}
		}
		return nil
	}

	// Error path: parse the envelope permissively.
	var env wireEnvelope
	if len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, &env)
	}
	serverHint := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())

	ae := &APIError{StatusCode: resp.StatusCode}
	if env.Error != nil {
		ae.Code = env.Error.Code
		ae.Message = env.Error.Message

		// Removed / deprecated capabilities are explicit, non-retryable,
		// distinguishable failures — never a silent downgrade.
		if env.Error.Capability != "" && (env.Error.Removed || env.Error.Deprecated) {
			return &CapabilityError{
				Capability: env.Error.Capability,
				Removed:    env.Error.Removed,
				Deprecated: env.Error.Deprecated,
			}
		}
	}

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		ae.Kind = KindRateLimit
		ae.Retryable = true
		ae.RetryAfter = serverHint
		if ae.Message == "" {
			ae.Message = "rate limited by server"
		}
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		ae.Kind = KindAuth
		ae.Retryable = true // try exactly one renewal-backed retry
		if ae.Message == "" {
			ae.Message = "authentication failed"
		}
	case resp.StatusCode >= 500:
		ae.Kind = KindUnknown
		ae.Retryable = true
		ae.RetryAfter = serverHint
		if ae.Message == "" {
			ae.Message = "server error"
		}
	default:
		ae.Kind = KindUnknown
		ae.Retryable = false
		if ae.Message == "" {
			ae.Message = "request rejected"
		}
	}
	c.log("sdk response classified",
		"attempt", attempt+1, "status", resp.StatusCode,
		"kind", ae.Kind.String(), "retryable", ae.Retryable,
		"retry_after", ae.RetryAfter.String(), "code", ae.Code)
	return ae
}

func (c *Client) log(msg string, args ...any) {
	if c.cfg.Logger != nil {
		c.cfg.Logger.Info(msg, args...)
	}
}

func jsonAccept(version string) string {
	var b strings.Builder
	b.WriteString("application/json")
	if version != "" {
		b.WriteString("; version=")
		b.WriteString(version)
	}
	return b.String()
}

func safeErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

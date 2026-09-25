package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// Client 是弹性 API 客户端。
type Client struct {
	baseURL string
	http    *http.Client
	tokens  *TokenManager
	retry   RetryPolicy
	// Version 为请求携带的接口版本头；为空则不发送。
	Version string
}

// Option 配置 Client。
type Option func(*Client)

// WithHTTPClient 替换底层 HTTP 客户端。
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithRetryPolicy 替换重试策略。
func WithRetryPolicy(p RetryPolicy) Option {
	return func(c *Client) { c.retry = p }
}

// WithVersion 设置接口版本头。
func WithVersion(v string) Option { return func(c *Client) { c.Version = v } }

// New 创建客户端。provider 可为 nil（不鉴权）。
func New(baseURL string, provider TokenProvider, opts ...Option) *Client {
	c := &Client{
		baseURL: baseURL,
		http:    http.DefaultClient,
		retry:   DefaultRetryPolicy(),
	}
	if provider != nil {
		c.tokens = NewTokenManager(provider, 0)
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Get 执行 GET 请求并把响应体解析进 out（可为 nil）。
// 自动处理：令牌附加与 401 续期重放、限流/5xx 退避重试、失败归类。
func (c *Client) Get(ctx context.Context, path string, out any) error {
	authReplayed := false // 401 续期重放只允许一次，避免死循环
	for attempt := 1; ; attempt++ {
		status, body, hdr, err := c.doOnce(ctx, path)
		if err != nil {
			if errors.Is(err, ErrAuth) {
				// 取令牌失败：续期失败不得被重试或伪装成传输层错误。
				return err
			}
			if attempt >= c.retry.MaxAttempts { // 传输层错误：可重试
				return fmt.Errorf("%w: %v", ErrServer, err)
			}
			if serr := sleep(ctx, c.retry.backoff(attempt, 0)); serr != nil {
				return serr
			}
			continue
		}

		if status >= 200 && status <= 299 {
			if out == nil || len(body) == 0 {
				return nil
			}
			return json.Unmarshal(body, out) // 未知字段默认忽略：向前兼容
		}

		ae := parseAPIError(status, body, hdr)

		// 服务端显式声明能力已移除/废弃：可区分的失败，绝不静默降级。
		if ae.Code == "capability_removed" || ae.Code == "deprecated" {
			return fmt.Errorf("%w: %w", ErrRemoved, ae)
		}

		if status == 401 && c.tokens != nil && !authReplayed {
			// 令牌失效：作废缓存并续期（并发请求共享同一次续期），随后重放原请求。
			c.tokens.Invalidate()
			if _, rerr := c.tokens.Valid(ctx); rerr != nil {
				// 续期失败：原请求不得被当作成功。
				return fmt.Errorf("%w: token renewal failed: %v", ErrAuth, rerr)
			}
			authReplayed = true
			continue
		}

		sentinel, retryable := classify(status)
		if !retryable || attempt >= c.retry.MaxAttempts {
			return fmt.Errorf("%w: %w", sentinel, ae)
		}
		if serr := sleep(ctx, c.retry.backoff(attempt, ae.RetryAfter)); serr != nil {
			return serr
		}
	}
}

// doOnce 执行单次请求（含令牌附加），返回状态码、错误体与传输错误。
func (c *Client) doOnce(ctx context.Context, path string) (status int, body []byte, hdr http.Header, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	if c.Version != "" {
		req.Header.Set(HeaderVersion, c.Version)
	}
	if c.tokens != nil {
		tok, terr := c.tokens.Valid(ctx)
		if terr != nil {
			return 0, nil, nil, fmt.Errorf("%w: %v", ErrAuth, terr)
		}
		req.Header.Set("Authorization", "Bearer "+tok.Value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	if rerr != nil {
		return 0, nil, nil, rerr
	}
	return resp.StatusCode, b, resp.Header, nil
}

// parseAPIError 从错误响应构造 APIError，并提取 Retry-After 提示。
func parseAPIError(status int, body []byte, hdr http.Header) *APIError {
	ae := &APIError{StatusCode: status}
	// 兼容 {"error": {"code","message"}} 与扁平 {"code","message"} 两种形态。
	var envelope struct {
		Err     *APIError `json:"error"`
		Code    string    `json:"code"`
		Message string    `json:"message"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		switch {
		case envelope.Err != nil:
			ae.Code, ae.Message = envelope.Err.Code, envelope.Err.Message
		default:
			ae.Code, ae.Message = envelope.Code, envelope.Message
		}
	}
	if ae.Message == "" {
		ae.Message = http.StatusText(status)
	}
	if ra := hdr.Get("Retry-After"); ra != "" {
		if f, ferr := strconv.ParseFloat(ra, 64); ferr == nil && f > 0 {
			ae.RetryAfter = f
		}
	}
	return ae
}

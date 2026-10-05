package sdk

import (
	"errors"
	"strconv"
)

// 失败归类：调用方可据此区分可重试与不可重试的失败。
var (
	// ErrRateLimited 表示触发服务端限流且重试预算已耗尽。
	ErrRateLimited = errors.New("sdk: rate limited")
	// ErrAuth 表示鉴权失败且令牌续期也失败。
	ErrAuth = errors.New("sdk: authentication failed")
	// ErrRemoved 表示请求的能力已被服务端移除或废弃（如 410 Gone）。
	ErrRemoved = errors.New("sdk: capability removed or deprecated")
	// ErrNotRetryable 表示不可重试的客户端错误（如 400/404/422）。
	ErrNotRetryable = errors.New("sdk: non-retryable request error")
	// ErrServer 表示服务端 5xx 错误且重试预算已耗尽。
	ErrServer = errors.New("sdk: server error")
	// ErrPagination 表示分页遍历出现异常（游标回退、重复页等）。
	ErrPagination = errors.New("sdk: pagination error")
	// ErrPageSequence 表示服务端给出的页序号未按顺序推进（乱序、缺页、跳页）。
	ErrPageSequence = errors.New("sdk: page sequence error")
	// ErrDedupCapacity 表示条目去重键集合达到配置上限且策略为失败。
	ErrDedupCapacity = errors.New("sdk: dedup capacity exhausted")
)

// APIError 是服务端返回的结构化错误。
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RetryAfter float64 // 服务端建议的等待秒数，0 表示未给出
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return "sdk: api error " + strconv.Itoa(e.StatusCode) + " (" + e.Code + "): " + e.Message
	}
	return "sdk: api error " + strconv.Itoa(e.StatusCode) + ": " + e.Message
}

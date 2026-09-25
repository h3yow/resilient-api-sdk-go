package sdk

import (
	"context"
	"time"
)

// RetryPolicy 控制重试次数与退避节奏。
type RetryPolicy struct {
	MaxAttempts int           // 总尝试次数（含首次），<=0 表示不重试
	BaseDelay   time.Duration // 首次退避基数
	MaxDelay    time.Duration // 退避上限（含服务端提示）
}

// DefaultRetryPolicy 返回默认策略。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 4, BaseDelay: 100 * time.Millisecond, MaxDelay: 2 * time.Second}
}

// classify 按状态码归类失败，返回哨兵错误与是否可重试。
func classify(status int) (sentinel error, retryable bool) {
	switch {
	case status == 401:
		return ErrAuth, false // 由令牌续期逻辑处理，不走普通重试
	case status == 410:
		return ErrRemoved, false
	case status == 429:
		return ErrRateLimited, true
	case status >= 500 && status <= 599:
		return ErrServer, true
	case status >= 400 && status <= 499:
		return ErrNotRetryable, false
	default:
		return ErrServer, true
	}
}

// backoff 计算第 attempt 次（从 1 起）重试前的等待时长；
// hint 为服务端给出的等待提示（秒），会被采纳但不超过策略上限。
func (p RetryPolicy) backoff(attempt int, hint float64) time.Duration {
	d := p.BaseDelay << (attempt - 1) // 指数退避
	if hint > 0 {
		h := time.Duration(hint * float64(time.Second))
		if h > d {
			d = h // 采纳服务端提示（取较大者）
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay // 退避上限对服务端提示同样生效
	}
	return d
}

// sleep 等待 d，可被 ctx 中止。
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

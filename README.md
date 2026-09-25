# resilient-api-sdk-go

一个可复用的 Go API 客户端 SDK，为调用方统一处理**分页遍历**、**鉴权令牌自动续期**、
**有界重试**与**服务端限流**，并对**版本兼容**给出明确的成功/失败语义。调用方无需
自行编写重试或令牌续期逻辑，也不依赖任何外部服务或真实凭据。

- 模块路径：`github.com/h3yow/resilient-api-sdk-go`
- Go 版本：1.25
- 仅使用标准库（无第三方依赖）

## 快速开始

```go
client := sdk.New(sdk.Config{
    BaseURL:    "https://api.example.com",
    APIVersion: "2026-09-01",
    Retry:      sdk.DefaultRetryPolicy(),
    Renewer: func(ctx context.Context) (sdk.Token, error) {
        // 由调用方实现：换取新令牌（例如走自己的凭据服务）
        return sdk.Token{AccessToken: "...", ExpiresAt: time.Now().Add(time.Hour)}, nil
    },
})

var out SomePayload
err := client.DoJSON(ctx, http.MethodGet, "/v1/widgets", nil, &out)
```

## 分页约定

分页由 `Iterator[T]` + `PageFetcher[T]` 统一驱动，游标与“下一页链接”使用同一套机制
（链接只是不透明字符串）：

- 抓取函数签名：`func(ctx, cursor string) (Page[T], error)`，**空游标恒表示第一页**。
- `Page.NextCursor == ""` 是约定的**末页标记**；非空值表示下一页游标或绝对链接。
- `Page.PageNumber`（可选，从 1 开始）用于页序号校验；纯游标服务端可置 0。

一致性保证（异常时宁可显式失败，也不静默重复或丢数据）：

| 服务端行为 | SDK 行为 |
| --- | --- |
| 条目跨页重复 | 按稳定 ID 去重，`Iterator.Duplicates` 计数，不重复产出 |
| 下一游标指向**已消费页**（游标回退/成环） | 交付当前页新数据后以 `ErrCursorRegression` 终止，绝不死循环 |
| 页序号**向前跳变**（如 1→3） | 以 `ErrPageSkipped` 终止（可能漏数据） |
| 页序号**倒退**（如 2→1） | 以 `ErrCursorRegression` 终止 |
| 抓取函数返回错误 | 逐字透传并停止遍历 |
| `ctx` 被取消/超时 | 立即中止，`Err()` 返回 `ctx.Err()` |

```go
it := sdk.NewIterator(fetchPage, func(w Widget) string { return w.ID })
for it.Next(ctx) {
    w := it.Item()
    _ = w
}
if err := it.Err(); err != nil { /* 处理分页异常 */ }
```

## 鉴权与令牌续期

- 令牌由调用方提供的 `TokenRenewer` 获取；SDK 在令牌过期前（含 30s 提前量）自动续期。
- **单飞（single-flight）**：令牌失效后并发触发的多个请求只产生**一次**续期，其余请求
  等待并共享同一份结果。
- 请求收到 401/403 时：失效当前令牌并**只做一次**续期支撑的重试；续期后仍鉴权失败则
  立即返回，不再反复续期。
- **续期失败绝不冒充成功**：此时原请求不会发出，返回 `Kind=KindAuth` 的 `*APIError`，
  并以 `errors.Is(err, sdk.ErrTokenRenewal)` 可判定的方式包裹续期失败原因。

## 重试与限流规则

`RetryPolicy{MaxAttempts, BaseDelay, MaxDelay}`（默认见 `DefaultRetryPolicy`）：

- 总尝试次数受 `MaxAttempts` 上限约束（含首次），**任何情况下都不会无限重试**。
- 退避为有界指数退避：`BaseDelay * 2^attempt`，并钳制在 `MaxDelay` 内。
- 服务端 `Retry-After`（支持秒数与 HTTP-date）**优先于**指数退避，但同样被钳制到
  `MaxDelay`，避免恶意服务端强制超长等待。
- 可重试与不可重试严格区分：

| 场景 | 归类 | 是否重试 |
| --- | --- | --- |
| 429 限流 | `*APIError{Kind: KindRateLimit}` | 是，有界 |
| 5xx 瞬时错误 | `*APIError{Retryable: true}` | 是，有界 |
| 401/403 鉴权失败 | `KindAuth` | 仅续期后重试一次 |
| 4xx 业务错误 | `*APIError{Retryable: false}` | 否 |
| 响应体损坏/无法解码 | `KindPayload` | 否 |
| 上下文取消/超时 | 透传 `ctx.Err()` | 否，立即停止 |

限流错误始终保持独立的 `KindRateLimit`，**不会被伪装成业务错误**；超限后最后一次错误
原样返回。可通过 `errors.As(err, &ae)` 读取 `Kind`、`StatusCode`、`Code`、`RetryAfter`。

## 版本与能力兼容

- 请求始终携带 `Api-Version: <Config.APIVersion>` 与
  `Accept: application/json; version=<version>`，服务端可据此应用兼容语义。
- **向前兼容**：成功响应按宽松方式解码（不使用 `DisallowUnknownFields`），服务端新增
  字段不会导致旧客户端解析失败。
- **移除/废弃能力显式失败**：服务端错误信封携带 `capability` 且 `removed`/`deprecated`
  为真时，返回独立的 `*CapabilityError`（`Removed`/`Deprecated` 可区分），**不静默降级**、
  不重试，也不会与普通 `*APIError` 混淆。可用 `errors.As` 判定。

兼容范围：针对 `Config.APIVersion` 日期版本；承诺对同版本“仅新增字段”的服务端变更保持
向前兼容；被移除或显式废弃的能力返回可区分错误而非降级。

## 错误速查

- `errors.As(err, &ae)` → `*sdk.APIError`：`Kind`（`KindAuth`/`KindRateLimit`/
  `KindCapability`/`KindPayload`）、`StatusCode`、`Code`、`Retryable`、`RetryAfter`。
- `errors.As(err, &ce)` → `*sdk.CapabilityError`：`Capability`、`Removed`、`Deprecated`。
- 分页哨兵：`sdk.ErrCursorRegression`、`sdk.ErrPageSkipped`；
  续期：`sdk.ErrTokenRenewal`。

## 本地验证

无需任何外部服务或真实凭据，全部使用 `httptest` 本地模拟服务端：

```bash
go build ./...
go vet ./...
go test -race -count=1 -v ./...
```

关键测试覆盖：

- `auth_test.go`：并发续期单飞（断言 renewer 仅调用 1 次）、失效后并发续期合并、
  续期失败不冒充成功（断言上游命中 0 次、包裹 `ErrTokenRenewal`）、401 仅续期重试一次。
- `retry_test.go`：持续 429 请求次数等于 `MaxAttempts`、`Retry-After` 优先且被钳制、
  5xx 可重试 / 4xx 仅一次、关闭重试。
- `pagination_test.go`：正常混合游标/链接遍历、跨页去重、游标成环、跳页、序号倒退、
  ctx 取消、抓取错误透传。
- `version_test.go`：未知字段不破坏解析、版本头、移除/废弃能力的可区分失败、坏 JSON
  归类为不可重试。
- `integration_test.go`：Client+Iterator 端到端，组合“401 单飞续期 + 429 退避 + 跨页
  去重”，并核对最终数据与请求归类。

测试日志会通过 `t.Logf` 打印关键判定依据（续期次数、命中次数、错误分类、退避时长等），
使用 `-v` 查看。

# resilient-api-sdk-go

可复用的 API 客户端 SDK：统一处理分页遍历、鉴权令牌续期与服务端限流，
让调用方在这些场景下得到一致且可预期的结果，无需自行实现重试与令牌管理。
仅依赖 Go 标准库，不依赖任何外部服务或真实凭据。

```go
c := sdk.New("https://api.example.com", sdk.TokenProviderFunc(fetchToken),
    sdk.WithVersion("2026-01"),
    sdk.WithRetryPolicy(sdk.DefaultRetryPolicy()))

var page sdk.Page[Item] // 配合 Iterator 遍历分页资源
```

## 分页约定

- 服务端分页信封支持两种形态，未知字段一律忽略（向前兼容）：
  - `{"items": [...], "next_cursor": "..."}`
  - `{"items": [...], "next": "<下一页链接或游标>"}`（亦兼容 `next_link`）
- `Next` 为空字符串表示末页。
- `Iterator.NextPage` 逐页推进；`Collect` 一次拉全。
- 异常防护：游标未前进（重复页）或回退到已见过的游标时，返回
  `ErrPagination` 并丢弃该页数据，保证不重复、不遗漏、不死循环。
- 遍历通过 `context.Context` 安全中止；`Collect` 中止时返回已收集的部分数据与错误。

### 分页一致性校验（页序）

- 服务端在分页信息里带出顺序标识时（`seq` / `page_seq` / `offset`，解析到
  `Page.Seq`），`Iterator` 默认校验页面严格逐页递增。
- 发现乱序、缺页或一次跳过多页时，返回 `ErrPageSequence`（与 `ErrPagination`
  可区分），**该页数据不会吐出**，内部状态不推进，调用方可决定重试或放弃。
- 页面不带顺序标识时不校验；`IteratorOptions.DisableSequenceCheck` 可显式关闭。

### 条目级去重与容量取舍

- `IteratorOptions.KeyOf` 提供条目稳定身份串（如记录主键）后，同一条记录
  哪怕换了游标再次出现，最终遍历结果里也只出现一次；`Iterator.Deduped()`
  返回本次遍历累计丢弃的重复条目数。
- 去重存原始身份串（不做哈希压缩），不会把内容不同的记录误判为重复。
- 内存有界：`DedupCapacity` 限制身份集合大小（默认 `DefaultDedupCapacity`
  = 1,000,000，可撑十万级以上结果集）。容量到顶时按 `DedupPolicy` 处理：
  - `DedupFail`（默认）：返回 `ErrDedupCapacity`，绝不悄悄放行可能重复的数据；
  - `DedupEvictOldest`：逐出最早身份继续遍历——被逐出身份的记录若再次出现
    会按约定放行，仅适用于对长尾重复不敏感的场景。
- 单页去重是事务式的：容量到顶时整页不生效，重试不会把已预检的条目误判为重复。

### 断点续传

- `Iterator.Snapshot()` 返回可序列化的 `Checkpoint`（游标、页序、已见游标、
  去重身份集合与计数）；`ResumeIterator(fetch, cp, opts)` 从中恢复。
- 恢复后继续遍历的结果与一口气跑完**完全一致**（不重复、不遗漏，请求次数相同）；
  恢复出的遍历器 `Deduped()` 为全程累计值。恢复时 `opts`（尤其 `KeyOf`、
  容量与策略）需与产生断点时一致。
- 配置 `IteratorOptions.Store` 后每推进一页自动保存断点（先存后提交，
  保存失败则本页推进整体不生效）；也可 `SaveCheckpoint` 显式保存、
  `LoadCheckpoint` 读取。`MemoryCheckpointStore` 为进程内实现。
- 并发安全：`Iterator` 全部方法与 `CheckpointStore` 实现均加锁，
  多个调用方同时遍历、保存、读取断点不会互相踩坏。

## 重试与限流规则

- 重试预算：`RetryPolicy.MaxAttempts` 为总尝试次数（含首次），耗尽即停，绝不无限重试。
- 退避：指数退避 `BaseDelay << (attempt-1)`；服务端 `Retry-After` 提示会被采纳
  （取较大者），但一律不超过 `MaxDelay` 上限。
- 失败归类（`errors.Is` 可判定）：

  | 场景                     | 归类              | 是否重试           |
  |--------------------------|-------------------|--------------------|
  | 429 限流                 | `ErrRateLimited`  | 是，遵循 Retry-After |
  | 5xx / 传输层错误          | `ErrServer`       | 是                 |
  | 401 且续期失败            | `ErrAuth`         | 否                 |
  | 410 / code=deprecated    | `ErrRemoved`      | 否                 |
  | 其他 4xx                 | `ErrNotRetryable` | 否                 |
  | 游标未前进/回退           | `ErrPagination`   | 否                 |
  | 页序错乱/缺页/跳页         | `ErrPageSequence` | 否（状态未推进，可由调用方决定重试） |
  | 去重容量到顶（DedupFail）  | `ErrDedupCapacity`| 否                 |

- 错误链中始终保留 `*APIError`（状态码、服务端 code/message、Retry-After），
  限流错误不会被伪装成业务错误。

## 鉴权与令牌续期

- `TokenManager` 缓存令牌，过期提前量 `skew` 可配。
- 并发安全：令牌失效时，任意多个并发请求只触发**一次**续期（singleflight），
  其余调用方共享该次结果。
- 收到 401 时：作废缓存令牌、续期、并重放原请求一次；续期失败则原请求以
  `ErrAuth` 失败，绝不会被当作成功。

## 版本兼容范围

- 请求头 `X-API-Version` 声明客户端期望的接口版本（`WithVersion`）。
- 服务端**新增字段**：解析时忽略，不会失败。
- 能力**被移除/废弃**：HTTP 410 或错误体 `code` 为 `deprecated` /
  `capability_removed` 时返回 `ErrRemoved`，绝不静默降级。
- `Supports(serverMajor)` 声明当前 SDK 兼容的服务端主版本范围（当前为 1.x）。

## 本地验证

全部测试使用 `httptest` 本地模拟服务端，无需外部服务与真实凭据：

```sh
go vet ./...
go test ./... -race -count=1 -v
```

测试覆盖：并发续期（32 个 goroutine 仅 1 次续期）、续期失败传播、
分页游标重复/回退/中止、页序缺页/乱序/关闭校验、跨游标条目去重与计数、
去重容量到顶（fail / evict 两种策略）、断点恢复与一口气跑的结果等价、
并发保存断点（-race）、限流退避与预算耗尽、401 重放、失败归类；
断言核对请求次数、去重数量与错误归类，日志打印每条判定依据。

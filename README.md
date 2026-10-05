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

## 分页一致性与可恢复

在 `NewIterator` 上按需叠加选项；不启用任何选项时行为与旧版完全一致。

- **页序校验** `WithSequenceCheck()`：信封携带 `page` / `page_index`（整数）
  时解析为页序号。启用后要求序号严格按 +1 推进；乱序、缺页、跳页一律返回
  `ErrPageSequence`（与 `ErrPagination` 可区分），且**该页数据不会吐出**，
  遍历不会当作正常续页继续。不带序号的页不参与校验、不推进基线。
- **条目级去重** `WithDedup(key, maxKeys, policy)`：`key` 返回条目的内容标识
  （如 `id@版本`），同一标识只保留首次出现；换游标后重复出现的记录会被过滤。
  遍历结束后用 `it.Stats().Deduped` 获取本次去掉的重复条目数。
  内容不同的记录（标识不同）不会被误判。
- **容量取舍**：去重键集合占用内存，`maxKeys` 为上限（<=0 表示不设上限，
  内存随结果集增长，需调用方自行评估）。到顶后的行为由策略决定：
  - `DedupFail`（默认）：返回 `ErrDedupCapacity`，显式失败，绝不悄悄漏数据；
  - `DedupWindow`：按 FIFO 淘汰最旧的键，内存严格有界（可撑十万级以上结果集），
    代价是超出窗口的重复条目可能漏判——选择即视为接受该取舍。
- **断点续传**：`it.Checkpoint()` 导出进度快照（可 JSON 序列化持久化），
  用 `WithCheckpoint(cp)` 恢复后继续遍历，结果与一口气跑完**完全一致**
  （含去重判定、页序基线、游标回退检测），既不重复也不遗漏。
  `NextPage`/`Checkpoint`/`Stats` 内部加锁，多个调用方同时保存和推进进度互不干扰。
- 失败归类速查：

  | 场景                       | 归类               |
  |----------------------------|--------------------|
  | 游标未前进 / 回退           | `ErrPagination`    |
  | 页序号乱序 / 缺页 / 跳页     | `ErrPageSequence`  |
  | 去重键集合到顶（DedupFail） | `ErrDedupCapacity` |

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
分页游标重复/回退/中止、页序跳页/回退/正常推进、换游标后的条目去重、
去重容量到顶（Fail/Window 两种策略）、十万级结果集有界去重、
断点快照恢复与一口气跑完的一致性、并发快照（race 检测）、
限流退避与预算耗尽、401 重放、失败归类；
断言核对请求次数、去重数量与错误归类，日志打印关键判定依据。

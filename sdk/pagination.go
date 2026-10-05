package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Page 是分页资源的一页。Next 为服务端返回的游标或下一页链接，为空表示末页。
// HasSeq 为真时 Seq 携带服务端给出的页序号/位置，可用于页序校验。
type Page[T any] struct {
	Items  []T
	Next   string
	Seq    int64
	HasSeq bool
}

// PageFetcher 按游标取一页；cursor 为空表示首页。
type PageFetcher[T any] func(ctx context.Context, cursor string) (Page[T], error)

// Iterator 统一遍历分页资源，游标去重、防回退，可被 ctx 中止。
// 通过 IteratorOption 可叠加页序校验、条目级去重与断点续传。
// NextPage/Stats/Checkpoint 均可被多个 goroutine 安全调用（内部加锁串行化）。
type Iterator[T any] struct {
	fetch PageFetcher[T]
	seen  map[string]struct{} // 已出现过的游标，用于检测重复页/回退
	next  string              // 下一次请求使用的游标
	done  bool

	mu       sync.Mutex         // 保护以下一致性状态，保证并发保存/推进互不干扰
	seqCheck bool               // 是否启用页序校验
	lastSeq  int64              // 最近一个带页序号页面的序号
	hasSeq   bool               // 是否已建立页序基线
	keyFn    func(T) string     // 条目内容标识函数，nil 表示不去重
	maxKeys  int                // 去重键集合上限，<=0 表示不设上限
	policy   DedupOverflowPolicy
	keys     map[string]struct{} // 去重键集合
	order    []string            // 键的插入序，供 DedupWindow FIFO 淘汰与快照恢复
	deduped  int                 // 已丢弃的重复条目数
}

// NewIterator 创建遍历器；opts 可启用页序校验、条目去重、断点恢复等一致性能力。
func NewIterator[T any](fetch PageFetcher[T], opts ...IteratorOption[T]) *Iterator[T] {
	it := &Iterator[T]{fetch: fetch, seen: map[string]struct{}{}}
	for _, opt := range opts {
		opt(it)
	}
	return it
}

// NextPage 返回下一页；没有更多页时 done=true。
// 游标未前进（重复页）或回退到已见过的游标时返回 ErrPagination，且不吐出该页数据。
// 启用页序校验后，页序号未按 +1 推进时返回 ErrPageSequence，同样不吐出该页数据。
// 启用条目去重后，返回页的 Items 已过滤重复条目；去重容量到顶且策略为
// DedupFail 时返回 ErrDedupCapacity。
func (it *Iterator[T]) NextPage(ctx context.Context) (page Page[T], done bool, err error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.done {
		return Page[T]{}, true, nil
	}
	if err := ctx.Err(); err != nil {
		return Page[T]{}, false, err
	}
	cursor := it.next
	page, err = it.fetch(ctx, cursor)
	if err != nil {
		return Page[T]{}, false, err
	}
	// 页序校验先于一切数据吐出：乱序/缺页/跳页时整页丢弃。
	if err := it.checkSeq(page); err != nil {
		return Page[T]{}, false, err
	}
	if page.Next == "" {
		it.done = true // 末页
	} else {
		if page.Next == cursor {
			return Page[T]{}, false, fmt.Errorf("%w: cursor did not advance (still %q)", ErrPagination, cursor)
		}
		if _, dup := it.seen[page.Next]; dup {
			return Page[T]{}, false, fmt.Errorf("%w: cursor regression to %q", ErrPagination, page.Next)
		}
		it.seen[page.Next] = struct{}{}
		it.next = page.Next
	}
	// 条目级去重：同一内容标识只保留首次出现。
	if err := it.dedupPage(&page); err != nil {
		return Page[T]{}, false, err
	}
	return page, false, nil
}

// Collect 拉取全部剩余条目；ctx 取消时安全中止并返回已收集部分与错误。
func Collect[T any](ctx context.Context, it *Iterator[T]) ([]T, error) {
	var out []T
	for {
		page, done, err := it.NextPage(ctx)
		if err != nil {
			return out, err
		}
		if done {
			return out, nil
		}
		out = append(out, page.Items...)
	}
}

// DecodePage 解析服务端分页信封；未知字段被忽略（向前兼容）。
// 支持 {"items": [...], "next_cursor": "..."} 与 {"items": [...], "next": "..."} 两种形态。
// 信封携带 "page" 或 "page_index"（整数）时解析为 Page.Seq 并置 HasSeq，供页序校验使用。
func DecodePage[T any](body []byte) (Page[T], error) {
	var env struct {
		Items      []T    `json:"items"`
		NextCursor string `json:"next_cursor"`
		Next       string `json:"next"`
		NextLink   string `json:"next_link"`
		Page       *int64 `json:"page"`
		PageIndex  *int64 `json:"page_index"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return Page[T]{}, fmt.Errorf("%w: decode page: %v", ErrPagination, err)
	}
	next := env.NextCursor
	if next == "" {
		next = env.Next
	}
	if next == "" {
		next = env.NextLink
	}
	p := Page[T]{Items: env.Items, Next: next}
	if env.Page != nil {
		p.Seq, p.HasSeq = *env.Page, true
	} else if env.PageIndex != nil {
		p.Seq, p.HasSeq = *env.PageIndex, true
	}
	return p, nil
}

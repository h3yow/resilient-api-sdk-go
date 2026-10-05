package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Page 是分页资源的一页。Next 为服务端返回的游标或下一页链接，为空表示末页。
// Seq 为服务端带出的页序号/位置等顺序标识，nil 表示该响应不含顺序信息。
type Page[T any] struct {
	Items []T
	Next  string
	Seq   *int64
}

// PageFetcher 按游标取一页；cursor 为空表示首页。
type PageFetcher[T any] func(ctx context.Context, cursor string) (Page[T], error)

// IteratorOptions 配置遍历的一致性校验、去重与断点行为。
type IteratorOptions[T any] struct {
	// KeyOf 返回条目的稳定身份串（如记录主键）。nil 表示不做条目级去重。
	// 身份串必须能唯一确定一条记录：不同记录不得产生相同身份。
	KeyOf func(T) string
	// DedupCapacity 去重身份集合容量上限，<=0 时使用 DefaultDedupCapacity。
	DedupCapacity int
	// DedupPolicy 容量到顶时的处理策略，零值 DedupFail（失败，不悄悄放行）。
	DedupPolicy DedupPolicy
	// DisableSequenceCheck 关闭页序校验（默认开启：页面携带 Seq 时即校验）。
	DisableSequenceCheck bool
	// Store 非空时，每成功推进一页就把断点保存到该仓库。
	Store CheckpointStore
	// CheckpointName 断点在仓库中的名字，为空时使用 "default"。
	CheckpointName string
}

// Iterator 统一遍历分页资源：游标去重防回退、页序校验、条目级去重、断点续传。
// 全部方法并发安全；多个调用方同时遍历/保存断点不会互相踩坏状态。
type Iterator[T any] struct {
	mu    sync.Mutex
	fetch PageFetcher[T]

	seen map[string]struct{} // 已出现过的游标，用于检测重复页/回退
	next string              // 下一次请求使用的游标
	done bool

	seqCheck bool   // 是否校验页序
	lastSeq  *int64 // 最近一次通过校验的页序号

	dd    *deduper // 条目级去重器，nil 表示不去重
	keyOf func(T) string

	store  CheckpointStore // 非空则每页自动保存断点
	cpName string
}

// NewIterator 创建遍历器（默认开启页序校验，不去重，不自动保存断点）。
func NewIterator[T any](fetch PageFetcher[T]) *Iterator[T] {
	return NewIteratorWithOptions(fetch, IteratorOptions[T]{})
}

// NewIteratorWithOptions 按配置创建遍历器。
func NewIteratorWithOptions[T any](fetch PageFetcher[T], opts IteratorOptions[T]) *Iterator[T] {
	it := &Iterator[T]{
		fetch:    fetch,
		seen:     map[string]struct{}{},
		seqCheck: !opts.DisableSequenceCheck,
		keyOf:    opts.KeyOf,
		store:    opts.Store,
		cpName:   opts.CheckpointName,
	}
	if it.cpName == "" {
		it.cpName = "default"
	}
	if opts.KeyOf != nil {
		it.dd = newDeduper(opts.DedupCapacity, opts.DedupPolicy)
	}
	return it
}

// ResumeIterator 从断点恢复遍历器。恢复后继续遍历的结果与一口气跑完全一致。
// opts 需与产生断点时的配置一致（尤其 KeyOf / DedupCapacity / DedupPolicy）。
func ResumeIterator[T any](fetch PageFetcher[T], cp Checkpoint, opts IteratorOptions[T]) *Iterator[T] {
	it := NewIteratorWithOptions(fetch, opts)
	it.next = cp.Cursor
	it.done = cp.Done
	if cp.LastSeq != nil {
		seq := *cp.LastSeq
		it.lastSeq = &seq
	}
	for _, c := range cp.SeenCursors {
		it.seen[c] = struct{}{}
	}
	if it.dd != nil {
		it.dd.restore(cp.DedupKeys, cp.DedupDropped)
	}
	return it
}

// LoadCheckpoint 从仓库读取并解析断点。
func LoadCheckpoint(ctx context.Context, store CheckpointStore, name string) (Checkpoint, error) {
	if name == "" {
		name = "default"
	}
	data, err := store.Load(ctx, name)
	if err != nil {
		return Checkpoint{}, err
	}
	return decodeCheckpoint(data)
}

// NextPage 返回下一页；没有更多页时 done=true。
// 游标未前进/回退返回 ErrPagination；页序错乱/缺页/跳页返回 ErrPageSequence；
// 去重容量到顶（DedupFail）返回 ErrDedupCapacity。
// 以上失败均不吐出该页数据，且不推进内部状态（调用方可决定重试或放弃）。
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

	// 页序校验：页面携带顺序标识时，必须严格逐页递增。
	if it.seqCheck && page.Seq != nil && it.lastSeq != nil && *page.Seq != *it.lastSeq+1 {
		return Page[T]{}, false, fmt.Errorf("%w: expected seq %d, got %d (cursor %q)",
			ErrPageSequence, *it.lastSeq+1, *page.Seq, cursor)
	}

	// 游标校验：未前进（重复页）或回退到已见过的游标。
	if page.Next != "" {
		if page.Next == cursor {
			return Page[T]{}, false, fmt.Errorf("%w: cursor did not advance (still %q)", ErrPagination, cursor)
		}
		if _, dup := it.seen[page.Next]; dup {
			return Page[T]{}, false, fmt.Errorf("%w: cursor regression to %q", ErrPagination, page.Next)
		}
	}

	// 条目级去重（预检，不产生副作用）。
	var keep []bool
	var newKeys []string
	var dropped int64
	if it.dd != nil {
		keys := make([]string, len(page.Items))
		for i, item := range page.Items {
			keys[i] = it.keyOf(item)
		}
		keep, newKeys, dropped, err = it.dd.plan(keys)
		if err != nil {
			return Page[T]{}, false, err
		}
	}

	// 断点自动保存：先存后提交，保存失败则本次推进整体不生效。
	if it.store != nil {
		cp := it.candidateCheckpoint(page, newKeys, dropped)
		data, err := encodeCheckpoint(cp)
		if err != nil {
			return Page[T]{}, false, err
		}
		if err := it.store.Save(ctx, it.cpName, data); err != nil {
			return Page[T]{}, false, fmt.Errorf("sdk: save checkpoint: %w", err)
		}
	}

	// 提交：页序、游标、去重状态一次性生效。
	if page.Seq != nil {
		seq := *page.Seq
		it.lastSeq = &seq
	}
	if page.Next == "" {
		it.done = true
	} else {
		it.seen[page.Next] = struct{}{}
		it.next = page.Next
	}
	if it.dd != nil {
		it.dd.commit(newKeys, dropped)
		kept := make([]T, 0, len(page.Items))
		for i, item := range page.Items {
			if keep[i] {
				kept = append(kept, item)
			}
		}
		page.Items = kept
	}
	return page, false, nil
}

// candidateCheckpoint 构造「假设本页提交后」的断点快照（调用方需持锁）。
func (it *Iterator[T]) candidateCheckpoint(page Page[T], newKeys []string, dropped int64) Checkpoint {
	cp := Checkpoint{
		Done:         page.Next == "",
		SeenCursors:  it.sortedCursors(),
		DedupDropped: dropped,
	}
	if page.Next == "" {
		cp.Cursor = it.next // 末页后游标不再前进，保留最后位置
	} else {
		cp.Cursor = page.Next
		cp.SeenCursors = append(cp.SeenCursors, page.Next)
	}
	if page.Seq != nil {
		seq := *page.Seq
		cp.LastSeq = &seq
	} else {
		cp.LastSeq = it.lastSeq
	}
	if it.dd != nil {
		cp.DedupKeys = it.dd.keysAfter(newKeys)
		cp.DedupDropped += it.dd.dropped
	}
	return cp
}

// sortedCursors 返回已见游标的有序快照（保证断点内容确定，调用方需持锁）。
func (it *Iterator[T]) sortedCursors() []string {
	out := make([]string, 0, len(it.seen))
	for c := range it.seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Snapshot 返回当前进度的可序列化快照；并发安全。
func (it *Iterator[T]) Snapshot() Checkpoint {
	it.mu.Lock()
	defer it.mu.Unlock()
	cp := Checkpoint{
		Cursor:      it.next,
		Done:        it.done,
		SeenCursors: it.sortedCursors(),
	}
	if it.lastSeq != nil {
		seq := *it.lastSeq
		cp.LastSeq = &seq
	}
	if it.dd != nil {
		cp.DedupKeys = it.dd.keys()
		cp.DedupDropped = it.dd.dropped
	}
	return cp
}

// SaveCheckpoint 主动把当前快照保存到仓库（自动保存之外的显式保存）。
func (it *Iterator[T]) SaveCheckpoint(ctx context.Context, store CheckpointStore, name string) error {
	data, err := encodeCheckpoint(it.Snapshot())
	if err != nil {
		return err
	}
	if name == "" {
		name = "default"
	}
	return store.Save(ctx, name, data)
}

// Deduped 返回本次遍历至今被判定为重复而丢弃的条目数。
func (it *Iterator[T]) Deduped() int64 {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.dd == nil {
		return 0
	}
	return it.dd.dropped
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
// 顺序标识识别 "seq"、"page_seq"、"offset"（均为整数，优先级依次降低）。
func DecodePage[T any](body []byte) (Page[T], error) {
	var env struct {
		Items      []T    `json:"items"`
		NextCursor string `json:"next_cursor"`
		Next       string `json:"next"`
		NextLink   string `json:"next_link"`
		Seq        *int64 `json:"seq"`
		PageSeq    *int64 `json:"page_seq"`
		Offset     *int64 `json:"offset"`
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
	seq := env.Seq
	if seq == nil {
		seq = env.PageSeq
	}
	if seq == nil {
		seq = env.Offset
	}
	return Page[T]{Items: env.Items, Next: next, Seq: seq}, nil
}

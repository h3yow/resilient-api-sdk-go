package sdk

import (
	"fmt"
	"sort"
)

// 分页一致性与可恢复能力：页序校验、条目级去重（有界）、断点续传。
// 本文件只扩展 Iterator 的行为，不改变既有默认语义：
// 不启用任何选项时，Iterator 与之前版本完全一致。

// DedupOverflowPolicy 决定去重键集合达到上限后的行为。
type DedupOverflowPolicy int

const (
	// DedupFail 到顶即返回 ErrDedupCapacity（默认）：绝不漏判重复，也绝不悄悄丢数据。
	DedupFail DedupOverflowPolicy = iota
	// DedupWindow 到顶后按 FIFO 淘汰最旧的键，内存严格有界；
	// 代价是超出窗口的重复条目可能漏判，调用方需显式接受该取舍。
	DedupWindow
)

// Stats 是一次遍历的统计信息。
type Stats struct {
	Deduped int // 被条目级去重丢弃的重复条目数
}

// Checkpoint 是遍历进度快照。可序列化（如 json.Marshal）后持久化，
// 之后用 WithCheckpoint 恢复，恢复结果与一口气跑完完全一致。
type Checkpoint struct {
	Next      string   // 下一次请求使用的游标
	Seen      []string // 已见过的游标（用于回退检测）
	LastSeq   int64    // 最近一个带页序号页面的序号
	HasSeq    bool     // 是否已建立页序基线
	DedupKeys []string // 当前去重键集合（按插入序，有界）
	Deduped   int      // 已累计去重的条目数
	Done      bool     // 是否已遍历完成
}

// IteratorOption 配置 Iterator 的一致性能力。
type IteratorOption[T any] func(*Iterator[T])

// WithSequenceCheck 启用页序校验：当 Page.HasSeq 为真时，
// 要求页序号严格按 +1 推进，否则返回 ErrPageSequence 且不吐出该页数据。
// 不带页序号的页不参与校验，也不推进基线。
func WithSequenceCheck[T any]() IteratorOption[T] {
	return func(it *Iterator[T]) { it.seqCheck = true }
}

// WithDedup 启用条目级去重：key 返回条目的内容标识，同一标识只保留首次出现。
// maxKeys 为去重键集合的内存上限，<=0 表示不设上限（调用方自行承担内存风险）。
// policy 决定到顶后的行为，见 DedupOverflowPolicy。
func WithDedup[T any](key func(T) string, maxKeys int, policy DedupOverflowPolicy) IteratorOption[T] {
	return func(it *Iterator[T]) {
		it.keyFn = key
		it.maxKeys = maxKeys
		it.policy = policy
		it.keys = map[string]struct{}{}
	}
}

// WithCheckpoint 从快照恢复遍历进度。恢复后的遍历结果（含去重判定、
// 页序基线、游标回退检测）与从头一口气跑完完全一致。
func WithCheckpoint[T any](cp Checkpoint) IteratorOption[T] {
	return func(it *Iterator[T]) {
		it.next = cp.Next
		it.done = cp.Done
		it.lastSeq, it.hasSeq = cp.LastSeq, cp.HasSeq
		it.deduped = cp.Deduped
		for _, c := range cp.Seen {
			it.seen[c] = struct{}{}
		}
		if len(cp.DedupKeys) > 0 && it.keys == nil {
			it.keys = map[string]struct{}{}
		}
		for _, k := range cp.DedupKeys {
			it.keys[k] = struct{}{}
			it.order = append(it.order, k)
		}
	}
}

// Stats 返回当前遍历统计（并发安全）。
func (it *Iterator[T]) Stats() Stats {
	it.mu.Lock()
	defer it.mu.Unlock()
	return Stats{Deduped: it.deduped}
}

// Checkpoint 导出当前进度快照（并发安全，深拷贝，序列化结果确定）。
func (it *Iterator[T]) Checkpoint() Checkpoint {
	it.mu.Lock()
	defer it.mu.Unlock()
	seen := make([]string, 0, len(it.seen))
	for c := range it.seen {
		seen = append(seen, c)
	}
	sort.Strings(seen) // 保证序列化结果确定，便于持久化与比对
	return Checkpoint{
		Next:      it.next,
		Seen:      seen,
		LastSeq:   it.lastSeq,
		HasSeq:    it.hasSeq,
		DedupKeys: append([]string(nil), it.order...),
		Deduped:   it.deduped,
		Done:      it.done,
	}
}

// checkSeq 校验页序号严格 +1 推进；仅在启用校验且该页携带序号时生效。
// 调用方须持有 it.mu。
func (it *Iterator[T]) checkSeq(page Page[T]) error {
	if !it.seqCheck || !page.HasSeq {
		return nil
	}
	if it.hasSeq && page.Seq != it.lastSeq+1 {
		return fmt.Errorf("%w: expected page seq %d, got %d", ErrPageSequence, it.lastSeq+1, page.Seq)
	}
	it.lastSeq, it.hasSeq = page.Seq, true
	return nil
}

// dedupPage 就地去重：同一内容标识只保留首次出现，并维护有界键集合。
// 调用方须持有 it.mu。
func (it *Iterator[T]) dedupPage(page *Page[T]) error {
	if it.keyFn == nil {
		return nil
	}
	kept := make([]T, 0, len(page.Items))
	for _, item := range page.Items {
		k := it.keyFn(item)
		if _, dup := it.keys[k]; dup {
			it.deduped++
			continue
		}
		if it.maxKeys > 0 && len(it.keys) >= it.maxKeys {
			switch it.policy {
			case DedupFail:
				return fmt.Errorf("%w: dedup key set reached limit %d", ErrDedupCapacity, it.maxKeys)
			case DedupWindow:
				oldest := it.order[0]
				it.order = it.order[1:]
				delete(it.keys, oldest)
			}
		}
		it.keys[k] = struct{}{}
		it.order = append(it.order, k)
		kept = append(kept, item)
	}
	page.Items = kept
	return nil
}

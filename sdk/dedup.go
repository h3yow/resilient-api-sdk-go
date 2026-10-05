package sdk

import "fmt"

// DefaultDedupCapacity 是去重身份集合的默认容量上限（条目数）。
// 存原始身份串而非哈希，保证不把内容不同的记录误判为重复；
// 内存占用有界（由容量上限决定），可支撑十万级以上结果集。
const DefaultDedupCapacity = 1_000_000

// DedupPolicy 决定去重容量到顶时的处理方式。
type DedupPolicy int

const (
	// DedupFail 容量到顶时以 ErrDedupCapacity 失败，绝不悄悄放行可能重复的数据（默认）。
	DedupFail DedupPolicy = iota
	// DedupEvictOldest 容量到顶时逐出最早记录的身份继续遍历。
	// 注意：被逐出身份的记录若再次出现将无法识别为重复，仅在对长尾重复不敏感的场景使用。
	DedupEvictOldest
)

// deduper 基于调用方提供的身份串做精确去重，容量有界。
// 采用「预检 + 提交」两阶段：单页要么整体生效，要么整体不生效，
// 避免容量到顶时半个页面的身份已入库、重试时被误判为重复而丢数据。
type deduper struct {
	capacity int
	policy   DedupPolicy
	seen     map[string]struct{}
	order    []string // FIFO 逐出队列，仅 DedupEvictOldest 使用
	dropped  int64    // 被判定为重复而丢弃的条目数
}

func newDeduper(capacity int, policy DedupPolicy) *deduper {
	if capacity <= 0 {
		capacity = DefaultDedupCapacity
	}
	return &deduper{
		capacity: capacity,
		policy:   policy,
		seen:     make(map[string]struct{}, min(capacity, 1024)),
	}
}

// plan 只读预检一批身份，不修改任何状态。
// 返回每个位置是否保留（同页内重复、历史重复均判为丢弃）以及本批新增身份。
// 容量到顶且策略为 DedupFail 时返回 ErrDedupCapacity。
func (d *deduper) plan(keys []string) (keep []bool, newKeys []string, dropped int64, err error) {
	keep = make([]bool, len(keys))
	batch := make(map[string]struct{}, len(keys))
	for i, k := range keys {
		if _, ok := d.seen[k]; ok {
			dropped++
			continue
		}
		if _, ok := batch[k]; ok {
			dropped++
			continue
		}
		batch[k] = struct{}{}
		newKeys = append(newKeys, k)
		keep[i] = true
	}
	if len(d.seen)+len(newKeys) > d.capacity && d.policy == DedupFail {
		return nil, nil, 0, fmt.Errorf("%w: capacity %d reached (policy=fail)", ErrDedupCapacity, d.capacity)
	}
	return keep, newKeys, dropped, nil
}

// commit 应用 plan 的结果；DedupEvictOldest 策略下按需逐出最早身份。
func (d *deduper) commit(newKeys []string, dropped int64) {
	for _, k := range newKeys {
		if len(d.seen) >= d.capacity { // 仅 DedupEvictOldest 会走到这里
			oldest := d.order[0]
			d.order = d.order[1:]
			delete(d.seen, oldest)
		}
		d.seen[k] = struct{}{}
		if d.policy == DedupEvictOldest {
			d.order = append(d.order, k)
		}
	}
	d.dropped += dropped
}

// keys 返回当前身份集合的快照（用于断点持久化）。
// DedupEvictOldest 策略下保持 FIFO 顺序，恢复后逐出行为一致。
func (d *deduper) keys() []string {
	if d.policy == DedupEvictOldest {
		return append([]string(nil), d.order...)
	}
	out := make([]string, 0, len(d.seen))
	for k := range d.seen {
		out = append(out, k)
	}
	return out
}

// keysAfter 返回假设 commit(newKeys) 之后的完整身份集合，用于断点保存前的预快照。
func (d *deduper) keysAfter(newKeys []string) []string {
	if d.policy == DedupEvictOldest {
		order := append(append([]string(nil), d.order...), newKeys...)
		if len(order) > d.capacity {
			order = order[len(order)-d.capacity:]
		}
		return order
	}
	out := d.keys()
	return append(out, newKeys...)
}

// restore 从快照恢复身份集合与计数。
func (d *deduper) restore(keys []string, dropped int64) {
	for _, k := range keys {
		d.seen[k] = struct{}{}
	}
	if d.policy == DedupEvictOldest {
		d.order = append(d.order, keys...)
	}
	d.dropped = dropped
}

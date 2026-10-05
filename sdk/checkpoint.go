package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Checkpoint 是分页遍历的可序列化进度快照。
// 从该快照恢复后继续遍历，结果与一口气跑完完全一致（不重复、不遗漏）。
type Checkpoint struct {
	Cursor       string   `json:"cursor"`                 // 下一次请求使用的游标
	Done         bool     `json:"done"`                   // 是否已遍历到末页
	LastSeq      *int64   `json:"last_seq,omitempty"`     // 最近一次通过校验的页序号
	SeenCursors  []string `json:"seen_cursors,omitempty"` // 已见游标（回退检测）
	DedupKeys    []string `json:"dedup_keys,omitempty"`   // 去重身份集合
	DedupDropped int64    `json:"dedup_dropped"`          // 已累计丢弃的重复条目数
}

// CheckpointStore 持久化断点。实现必须并发安全：
// 多个调用方同时保存/读取同一名字时不得互相踩坏数据。
type CheckpointStore interface {
	Save(ctx context.Context, name string, data []byte) error
	Load(ctx context.Context, name string) ([]byte, error)
}

// MemoryCheckpointStore 是进程内 CheckpointStore 实现，互斥锁保护，并发安全。
type MemoryCheckpointStore struct {
	mu    sync.Mutex
	saved map[string][]byte
}

// NewMemoryCheckpointStore 创建内存断点仓库。
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{saved: map[string][]byte{}}
}

func (s *MemoryCheckpointStore) Save(_ context.Context, name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved[name] = append([]byte(nil), data...) // 拷贝，调用方后续修改不影响已存快照
	return nil
}

func (s *MemoryCheckpointStore) Load(_ context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.saved[name]
	if !ok {
		return nil, fmt.Errorf("sdk: checkpoint %q not found", name)
	}
	return append([]byte(nil), data...), nil
}

// encode 序列化断点。
func encodeCheckpoint(cp Checkpoint) ([]byte, error) {
	return json.Marshal(cp)
}

// decodeCheckpoint 反序列化断点。
func decodeCheckpoint(data []byte) (Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: decode checkpoint: %v", ErrPagination, err)
	}
	return cp, nil
}

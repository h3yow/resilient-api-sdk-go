package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
)

// rec 是模拟服务返回的记录，ID 为记录身份。
type rec struct {
	ID  string `json:"id"`
	Val int    `json:"val"`
}

func recIDs(rs []rec) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

// pageServer 用 httptest 起本地模拟服务：按 cursor 返回预设页面，统计请求次数。
type pageServer struct {
	srv     *httptest.Server
	fetches atomic.Int32
}

type serverPage struct {
	items []rec
	next  string
	seq   *int64
}

func seqPtr(v int64) *int64 { return &v }

func newPageServer(t *testing.T, pages map[string]serverPage) *pageServer {
	t.Helper()
	ps := &pageServer{}
	ps.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ps.fetches.Add(1)
		cursor := r.URL.Query().Get("cursor")
		p, ok := pages[cursor]
		if !ok {
			http.Error(w, `{"code":"bad_cursor","message":"unknown cursor"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[`)
		for i, it := range p.items {
			if i > 0 {
				fmt.Fprintf(w, `,`)
			}
			fmt.Fprintf(w, `{"id":%q,"val":%d}`, it.ID, it.Val)
		}
		fmt.Fprintf(w, `],"next_cursor":%q`, p.next)
		if p.seq != nil {
			fmt.Fprintf(w, `,"seq":%d`, *p.seq)
		}
		fmt.Fprintf(w, `}`)
	}))
	t.Cleanup(ps.srv.Close)
	return ps
}

// fetcher 基于真实 HTTP 往返 + DecodePage 构造 PageFetcher。
func (ps *pageServer) fetcher(t *testing.T) PageFetcher[rec] {
	return func(ctx context.Context, cursor string) (Page[rec], error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			ps.srv.URL+"/items?cursor="+url.QueryEscape(cursor), nil)
		if err != nil {
			return Page[rec]{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return Page[rec]{}, err
		}
		defer resp.Body.Close()
		body := make([]byte, 0, 4096)
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			body = append(body, buf[:n]...)
			if err != nil {
				break
			}
		}
		if resp.StatusCode != http.StatusOK {
			return Page[rec]{}, fmt.Errorf("status %d: %s", resp.StatusCode, body)
		}
		return DecodePage[rec](body)
	}
}

// 页序异常：服务端页序号从 1 直接跳到 3（缺第 2 页），必须归类为 ErrPageSequence，
// 且跳页那一页的数据不得吐出，内部状态不得推进。
func TestIterator_PageSequenceSkip(t *testing.T) {
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}}, next: "c1", seq: seqPtr(0)},
		"c1": {items: []rec{{ID: "b"}}, next: "c2", seq: seqPtr(1)},
		"c2": {items: []rec{{ID: "d"}}, next: "c3", seq: seqPtr(3)}, // 缺 seq=2
	})
	it := NewIterator(ps.fetcher(t))
	got, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrPageSequence) {
		t.Fatalf("错误 = %v, 期望 ErrPageSequence", err)
	}
	if ids := recIDs(got); fmt.Sprint(ids) != "[a b]" {
		t.Fatalf("跳页数据被吐出: got %v, 期望仅 [a b]", ids)
	}
	if ps.fetches.Load() != 3 {
		t.Fatalf("请求次数 = %d, 期望 3（首页+c1+c2）", ps.fetches.Load())
	}
	// 状态未推进：重试同一游标仍报同样的错，而不是静默继续。
	_, _, err2 := it.NextPage(context.Background())
	if !errors.Is(err2, ErrPageSequence) {
		t.Fatalf("重试错误 = %v, 期望仍为 ErrPageSequence", err2)
	}
	t.Logf("判定依据: seq 1→3 缺页, 归类 ErrPageSequence: %v; 已吐出 %v 不含跳页数据; 请求 %d 次; 状态未推进重试仍报错",
		err, recIDs(got), ps.fetches.Load())
}

// 页序回退（乱序）：seq 从 2 回到 1，同样归类为 ErrPageSequence。
func TestIterator_PageSequenceOutOfOrder(t *testing.T) {
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}}, next: "c1", seq: seqPtr(1)},
		"c1": {items: []rec{{ID: "b"}}, next: "c2", seq: seqPtr(2)},
		"c2": {items: []rec{{ID: "c"}}, next: "", seq: seqPtr(1)}, // 乱序回退
	})
	it := NewIterator(ps.fetcher(t))
	got, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrPageSequence) {
		t.Fatalf("错误 = %v, 期望 ErrPageSequence", err)
	}
	if ids := recIDs(got); fmt.Sprint(ids) != "[a b]" {
		t.Fatalf("乱序页数据被吐出: got %v, 期望仅 [a b]", ids)
	}
	t.Logf("判定依据: seq 2→1 乱序, 归类 ErrPageSequence: %v; 已吐出 %v; 请求 %d 次",
		err, recIDs(got), ps.fetches.Load())
}

// 页面不带顺序标识时不触发页序校验；显式关闭校验时跳页也不报错（兼容旧行为）。
func TestIterator_SequenceCheckOptional(t *testing.T) {
	// 无 seq：正常遍历。
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}}, next: "c1"},
		"c1": {items: []rec{{ID: "b"}}, next: ""},
	})
	got, err := Collect(context.Background(), NewIterator(ps.fetcher(t)))
	if err != nil || len(got) != 2 {
		t.Fatalf("无 seq 遍历失败: %v, %v", got, err)
	}
	// 有 seq 但显式关闭校验。
	ps2 := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}}, next: "c1", seq: seqPtr(0)},
		"c1": {items: []rec{{ID: "b"}}, next: "", seq: seqPtr(5)},
	})
	it := NewIteratorWithOptions(ps2.fetcher(t), IteratorOptions[rec]{DisableSequenceCheck: true})
	got2, err := Collect(context.Background(), it)
	if err != nil || len(got2) != 2 {
		t.Fatalf("关闭校验后遍历失败: %v, %v", got2, err)
	}
	t.Logf("判定依据: 无 seq 页面不校验（%d 条）; DisableSequenceCheck 时 seq 0→5 也不报错（%d 条）",
		len(got), len(got2))
}

// 条目级去重：游标换了新值但返回内容混入已拉过的记录，最终结果只出现一次，
// Deduped() 报告丢弃的重复条目数。
func TestIterator_DedupAcrossCursorRotation(t *testing.T) {
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}, {ID: "b"}, {ID: "c"}}, next: "c1"},
		"c1": {items: []rec{{ID: "c"}, {ID: "d"}, {ID: "e"}}, next: "c2"}, // c 重复
		"c2": {items: []rec{{ID: "a"}, {ID: "f"}}, next: ""},              // a 重复
	})
	it := NewIteratorWithOptions(ps.fetcher(t), IteratorOptions[rec]{
		KeyOf: func(r rec) string { return r.ID },
	})
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c", "d", "e", "f"}
	if ids := recIDs(got); fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("去重结果 = %v, 期望 %v", ids, want)
	}
	if it.Deduped() != 2 {
		t.Fatalf("去重计数 = %d, 期望 2", it.Deduped())
	}
	if ps.fetches.Load() != 3 {
		t.Fatalf("请求次数 = %d, 期望 3", ps.fetches.Load())
	}
	t.Logf("判定依据: 8 条原始记录中 c/a 各重复 1 次, 结果 %v 无重复, Deduped=%d, 请求 %d 次",
		recIDs(got), it.Deduped(), ps.fetches.Load())
}

// 去重容量到顶（DedupFail）：归类为 ErrDedupCapacity，不悄悄放行、不丢已提交状态。
func TestIterator_DedupCapacityFail(t *testing.T) {
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}, {ID: "b"}}, next: "c1"},
		"c1": {items: []rec{{ID: "c"}, {ID: "d"}}, next: ""}, // 容量 3，第 4 个身份到顶
	})
	it := NewIteratorWithOptions(ps.fetcher(t), IteratorOptions[rec]{
		KeyOf:         func(r rec) string { return r.ID },
		DedupCapacity: 3,
		DedupPolicy:   DedupFail,
	})
	got, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrDedupCapacity) {
		t.Fatalf("错误 = %v, 期望 ErrDedupCapacity", err)
	}
	if ids := recIDs(got); fmt.Sprint(ids) != "[a b]" {
		t.Fatalf("容量到顶的页不应吐出: got %v, 期望仅 [a b]", ids)
	}
	t.Logf("判定依据: 容量 3, 第 2 页需登记 c/d 两个新身份到顶, 归类 ErrDedupCapacity: %v; 已提交页 [a b] 保留",
		err)
}

// 去重容量到顶（DedupEvictOldest）：逐出最早身份继续遍历，内存有界；
// 被逐出身份再次出现时按约定会放行（测试固定该行为）。
func TestIterator_DedupEvictOldest(t *testing.T) {
	ps := newPageServer(t, map[string]serverPage{
		"":   {items: []rec{{ID: "a"}, {ID: "b"}}, next: "c1"},
		"c1": {items: []rec{{ID: "c"}, {ID: "b"}}, next: "c2"}, // b 在窗口内, 去重
		"c2": {items: []rec{{ID: "a"}}, next: ""},              // a 已被逐出, 按约定放行
	})
	it := NewIteratorWithOptions(ps.fetcher(t), IteratorOptions[rec]{
		KeyOf:         func(r rec) string { return r.ID },
		DedupCapacity: 2,
		DedupPolicy:   DedupEvictOldest,
	})
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "b", "c", "a"} // b 去重; a 被逐出后再次出现会放行
	if ids := recIDs(got); fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("结果 = %v, 期望 %v", ids, want)
	}
	if it.Deduped() != 1 {
		t.Fatalf("去重计数 = %d, 期望 1", it.Deduped())
	}
	t.Logf("判定依据: 容量 2 逐出策略, b 在窗口内被去重(Deduped=1), a 被逐出后重现按约定放行, 结果 %v",
		recIDs(got))
}

// 断点恢复：遍历到一半 Snapshot，换新遍历器 Resume，合并结果与一口气跑完全一致。
func TestIterator_CheckpointResumeEquivalence(t *testing.T) {
	pages := map[string]serverPage{
		"":   {items: []rec{{ID: "a"}, {ID: "b"}}, next: "c1", seq: seqPtr(0)},
		"c1": {items: []rec{{ID: "c"}, {ID: "a"}}, next: "c2", seq: seqPtr(1)}, // a 重复
		"c2": {items: []rec{{ID: "d"}, {ID: "e"}}, next: "c3", seq: seqPtr(2)},
		"c3": {items: []rec{{ID: "c"}, {ID: "f"}}, next: "", seq: seqPtr(3)}, // c 重复
	}
	opts := IteratorOptions[rec]{KeyOf: func(r rec) string { return r.ID }}

	// 一口气跑完作为基准。
	ps1 := newPageServer(t, pages)
	it1 := NewIteratorWithOptions(ps1.fetcher(t), opts)
	want, err := Collect(context.Background(), it1)
	if err != nil {
		t.Fatal(err)
	}
	wantDeduped := it1.Deduped()

	// 跑 2 页 → Snapshot → Resume 跑剩余。
	ps2 := newPageServer(t, pages)
	itA := NewIteratorWithOptions(ps2.fetcher(t), opts)
	var head []rec
	for i := 0; i < 2; i++ {
		p, done, err := itA.NextPage(context.Background())
		if err != nil || done {
			t.Fatalf("第 %d 页异常: done=%v err=%v", i, done, err)
		}
		head = append(head, p.Items...)
	}
	cp := itA.Snapshot()
	itB := ResumeIterator(ps2.fetcher(t), cp, opts)
	tail, err := Collect(context.Background(), itB)
	if err != nil {
		t.Fatal(err)
	}
	got := append(head, tail...)

	if fmt.Sprint(recIDs(got)) != fmt.Sprint(recIDs(want)) {
		t.Fatalf("断点恢复结果 = %v, 期望与一口气跑一致 %v", recIDs(got), recIDs(want))
	}
	// 恢复出的遍历器 Deduped() 为全程累计值（含断点前），应与基准一致。
	if total := itB.Deduped(); total != wantDeduped {
		t.Fatalf("恢复后累计去重计数 = %d, 期望 %d", total, wantDeduped)
	}
	if ps2.fetches.Load() != ps1.fetches.Load() {
		t.Fatalf("分段请求次数 = %d, 期望与一口气跑相同 %d", ps2.fetches.Load(), ps1.fetches.Load())
	}
	t.Logf("判定依据: 基准 %v(Deduped=%d), 分段 %v(恢复后累计 Deduped=%d), 请求 %d=%d 次, 结果完全一致",
		recIDs(want), wantDeduped, recIDs(got), itB.Deduped(),
		ps2.fetches.Load(), ps1.fetches.Load())
}

// 断点仓库：每页自动保存，换进程（新遍历器）从仓库 Load 恢复，结果一致。
func TestIterator_CheckpointStoreAutoSave(t *testing.T) {
	pages := map[string]serverPage{
		"":   {items: []rec{{ID: "a"}}, next: "c1", seq: seqPtr(0)},
		"c1": {items: []rec{{ID: "b"}}, next: "c2", seq: seqPtr(1)},
		"c2": {items: []rec{{ID: "c"}}, next: "", seq: seqPtr(2)},
	}
	store := NewMemoryCheckpointStore()
	ps := newPageServer(t, pages)
	opts := IteratorOptions[rec]{
		KeyOf:          func(r rec) string { return r.ID },
		Store:          store,
		CheckpointName: "job-1",
	}
	itA := NewIteratorWithOptions(ps.fetcher(t), opts)
	if _, _, err := itA.NextPage(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 模拟中断后换调用方：从仓库恢复。
	cp, err := LoadCheckpoint(context.Background(), store, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if cp.Cursor != "c1" || cp.LastSeq == nil || *cp.LastSeq != 0 {
		t.Fatalf("断点内容 = %+v, 期望 cursor=c1 seq=0", cp)
	}
	itB := ResumeIterator(ps.fetcher(t), cp, opts)
	tail, err := Collect(context.Background(), itB)
	if err != nil {
		t.Fatal(err)
	}
	if ids := recIDs(tail); fmt.Sprint(ids) != "[b c]" {
		t.Fatalf("恢复后结果 = %v, 期望 [b c]（不重复不遗漏）", ids)
	}
	t.Logf("判定依据: 1 页后自动保存断点 %+v, 恢复后拉到 %v, 与全集 [a b c] 拼接无重复无遗漏",
		cp, recIDs(tail))
}

// 并发安全：多个调用方同时遍历、Snapshot、SaveCheckpoint，互不踩坏（配合 -race）。
func TestIterator_ConcurrentSnapshotAndAdvance(t *testing.T) {
	pages := map[string]serverPage{}
	for i := 0; i < 50; i++ {
		cursor := fmt.Sprintf("c%d", i)
		if i == 0 {
			cursor = ""
		}
		next := fmt.Sprintf("c%d", i+1)
		if i == 49 {
			next = ""
		}
		pages[cursor] = serverPage{
			items: []rec{{ID: fmt.Sprintf("r%03d", i)}},
			next:  next,
			seq:   seqPtr(int64(i)),
		}
	}
	ps := newPageServer(t, pages)
	store := NewMemoryCheckpointStore()
	it := NewIteratorWithOptions(ps.fetcher(t), IteratorOptions[rec]{
		KeyOf: func(r rec) string { return r.ID },
	})

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				cp := it.Snapshot()
				_ = it.SaveCheckpoint(context.Background(), store, fmt.Sprintf("g%d", g))
				_ = cp
			}
		}(g)
	}
	got, err := Collect(context.Background(), it)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("结果条数 = %d, 期望 50", len(got))
	}
	for i, r := range got {
		if r.ID != fmt.Sprintf("r%03d", i) {
			t.Fatalf("第 %d 条 = %s, 期望 r%03d（顺序错乱）", i, r.ID, i)
		}
	}
	// 并发保存的 4 个断点都可读且可解析。
	for g := 0; g < 4; g++ {
		if _, err := LoadCheckpoint(context.Background(), store, fmt.Sprintf("g%d", g)); err != nil {
			t.Fatalf("断点 g%d 不可读: %v", g, err)
		}
	}
	t.Logf("判定依据: 4 个 goroutine 并发 Snapshot/Save 共 120 次, 遍历结果 50 条有序无缺, 断点均可解析")
}

// DecodePage 顺序标识解析：seq / page_seq / offset 三种形态。
func TestDecodePage_SequenceFields(t *testing.T) {
	p1, _ := DecodePage[int]([]byte(`{"items":[1],"next_cursor":"c","seq":7}`))
	p2, _ := DecodePage[int]([]byte(`{"items":[1],"next_cursor":"c","page_seq":8}`))
	p3, _ := DecodePage[int]([]byte(`{"items":[1],"next_cursor":"c","offset":9}`))
	p4, _ := DecodePage[int]([]byte(`{"items":[1],"next_cursor":"c"}`))
	if p1.Seq == nil || *p1.Seq != 7 || p2.Seq == nil || *p2.Seq != 8 || p3.Seq == nil || *p3.Seq != 9 {
		t.Fatalf("顺序标识解析失败: %v %v %v", p1.Seq, p2.Seq, p3.Seq)
	}
	if p4.Seq != nil {
		t.Fatalf("无顺序标识应为 nil, got %v", *p4.Seq)
	}
	t.Logf("判定依据: seq=7 / page_seq=8 / offset=9 均解析到 Seq, 缺省时为 nil")
}

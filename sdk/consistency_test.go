package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
)

// record 模拟业务记录：ID 是服务端记录标识，Rev 是内容版本。
// 去重键取 "ID@Rev"：同一条记录（同 ID 同内容）才算重复，
// 同 ID 但内容已变（Rev 不同）的记录不得误判为重复。
type record struct {
	ID  string `json:"id"`
	Rev int    `json:"rev"`
}

func recordKey(r record) string { return fmt.Sprintf("%s@%d", r.ID, r.Rev) }

// newPageServer 启动本地模拟分页服务：按 cursor 查询参数返回预置的 JSON 信封，
// 并统计请求次数。未知游标返回 404。
func newPageServer(t *testing.T, pages map[string]string, fetches *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		cursor := r.URL.Query().Get("cursor")
		body, ok := pages[cursor]
		if !ok {
			http.Error(w, `{"code":"unknown_cursor"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fetchFrom 构造打到本地模拟服务的 PageFetcher。
func fetchFrom(base string, fetchesOK *atomic.Int32) PageFetcher[record] {
	return func(ctx context.Context, cursor string) (Page[record], error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/items?cursor="+url.QueryEscape(cursor), nil)
		if err != nil {
			return Page[record]{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return Page[record]{}, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return Page[record]{}, err
		}
		if resp.StatusCode != http.StatusOK {
			return Page[record]{}, fmt.Errorf("status %d: %s", resp.StatusCode, body)
		}
		return DecodePage[record](body)
	}
}

// 页序异常：页序号跳页（1,2,5）必须归类为 ErrPageSequence，
// 且乱序页的数据不得吐出，也不能继续当正常续页。
func TestSequenceCheck_PageGap(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"":   `{"items":[{"id":"a","rev":1}],"next_cursor":"c1","page":1}`,
		"c1": `{"items":[{"id":"b","rev":1}],"next_cursor":"c2","page":2}`,
		"c2": `{"items":[{"id":"LOST","rev":1}],"next_cursor":"","page":5}`, // 3、4 页丢失
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches), WithSequenceCheck[record]())
	var got []record
	var seqErr error
	for {
		page, done, err := it.NextPage(context.Background())
		if err != nil {
			seqErr = err
			break
		}
		if done {
			break
		}
		got = append(got, page.Items...)
	}
	if !errors.Is(seqErr, ErrPageSequence) {
		t.Fatalf("错误 = %v, 期望 ErrPageSequence", seqErr)
	}
	if errors.Is(seqErr, ErrPagination) {
		t.Fatalf("ErrPageSequence 必须与 ErrPagination 可区分, 实际错误链同时匹配两者: %v", seqErr)
	}
	for _, r := range got {
		if r.ID == "LOST" {
			t.Fatalf("乱序页的数据不应吐出, 实际结果含 %v", r)
		}
	}
	if len(got) != 2 {
		t.Fatalf("跳页前已收集 %d 条, 期望 2 条", len(got))
	}
	if fetches.Load() != 3 {
		t.Fatalf("请求次数 = %d, 期望 3（第 3 页抓取后才发现跳页）", fetches.Load())
	}
	t.Logf("判定依据: 页序 1,2,5 -> 期望 3 实得 5, 归类 ErrPageSequence; 已收集 %v 不含乱序页; 请求 %d 次",
		got, fetches.Load())
}

// 页序异常：页序号回退（1,2,2）同样归类为 ErrPageSequence。
func TestSequenceCheck_PageOutOfOrder(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"":   `{"items":[{"id":"a","rev":1}],"next_cursor":"c1","page":1}`,
		"c1": `{"items":[{"id":"b","rev":1}],"next_cursor":"c2","page":2}`,
		"c2": `{"items":[{"id":"c","rev":1}],"next_cursor":"","page":2}`, // 序号回退
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches), WithSequenceCheck[record]())
	_, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrPageSequence) {
		t.Fatalf("错误 = %v, 期望 ErrPageSequence", err)
	}
	t.Logf("判定依据: 页序 1,2,2 -> 期望 3 实得 2, 归类 ErrPageSequence: %v", err)
}

// 页序正常推进时不应误报。
func TestSequenceCheck_InOrder(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"":   `{"items":[{"id":"a","rev":1}],"next_cursor":"c1","page":1}`,
		"c1": `{"items":[{"id":"b","rev":1}],"next_cursor":"c2","page":2}`,
		"c2": `{"items":[{"id":"c","rev":1}],"next_cursor":"","page":3}`,
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches), WithSequenceCheck[record]())
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatalf("顺序正常不应报错: %v", err)
	}
	if len(got) != 3 || fetches.Load() != 3 {
		t.Fatalf("结果 %d 条 / 请求 %d 次, 期望 3 条 / 3 次", len(got), fetches.Load())
	}
	t.Logf("判定依据: 页序 1,2,3 严格 +1, 遍历正常完成, 结果 %v", got)
}

// 条目级去重：服务端换游标后返回了已拉过的记录，最终只出现一次；
// 内容确实不同的记录（同 ID 不同 Rev）不得误判。
func TestDedup_AcrossCursorRotation(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		// 第一页：a@1, b@1
		"": `{"items":[{"id":"a","rev":1},{"id":"b","rev":1}],"next_cursor":"c1"}`,
		// 数据更新后换了新游标，但混入了已拉过的 b@1；a@2 是同 ID 新内容，必须保留
		"c1": `{"items":[{"id":"b","rev":1},{"id":"a","rev":2},{"id":"c","rev":1}],"next_cursor":""}`,
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches),
		WithDedup(recordKey, 0, DedupFail))
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	want := []record{{"a", 1}, {"b", 1}, {"a", 2}, {"c", 1}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("遍历结果 = %v, 期望 %v", got, want)
	}
	if d := it.Stats().Deduped; d != 1 {
		t.Fatalf("去重数 = %d, 期望 1（仅 b@1 重复）", d)
	}
	t.Logf("判定依据: b@1 两次出现去 1 条; a@2 与 a@1 内容不同均保留; 结果 %v, 去重 %d 条, 请求 %d 次",
		got, it.Stats().Deduped, fetches.Load())
}

// 容量到顶（DedupFail）：第 3 个不同的键触发 ErrDedupCapacity，必须显式失败而非悄悄漏数据。
func TestDedup_CapacityFail(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"":   `{"items":[{"id":"a","rev":1},{"id":"b","rev":1}],"next_cursor":"c1"}`,
		"c1": `{"items":[{"id":"c","rev":1}],"next_cursor":""}`,
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches),
		WithDedup(recordKey, 2, DedupFail))
	_, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrDedupCapacity) {
		t.Fatalf("错误 = %v, 期望 ErrDedupCapacity", err)
	}
	if fetches.Load() != 2 {
		t.Fatalf("请求次数 = %d, 期望 2（第 2 页处理中到顶即停）", fetches.Load())
	}
	t.Logf("判定依据: 上限 2 个键, 第 3 个新键 c@1 触发容量失败, 归类 ErrDedupCapacity: %v; 请求 %d 次",
		err, fetches.Load())
}

// 容量到顶（DedupWindow）：内存严格有界，窗口内重复可去，窗口外重复按约定漏判。
func TestDedup_CapacityWindow(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"": `{"items":[{"id":"a","rev":1},{"id":"b","rev":1}],"next_cursor":"c1"}`,
		// b@1 仍在窗口内 -> 去重；加入 c@1 时淘汰最旧的 a@1
		"c1": `{"items":[{"id":"b","rev":1},{"id":"c","rev":1}],"next_cursor":"c2"}`,
		// a@1 已被淘汰 -> 按约定视为新条目（调用方接受的取舍）
		"c2": `{"items":[{"id":"a","rev":1}],"next_cursor":""}`,
	}, &fetches)

	it := NewIterator(fetchFrom(srv.URL, &fetches),
		WithDedup(recordKey, 2, DedupWindow))
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	want := []record{{"a", 1}, {"b", 1}, {"c", 1}, {"a", 1}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("遍历结果 = %v, 期望 %v（窗口外 a@1 按约定漏判）", got, want)
	}
	if d := it.Stats().Deduped; d != 1 {
		t.Fatalf("去重数 = %d, 期望 1（仅窗口内的 b@1）", d)
	}
	t.Logf("判定依据: 窗口=2, b@1 在窗口内被去重; a@1 已被 FIFO 淘汰, 再次出现按约定放行; 去重 %d 条", it.Stats().Deduped)
}

// 断点续传：遍历到一半保存快照（经 JSON 序列化模拟持久化），
// 恢复后的结果与一口气跑完完全一致，既不重复也不遗漏。
func TestCheckpoint_ResumeMatchesOneShot(t *testing.T) {
	pages := map[string]string{
		"":   `{"items":[{"id":"a","rev":1},{"id":"b","rev":1}],"next_cursor":"c1","page":1}`,
		"c1": `{"items":[{"id":"c","rev":1}],"next_cursor":"c2","page":2}`,
		"c2": `{"items":[{"id":"b","rev":1},{"id":"d","rev":1}],"next_cursor":"c3","page":3}`, // b@1 重复
		"c3": `{"items":[{"id":"e","rev":1}],"next_cursor":"","page":4}`,
	}
	opts := func() []IteratorOption[record] {
		return []IteratorOption[record]{WithSequenceCheck[record](), WithDedup(recordKey, 0, DedupFail)}
	}

	// 一口气跑完
	var fetches1 atomic.Int32
	srv1 := newPageServer(t, pages, &fetches1)
	it1 := NewIterator(fetchFrom(srv1.URL, &fetches1), opts()...)
	want, err := Collect(context.Background(), it1)
	if err != nil {
		t.Fatal(err)
	}
	wantDeduped := it1.Stats().Deduped

	// 跑 2 页后保存快照，序列化后恢复继续
	var fetches2 atomic.Int32
	srv2 := newPageServer(t, pages, &fetches2)
	it2 := NewIterator(fetchFrom(srv2.URL, &fetches2), opts()...)
	var got []record
	for i := 0; i < 2; i++ {
		page, done, err := it2.NextPage(context.Background())
		if err != nil || done {
			t.Fatalf("前 2 页不应结束或报错: done=%v err=%v", done, err)
		}
		got = append(got, page.Items...)
	}
	blob, err := json.Marshal(it2.Checkpoint())
	if err != nil {
		t.Fatal(err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(blob, &cp); err != nil {
		t.Fatal(err)
	}
	t.Logf("快照内容: %s", blob)

	// 从快照恢复（模拟另一个进程/另一时刻）
	var fetches3 atomic.Int32
	srv3 := newPageServer(t, pages, &fetches3)
	it3 := NewIterator(fetchFrom(srv3.URL, &fetches3),
		append(opts(), WithCheckpoint[record](cp))...)
	rest, err := Collect(context.Background(), it3)
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, rest...)

	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("恢复遍历结果 = %v, 期望与一口气跑完一致 %v", got, want)
	}
	if d := it2.Stats().Deduped + it3.Stats().Deduped; d != wantDeduped {
		t.Fatalf("恢复后累计去重 = %d, 期望 %d", d, wantDeduped)
	}
	if fetches2.Load()+fetches3.Load() != fetches1.Load() {
		t.Fatalf("恢复遍历总请求 = %d+%d, 期望与一口气 %d 次一致", fetches2.Load(), fetches3.Load(), fetches1.Load())
	}
	t.Logf("判定依据: 恢复结果 %v 与一口气完全一致; 去重 %d=%d; 请求 %d+%d=%d 次, 不重复不遗漏",
		got, it2.Stats().Deduped+it3.Stats().Deduped, wantDeduped,
		fetches2.Load(), fetches3.Load(), fetches1.Load())
}

// 断点续传：遍历完成后的快照恢复出来应立即 done，不再发请求。
func TestCheckpoint_ResumeFromDone(t *testing.T) {
	var fetches atomic.Int32
	srv := newPageServer(t, map[string]string{
		"": `{"items":[{"id":"a","rev":1}],"next_cursor":""}`,
	}, &fetches)
	it := NewIterator(fetchFrom(srv.URL, &fetches))
	if _, err := Collect(context.Background(), it); err != nil {
		t.Fatal(err)
	}
	cp := it.Checkpoint()
	if !cp.Done {
		t.Fatal("遍历完成后快照应标记 Done")
	}
	before := fetches.Load()
	it2 := NewIterator(fetchFrom(srv.URL, &fetches), WithCheckpoint[record](cp))
	got, err := Collect(context.Background(), it2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || fetches.Load() != before {
		t.Fatalf("从已完成快照恢复不应再拉数据: 新增 %d 条 / 新增请求 %d 次", len(got), fetches.Load()-before)
	}
	t.Logf("判定依据: Done 快照恢复后 0 条数据 0 次请求, 不重复拉取")
}

// 并发安全：多个调用方同时保存快照、另一个在推进遍历，互不干扰（race 检测 + 快照可用）。
func TestCheckpoint_ConcurrentSnapshot(t *testing.T) {
	pages := map[string]string{}
	for i := 0; i < 50; i++ {
		cursor := fmt.Sprintf("c%d", i)
		if i == 0 {
			cursor = ""
		}
		next := fmt.Sprintf("c%d", i+1)
		if i == 49 {
			next = ""
		}
		pages[cursor] = fmt.Sprintf(`{"items":[{"id":"r%d","rev":1}],"next_cursor":%q,"page":%d}`, i, next, i+1)
	}
	var fetches atomic.Int32
	srv := newPageServer(t, pages, &fetches)
	it := NewIterator(fetchFrom(srv.URL, &fetches),
		WithSequenceCheck[record](), WithDedup(recordKey, 0, DedupFail))

	var wg sync.WaitGroup
	snaps := make(chan Checkpoint, 8)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				snaps <- it.Checkpoint() // 并发保存快照
			}
		}()
	}
	go func() { wg.Wait(); close(snaps) }()

	got, err := Collect(context.Background(), it) // 主 goroutine 推进遍历
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 {
		t.Fatalf("并发快照下遍历结果 = %d 条, 期望 50", len(got))
	}
	// 所有并发快照都必须自洽：页序基线单调有界；Done 快照的页序基线已到末页。
	// （Done=true 时 Next 保留最后使用的游标属正常，恢复时 Done 优先，不再发请求。）
	count := 0
	for cp := range snaps {
		count++
		if cp.HasSeq && (cp.LastSeq < 1 || cp.LastSeq > 50) {
			t.Fatalf("快照页序基线异常: %d", cp.LastSeq)
		}
		if cp.Done && cp.LastSeq != 50 {
			t.Fatalf("Done 快照页序基线应为末页 50, 实际 %d", cp.LastSeq)
		}
		if len(cp.DedupKeys) != int(cp.LastSeq) {
			t.Fatalf("快照去重键数 %d 与已推进页数 %d 不一致", len(cp.DedupKeys), cp.LastSeq)
		}
	}
	t.Logf("判定依据: 4 个 goroutine 共 %d 次并发快照全部自洽, 遍历 50 条完整, race 检测通过", count)
}

// DecodePage：页序号字段 page / page_index 均能解析并置 HasSeq。
func TestDecodePage_Sequence(t *testing.T) {
	p1, err := DecodePage[int]([]byte(`{"items":[1],"next_cursor":"c1","page":3}`))
	if err != nil || !p1.HasSeq || p1.Seq != 3 {
		t.Fatalf("page 字段解析失败: %+v, %v", p1, err)
	}
	p2, err := DecodePage[int]([]byte(`{"items":[2],"next_cursor":"","page_index":7}`))
	if err != nil || !p2.HasSeq || p2.Seq != 7 {
		t.Fatalf("page_index 字段解析失败: %+v, %v", p2, err)
	}
	p3, err := DecodePage[int]([]byte(`{"items":[3],"next_cursor":""}`))
	if err != nil || p3.HasSeq {
		t.Fatalf("无序号字段时 HasSeq 应为 false: %+v, %v", p3, err)
	}
	t.Logf("判定依据: page -> Seq=%d, page_index -> Seq=%d, 无字段 -> HasSeq=false", p1.Seq, p2.Seq)
}

// 规模验证：十万级条目、有界去重窗口下遍历结果精确（不重复不遗漏），
// 且去重键集合始终不超过窗口上限（内存有界）。
func TestDedup_Scale100K(t *testing.T) {
	const (
		pages    = 1000
		pageSize = 100 // 共 100,000 条
		window   = 5000
	)
	// 每页混入少量 5 页之前的记录（在窗口内，应被去重）。
	fetch := func(ctx context.Context, cursor string) (Page[record], error) {
		idx := 0
		if cursor != "" {
			fmt.Sscanf(cursor, "c%d", &idx)
		}
		items := make([]record, 0, pageSize)
		for i := 0; i < pageSize; i++ {
			items = append(items, record{ID: fmt.Sprintf("r%d", idx*pageSize+i), Rev: 1})
		}
		if idx >= 5 {
			for i := 0; i < 3; i++ { // 3 条旧记录重复出现
				items = append(items, record{ID: fmt.Sprintf("r%d", (idx-5)*pageSize+i), Rev: 1})
			}
		}
		next := fmt.Sprintf("c%d", idx+1)
		if idx == pages-1 {
			next = ""
		}
		return Page[record]{Items: items, Next: next, Seq: int64(idx + 1), HasSeq: true}, nil
	}

	it := NewIterator(fetch,
		WithSequenceCheck[record](),
		WithDedup(recordKey, window, DedupWindow))
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	wantDeduped := 3 * (pages - 5)
	if len(got) != pages*pageSize {
		t.Fatalf("结果 = %d 条, 期望 %d（重复应全部去除）", len(got), pages*pageSize)
	}
	if d := it.Stats().Deduped; d != wantDeduped {
		t.Fatalf("去重数 = %d, 期望 %d", d, wantDeduped)
	}
	// 校验结果无重复（调用方视角的最终保证）。
	seen := make(map[string]struct{}, len(got))
	for _, r := range got {
		k := recordKey(r)
		if _, dup := seen[k]; dup {
			t.Fatalf("结果中存在重复条目 %v", r)
		}
		seen[k] = struct{}{}
	}
	// 快照中的键集合不超过窗口上限：内存有界的直接证据。
	if n := len(it.Checkpoint().DedupKeys); n > window {
		t.Fatalf("去重键集合 = %d, 超过窗口上限 %d", n, window)
	}
	t.Logf("判定依据: %d 页 x %d 条 = %d 条全部保留; %d 条窗口内重复全部去除; 键集合峰值 <= %d, 内存有界",
		pages, pageSize, len(got), wantDeduped, window)
}

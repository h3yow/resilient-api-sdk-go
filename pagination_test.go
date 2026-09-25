package sdk

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type thing struct{ ID string }

func thingID(t thing) string { return t.ID }

// TestPaginationHappyPath walks a 3-page resource and verifies order and
// fetch counts for mixed cursor / next-link pagination.
func TestPaginationHappyPath(t *testing.T) {
	pages := map[string]Page[thing]{
		"":                             {Items: []thing{{"a"}, {"b"}}, NextCursor: "c1", PageNumber: 1},
		"c1":                           {Items: []thing{{"c"}}, NextCursor: "https://api.example/n?page=2", PageNumber: 2},
		"https://api.example/n?page=2": {Items: []thing{{"d"}, {"e"}}, NextCursor: "", PageNumber: 3},
	}
	var fetches int32
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		atomic.AddInt32(&fetches, 1)
		p, ok := pages[cursor]
		if !ok {
			t.Errorf("unexpected cursor %q", cursor)
			return Page[thing]{}, errors.New("bad cursor")
		}
		return p, nil
	}

	it := NewIterator(fetch, thingID)
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatalf("happy path returned error: %v", err)
	}
	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("items = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("item %d = %q, want %q (order preserved)", i, got[i].ID, want[i])
		}
	}
	if atomic.LoadInt32(&fetches) != 3 {
		t.Fatalf("fetches = %d, want 3", fetches)
	}
	t.Logf("判定依据: 游标+下一页链接混合，3 次抓取按序得到 %v，无重复无遗漏", want)
}

// TestPaginationRepeatedItemsDedup verifies that a server repeating items
// across pages does not duplicate them, while traversal completes cleanly.
func TestPaginationRepeatedItemsDedup(t *testing.T) {
	pages := []Page[thing]{
		{Items: []thing{{"a"}, {"b"}}, NextCursor: "c1"},
		{Items: []thing{{"b"}, {"c"}}, NextCursor: "c2"}, // b repeated
		{Items: []thing{{"c"}}, NextCursor: ""},          // c repeated, final
	}
	var fetches int32
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		n := int(atomic.AddInt32(&fetches, 1)) - 1
		if n >= len(pages) {
			return Page[thing]{}, errors.New("unexpected extra fetch")
		}
		return pages[n], nil
	}

	it := NewIterator(fetch, thingID)
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatalf("dedup traversal should finish cleanly: %v", err)
	}
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Fatalf("got %v, want [a b c]", got)
	}
	if it.Duplicates != 2 {
		t.Fatalf("Duplicates counter = %d, want 2", it.Duplicates)
	}
	t.Logf("判定依据: 跨页重复条目 b/c 被抑制 %d 次，输出 [a b c] 无重复", it.Duplicates)
}

// TestPaginationCursorLoop verifies that a next cursor pointing at an
// already consumed page is rejected with ErrCursorRegression instead of an
// infinite loop or silent truncation.
func TestPaginationCursorLoop(t *testing.T) {
	var fetches int32
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		atomic.AddInt32(&fetches, 1)
		switch cursor {
		case "":
			return Page[thing]{Items: []thing{{"a"}}, NextCursor: "c1"}, nil
		case "c1":
			// Self-loop: the next cursor "c1" was already consumed.
			return Page[thing]{Items: []thing{{"b"}}, NextCursor: "c1"}, nil
		}
		return Page[thing]{}, errors.New("unreachable")
	}

	it := NewIterator(fetch, thingID)
	var ids []string
	for it.Next(context.Background()) {
		ids = append(ids, it.Item().ID)
	}
	// "b" from the looping page is delivered, then the regression surfaces.
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("ids = %v, want [a b] before anomaly", ids)
	}
	if !errors.Is(it.Err(), ErrCursorRegression) {
		t.Fatalf("Err = %v, want ErrCursorRegression", it.Err())
	}
	if atomic.LoadInt32(&fetches) != 2 {
		t.Fatalf("fetches = %d, want exactly 2 (no infinite loop)", fetches)
	}
	t.Logf("判定依据: 游标回退到已消费页，先交付 %v，随后 ErrCursorRegression 终止，抓取 %d 次", ids, fetches)
}

// TestPaginationImmediateLoop verifies a zero-item repeat page fails
// immediately without looping.
func TestPaginationImmediateLoop(t *testing.T) {
	var fetches int32
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		atomic.AddInt32(&fetches, 1)
		// First page is empty but points back at the start cursor "".
		return Page[thing]{Items: nil, NextCursor: ""}, nil
	}
	it := NewIterator(fetch, thingID)
	for it.Next(context.Background()) {
		t.Fatal("no items expected")
	}
	if it.Err() != nil {
		t.Fatalf("empty final page should end cleanly, got %v", it.Err())
	}

	// Now a non-empty first page whose next cursor is the start cursor "".
	atomic.StoreInt32(&fetches, 0)
	fetch2 := func(ctx context.Context, cursor string) (Page[thing], error) {
		atomic.AddInt32(&fetches, 1)
		return Page[thing]{Items: []thing{{"a"}}, NextCursor: ""}, nil
	}
	it2 := NewIterator(fetch2, thingID)
	var ids []string
	for it2.Next(context.Background()) {
		ids = append(ids, it2.Item().ID)
	}
	if len(ids) != 1 || ids[0] != "a" {
		t.Fatalf("ids = %v, want [a] with clean end", ids)
	}
	if it2.Err() != nil {
		t.Fatalf("next cursor \"\" is the documented end marker, not a loop: %v", it2.Err())
	}
	t.Log("判定依据: NextCursor=\"\" 是约定的末页标记，正常结束而非回退错误")
}

// TestPaginationPageSkipped verifies a jumping page ordinal fails loudly
// with ErrPageSkipped rather than silently omitting data.
func TestPaginationPageSkipped(t *testing.T) {
	ord := 0
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		ord++
		switch ord {
		case 1:
			return Page[thing]{Items: []thing{{"a"}}, NextCursor: "c1", PageNumber: 1}, nil
		case 2:
			return Page[thing]{Items: []thing{{"z"}}, NextCursor: "c2", PageNumber: 3}, nil // skipped 2
		default:
			return Page[thing]{}, nil
		}
	}
	it := NewIterator(fetch, thingID)
	var ids []string
	for it.Next(context.Background()) {
		ids = append(ids, it.Item().ID)
	}
	if len(ids) != 1 || ids[0] != "a" {
		t.Fatalf("ids before skip = %v, want [a]", ids)
	}
	if !errors.Is(it.Err(), ErrPageSkipped) {
		t.Fatalf("Err = %v, want ErrPageSkipped", it.Err())
	}
	t.Logf("判定依据: 页序号 1→3 跳页，交付 %v 后以 ErrPageSkipped 显式失败", ids)
}

// TestPaginationPageBackwards verifies a decreasing page ordinal is a
// regression error.
func TestPaginationPageBackwards(t *testing.T) {
	ord := 0
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		ord++
		switch ord {
		case 1:
			return Page[thing]{Items: []thing{{"a"}}, NextCursor: "c1", PageNumber: 1}, nil
		case 2:
			return Page[thing]{Items: []thing{{"b"}}, NextCursor: "c2", PageNumber: 2}, nil
		case 3:
			return Page[thing]{Items: []thing{{"x"}}, NextCursor: "", PageNumber: 1}, nil // backwards
		default:
			return Page[thing]{}, nil
		}
	}
	it := NewIterator(fetch, thingID)
	var ids []string
	for it.Next(context.Background()) {
		ids = append(ids, it.Item().ID)
	}
	if !errors.Is(it.Err(), ErrCursorRegression) {
		t.Fatalf("Err = %v, want ErrCursorRegression", it.Err())
	}
	t.Logf("判定依据: 页序号 2→1 回退，以 ErrCursorRegression 终止，已交付 %v", ids)
}

// TestPaginationContextCancellation verifies the caller can abort a blocked
// traversal safely and promptly.
func TestPaginationContextCancellation(t *testing.T) {
	entered := make(chan struct{})
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		close(entered)
		<-ctx.Done() // simulate a hanging upstream
		return Page[thing]{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	it := NewIterator(fetch, thingID)
	done := make(chan struct{})
	go func() {
		it.Next(ctx)
		close(done)
	}()
	<-entered
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("iterator did not stop after context cancellation")
	}
	if !errors.Is(it.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", it.Err())
	}
	t.Log("判定依据: 抓取阻塞时 cancel()，遍历在 1s 内安全中止，Err=context.Canceled")
}

// TestPaginationFetchErrorPropagates verifies a fetcher error stops
// iteration and is returned verbatim.
func TestPaginationFetchErrorPropagates(t *testing.T) {
	boom := errors.New("boom")
	fetch := func(ctx context.Context, cursor string) (Page[thing], error) {
		if cursor == "" {
			return Page[thing]{Items: []thing{{"a"}}, NextCursor: "c1"}, nil
		}
		return Page[thing]{}, boom
	}
	it := NewIterator(fetch, thingID)
	var ids []string
	for it.Next(context.Background()) {
		ids = append(ids, it.Item().ID)
	}
	if !errors.Is(it.Err(), boom) {
		t.Fatalf("Err = %v, want fetcher boom", it.Err())
	}
	t.Logf("判定依据: 抓取错误逐字透传并停止遍历，已交付 %v", ids)
}

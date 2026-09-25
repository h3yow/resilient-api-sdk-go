package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

// 正常翻页：按游标顺序遍历，无重复无遗漏。
func TestIterator_NormalTraversal(t *testing.T) {
	pages := map[string]Page[int]{
		"":   {Items: []int{1, 2}, Next: "c1"},
		"c1": {Items: []int{3, 4}, Next: "c2"},
		"c2": {Items: []int{5}, Next: ""},
	}
	var fetches atomic.Int32
	it := NewIterator(func(ctx context.Context, cursor string) (Page[int], error) {
		fetches.Add(1)
		p, ok := pages[cursor]
		if !ok {
			return Page[int]{}, fmt.Errorf("unknown cursor %q", cursor)
		}
		return p, nil
	})
	got, err := Collect(context.Background(), it)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{1, 2, 3, 4, 5}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("遍历结果 = %v, 期望 %v", got, want)
	}
	if fetches.Load() != 3 {
		t.Fatalf("抓取次数 = %d, 期望 3", fetches.Load())
	}
	t.Logf("判定依据: 3 页抓取 3 次, 结果 %v 完整且有序", got)
}

// 游标未前进（服务端返回相同游标）：必须报 ErrPagination 而不是死循环或重复数据。
func TestIterator_CursorNotAdvancing(t *testing.T) {
	it := NewIterator(func(ctx context.Context, cursor string) (Page[int], error) {
		return Page[int]{Items: []int{1}, Next: "same"}, nil // 永远返回同一游标
	})
	if _, _, err := it.NextPage(context.Background()); err != nil {
		t.Fatalf("首页不应报错: %v", err)
	}
	_, _, err := it.NextPage(context.Background())
	if !errors.Is(err, ErrPagination) {
		t.Fatalf("错误 = %v, 期望 ErrPagination", err)
	}
	t.Logf("判定依据: 第二页游标未前进, 归类为 ErrPagination: %v", err)
}

// 游标回退（服务端返回已见过的旧游标）：必须报 ErrPagination。
func TestIterator_CursorRegression(t *testing.T) {
	pages := map[string]Page[int]{
		"":   {Items: []int{1}, Next: "c1"},
		"c1": {Items: []int{2}, Next: "c2"},
		"c2": {Items: []int{3}, Next: "c1"}, // 回退到 c1
	}
	it := NewIterator(func(ctx context.Context, cursor string) (Page[int], error) {
		return pages[cursor], nil
	})
	_, err := Collect(context.Background(), it)
	if !errors.Is(err, ErrPagination) {
		t.Fatalf("错误 = %v, 期望 ErrPagination", err)
	}
	t.Logf("判定依据: 游标从 c2 回退到已见过的 c1, 归类为 ErrPagination: %v", err)
}

// 遍历中止：ctx 取消后安全退出，返回已收集部分与错误。
func TestIterator_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var fetches atomic.Int32
	it := NewIterator(func(ctx context.Context, cursor string) (Page[int], error) {
		n := fetches.Add(1)
		if n == 2 {
			cancel() // 第二页抓取时取消
		}
		return Page[int]{Items: []int{int(n)}, Next: fmt.Sprintf("c%d", n)}, nil
	})
	got, err := Collect(ctx, it)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("错误 = %v, 期望 context.Canceled", err)
	}
	if len(got) == 0 {
		t.Fatal("中止前应保留已收集的部分数据")
	}
	t.Logf("判定依据: 第 2 页后取消, 已收集 %v, 错误归类为 context.Canceled", got)
}

// DecodePage：两种信封形态都能解析，未知字段被忽略。
func TestDecodePage_Compat(t *testing.T) {
	p1, err := DecodePage[int]([]byte(`{"items":[1,2],"next_cursor":"abc","brand_new_field":{"x":1}}`))
	if err != nil || p1.Next != "abc" || len(p1.Items) != 2 {
		t.Fatalf("next_cursor 形态解析失败: %+v, %v", p1, err)
	}
	p2, err := DecodePage[int]([]byte(`{"items":[3],"next":"https://api.local/items?cursor=z","future_flag":true}`))
	if err != nil || p2.Next == "" || len(p2.Items) != 1 {
		t.Fatalf("next 链接形态解析失败: %+v, %v", p2, err)
	}
	t.Logf("判定依据: 未知字段被忽略, 两种形态均解析成功: %+v / %+v", p1, p2)
}

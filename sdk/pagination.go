package sdk

import (
	"context"
	"encoding/json"
	"fmt"
)

// Page 是分页资源的一页。Next 为服务端返回的游标或下一页链接，为空表示末页。
type Page[T any] struct {
	Items []T
	Next  string
}

// PageFetcher 按游标取一页；cursor 为空表示首页。
type PageFetcher[T any] func(ctx context.Context, cursor string) (Page[T], error)

// Iterator 统一遍历分页资源，游标去重、防回退，可被 ctx 中止。
type Iterator[T any] struct {
	fetch PageFetcher[T]
	seen  map[string]struct{} // 已出现过的游标，用于检测重复页/回退
	next  string              // 下一次请求使用的游标
	done  bool
}

// NewIterator 创建遍历器。
func NewIterator[T any](fetch PageFetcher[T]) *Iterator[T] {
	return &Iterator[T]{fetch: fetch, seen: map[string]struct{}{}}
}

// NextPage 返回下一页；没有更多页时 done=true。
// 游标未前进（重复页）或回退到已见过的游标时返回 ErrPagination，且不吐出该页数据。
func (it *Iterator[T]) NextPage(ctx context.Context) (page Page[T], done bool, err error) {
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
	if page.Next == "" {
		it.done = true // 末页
		return page, false, nil
	}
	if page.Next == cursor {
		return Page[T]{}, false, fmt.Errorf("%w: cursor did not advance (still %q)", ErrPagination, cursor)
	}
	if _, dup := it.seen[page.Next]; dup {
		return Page[T]{}, false, fmt.Errorf("%w: cursor regression to %q", ErrPagination, page.Next)
	}
	it.seen[page.Next] = struct{}{}
	it.next = page.Next
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
func DecodePage[T any](body []byte) (Page[T], error) {
	var env struct {
		Items      []T    `json:"items"`
		NextCursor string `json:"next_cursor"`
		Next       string `json:"next"`
		NextLink   string `json:"next_link"`
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
	return Page[T]{Items: env.Items, Next: next}, nil
}

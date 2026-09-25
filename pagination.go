package sdk

import "context"

// Page is one page produced by a PageFetcher.
type Page[T any] struct {
	// Items are the items on this page, in server order.
	Items []T
	// NextCursor is the cursor to use for the next page. "" marks the final
	// page. A link-based fetcher may place the absolute next-page URL here;
	// the iterator treats the value opaquely.
	NextCursor string
	// PageNumber is an optional 1-based ordinal assigned by the fetcher.
	// Leave <= 0 when the server is cursor-only. When present, the iterator
	// verifies strict progression so a skipped page cannot silently drop
	// data.
	PageNumber int
}

// PageFetcher retrieves a single page for the given cursor. The empty
// cursor always denotes the first page.
type PageFetcher[T any] func(ctx context.Context, cursor string) (Page[T], error)

// Iterator walks a paginated resource. It guarantees:
//
//   - An item whose stable identifier has already been yielded is skipped
//     (repeated pages cannot duplicate data).
//   - A next cursor equal to a cursor already consumed, or a page ordinal
//     that moves backwards, terminates with ErrCursorRegression.
//   - A page ordinal that jumps forward terminates with ErrPageSkipped,
//     because data would have been omitted.
//   - Cancellation of the context passed to Next stops iteration promptly;
//     Err then returns ctx.Err.
type Iterator[T any] struct {
	fetch PageFetcher[T]
	id    func(T) string

	cursor      string
	buffer      []T
	pos         int
	seenCursors map[string]struct{}
	seenIDs     map[string]struct{}
	pagesSeen   int
	// lastReached means the server indicated there are no further pages.
	lastReached bool
	// pendingErr is an anomaly discovered while items from the current page
	// remain buffered; it is promoted to err after the buffer drains, so no
	// data that was actually received is silently dropped.
	pendingErr error
	err        error
	// Duplicates counts items suppressed because their identifier had
	// already been yielded.
	Duplicates int
}

// NewIterator builds an Iterator. id extracts a stable per-item identifier
// used to suppress duplicates; it must return a unique value per item.
func NewIterator[T any](fetch PageFetcher[T], id func(T) string) *Iterator[T] {
	return &Iterator[T]{
		fetch:       fetch,
		id:          id,
		seenCursors: map[string]struct{}{"": {}},
		seenIDs:     make(map[string]struct{}),
	}
}

// Next advances to the next item. It returns false when traversal is done
// (check Err to distinguish success from failure) or when ctx is cancelled.
func (it *Iterator[T]) Next(ctx context.Context) bool {
	if it.err != nil {
		return false
	}
	if it.pos < len(it.buffer) {
		it.pos++
		return true
	}
	if it.pendingErr != nil {
		it.err = it.pendingErr
		return false
	}
	if it.lastReached {
		return false
	}

	for {
		if err := ctx.Err(); err != nil {
			it.err = err
			return false
		}
		page, err := it.fetch(ctx, it.cursor)
		if err != nil {
			it.err = err
			return false
		}
		it.pagesSeen++

		// Validate ordinal progression when the fetcher provides one.
		if page.PageNumber > 0 {
			switch {
			case page.PageNumber < it.pagesSeen:
				it.err = ErrCursorRegression
				return false
			case page.PageNumber > it.pagesSeen:
				it.err = ErrPageSkipped
				return false
			}
		}

		it.buffer = it.buffer[:0]
		for _, item := range page.Items {
			key := it.id(item)
			if _, dup := it.seenIDs[key]; dup {
				it.Duplicates++
				continue
			}
			it.seenIDs[key] = struct{}{}
			it.buffer = append(it.buffer, item)
		}
		it.pos = 0

		if page.NextCursor == "" {
			it.lastReached = true
		} else if _, seen := it.seenCursors[page.NextCursor]; seen {
			// The server pointed back at a page already consumed. Serve the
			// fresh items from the current page, then report the regression
			// rather than looping or silently truncating.
			it.lastReached = true
			if len(it.buffer) == 0 {
				it.err = ErrCursorRegression
				return false
			}
			it.pendingErr = ErrCursorRegression
		} else {
			it.seenCursors[page.NextCursor] = struct{}{}
			it.cursor = page.NextCursor
		}

		if len(it.buffer) > 0 {
			it.pos++
			return true
		}
		if it.lastReached {
			// Final page carried no new items: traversal is complete.
			return false
		}
		// Empty intermediate page: keep paging.
	}
}

// Item returns the item most recently advanced to by Next.
func (it *Iterator[T]) Item() T {
	if it.pos > 0 && it.pos <= len(it.buffer) {
		return it.buffer[it.pos-1]
	}
	var zero T
	return zero
}

// Err returns the first error that stopped iteration, or nil on clean
// completion or active iteration.
func (it *Iterator[T]) Err() error { return it.err }

// Collect drains the iterator into a slice. It stops and returns the
// context error when ctx is cancelled.
func Collect[T any](ctx context.Context, it *Iterator[T]) ([]T, error) {
	var out []T
	for it.Next(ctx) {
		out = append(out, it.Item())
	}
	return out, it.Err()
}

package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type userPayload struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// TestForwardCompatibleUnknownFields verifies that fields added by a newer
// server do not break older clients (no DisallowUnknownFields).
func TestForwardCompatibleUnknownFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// "newFeature", "futureFlag", "nested" are unknown to this SDK.
		fmt.Fprint(w, `{"id":"u1","name":"simon","newFeature":true,"futureFlag":{"x":1},"nested":[1,2,3]}`)
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL, APIVersion: "2026-09-01", Logger: testLogger(t)})

	var u userPayload
	if err := c.DoJSON(context.Background(), http.MethodGet, "/users/u1", nil, &u); err != nil {
		t.Fatalf("unknown fields must not break decoding: %v", err)
	}
	if u.ID != "u1" || u.Name != "simon" {
		t.Fatalf("decoded payload wrong: %+v", u)
	}
	t.Logf("判定依据: 服务端新增 newFeature/futureFlag/nested 字段被忽略，已知字段正确解析为 %+v", u)
}

// TestVersionHeadersSent verifies the negotiated version is advertised on
// every request so the server can apply compatible semantics.
func TestVersionHeadersSent(t *testing.T) {
	var mu sync.Mutex
	var gotAccept, gotVersion string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAccept = r.Header.Get("Accept")
		gotVersion = r.Header.Get("Api-Version")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL, APIVersion: "2026-09-01"})
	if err := c.DoJSON(context.Background(), http.MethodGet, "/x", nil, nil); err != nil {
		t.Fatal(err)
	}
	if gotVersion != "2026-09-01" {
		t.Fatalf("Api-Version header = %q", gotVersion)
	}
	if gotAccept != "application/json; version=2026-09-01" {
		t.Fatalf("Accept header = %q", gotAccept)
	}
	t.Logf("判定依据: 请求携带 Api-Version=%s 与 Accept=%s", gotVersion, gotAccept)
}

// TestRemovedCapabilityFailsLoudly verifies a removed capability returns a
// distinct *CapabilityError instead of a silent downgrade, is non-retryable,
// and hits the server exactly once.
func TestRemovedCapabilityFailsLoudly(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		fmt.Fprint(w, `{"error":{"code":"capability_removed","message":"legacy export is gone","capability":"legacy.export","removed":true}}`)
	}))
	defer srv.Close()
	c := New(Config{
		BaseURL:    srv.URL,
		APIVersion: "2026-09-01",
		Retry:      RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Logger:     testLogger(t),
	})
	err := c.DoJSON(context.Background(), http.MethodGet, "/export", nil, nil)

	var ce *CapabilityError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CapabilityError, got %T: %v", err, err)
	}
	if ce.Capability != "legacy.export" || !ce.Removed {
		t.Fatalf("capability error fields wrong: %+v", ce)
	}
	if hits != 1 {
		t.Fatalf("removed capability hits = %d, want exactly 1 (must not retry)", hits)
	}
	// Must not masquerade as a generic business/API error.
	var ae *APIError
	if errors.As(err, &ae) {
		t.Fatalf("CapabilityError must remain distinct from *APIError, got %v", ae)
	}
	t.Logf("判定依据: 已移除能力返回独立 CapabilityError{removed=true}，请求 %d 次不重试、不静默降级；%v", hits, err)
}

// TestDeprecatedCapabilityFailsLoudly verifies deprecated-but-advertised
// capabilities are also distinguishable failures.
func TestDeprecatedCapabilityFailsLoudly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"code":"capability_deprecated","capability":"v1.search","deprecated":true}}`)
	}))
	defer srv.Close()
	c := New(Config{BaseURL: srv.URL, APIVersion: "2026-09-01", Logger: testLogger(t)})
	err := c.DoJSON(context.Background(), http.MethodGet, "/search", nil, nil)
	var ce *CapabilityError
	if !errors.As(err, &ce) || !ce.Deprecated || ce.Removed {
		t.Fatalf("want deprecated CapabilityError, got %v", err)
	}
	t.Logf("判定依据: 废弃能力返回 CapabilityError{deprecated=true}，与移除、普通业务错误可区分；%v", err)
}

// TestMalformedPayload verifies a corrupt success payload is a non-retryable
// payload error rather than being retried pointlessly.
func TestMalformedPayload(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"id":`) // malformed
	}))
	defer srv.Close()
	c := New(Config{
		BaseURL: srv.URL, APIVersion: "2026-09-01",
		Retry:  RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		Logger: testLogger(t),
	})
	err := c.DoJSON(context.Background(), http.MethodGet, "/u", nil, &userPayload{})
	var ae *APIError
	if !errors.As(err, &ae) || ae.Kind != KindPayload || ae.Retryable {
		t.Fatalf("want non-retryable KindPayload error, got %v", err)
	}
	if hits != 1 {
		t.Fatalf("malformed payload hits = %d, want 1 (not retried)", hits)
	}
	t.Logf("判定依据: 损坏 JSON 归类 KindPayload 且不可重试，请求 %d 次；%v", hits, err)
}

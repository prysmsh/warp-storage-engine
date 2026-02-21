package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(100, 50, 10)
	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}
}

func TestRateLimiter_Middleware_AllowsRequest(t *testing.T) {
	rl := NewRateLimiter(1000, 500, 100)
	handler := rl.Middleware(100)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := handler(next)

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

func TestNewConcurrencyLimiter(t *testing.T) {
	cl := NewConcurrencyLimiter(10)
	if cl == nil {
		t.Fatal("NewConcurrencyLimiter returned nil")
	}
}

func TestConcurrencyLimiter_Middleware(t *testing.T) {
	cl := NewConcurrencyLimiter(2)
	handler := cl.Middleware()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	wrapped := handler(next)

	req := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

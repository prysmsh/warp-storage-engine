package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prysmsh/warp-storage-engine/internal/config"
)

func TestServer_healthCheck(t *testing.T) {
	s := &Server{config: &config.Config{}}
	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	s.healthCheck(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("healthCheck code = %d, want 200", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %s", rr.Header().Get("Content-Type"))
	}
	if rr.Body.Len() == 0 {
		t.Error("body empty")
	}
}

func TestServer_healthCheck_ShuttingDown(t *testing.T) {
	s := &Server{config: &config.Config{}}
	s.SetShuttingDown()
	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	s.healthCheck(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("healthCheck when shutting down code = %d, want 503", rr.Code)
	}
}

func TestServer_readinessCheck(t *testing.T) {
	s := &Server{config: &config.Config{}}
	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()
	s.readinessCheck(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("readinessCheck code = %d, want 200", rr.Code)
	}
}

func TestServer_readinessCheck_ShuttingDown(t *testing.T) {
	s := &Server{config: &config.Config{}}
	s.SetShuttingDown()
	req := httptest.NewRequest("GET", "/ready", nil)
	rr := httptest.NewRecorder()
	s.readinessCheck(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("readinessCheck when shutting down code = %d, want 503", rr.Code)
	}
}

func TestServer_getFeatures(t *testing.T) {
	s := &Server{config: &config.Config{}}
	req := httptest.NewRequest("GET", "/api/features", nil)
	rr := httptest.NewRecorder()
	s.getFeatures(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("getFeatures code = %d, want 200", rr.Code)
	}
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %s", rr.Header().Get("Content-Type"))
	}
	body := rr.Body.String()
	for _, key := range []string{"virustotal", "auth0", "ui", "shareLinks"} {
		if !strings.Contains(body, key) {
			t.Errorf("getFeatures body missing %q: %s", key, body)
		}
	}
}

func TestServer_SetShuttingDown_IsShuttingDown(t *testing.T) {
	s := &Server{config: &config.Config{}}
	if s.IsShuttingDown() {
		t.Error("expected not shutting down initially")
	}
	s.SetShuttingDown()
	if !s.IsShuttingDown() {
		t.Error("expected shutting down after SetShuttingDown")
	}
}

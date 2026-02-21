package proxy

import (
	"net/http/httptest"
	"testing"
)

func TestServeSwaggerUI_Spec(t *testing.T) {
	spec := []byte("openapi: 3.0.0\ninfo:\n  title: Test API\n")
	handler := ServeSwaggerUI(spec, "/docs")

	tests := []struct {
		path     string
		wantCode int
		bodyLen  int
	}{
		{"/docs/openapi.yaml", 200, len(spec)},
		{"/docs/openapi.json", 200, len(spec)},
		{"/docs/spec", 200, len(spec)},
		{"/docs/", 200, 0},   // HTML
		{"/docs", 200, 0},    // HTML
		{"/docs/index.html", 200, 0},
		{"/docs/other", 404, 0},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			rr := httptest.NewRecorder()
			handler(rr, req)
			if rr.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rr.Code, tt.wantCode)
			}
			if tt.bodyLen > 0 && rr.Body.Len() != tt.bodyLen {
				t.Errorf("body len = %d, want %d", rr.Body.Len(), tt.bodyLen)
			}
		})
	}
}

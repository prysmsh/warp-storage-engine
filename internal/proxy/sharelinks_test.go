package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/einyx/foundation-storage-engine/internal/config"
	"github.com/einyx/foundation-storage-engine/internal/storage"
)

func TestCreateShareLinkRequiresAuthentication(t *testing.T) {
	handler := NewShareLinkHandler(nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	req := httptest.NewRequest("POST", "/api/share/create", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()

	handler.CreateShareLinkHandler(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", rr.Code)
	}
}

func TestCreateShareLinkRequiresAdmin(t *testing.T) {
	handler := NewShareLinkHandler(nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	req := httptest.NewRequest("POST", "/api/share/create", strings.NewReader(`{"bucket_name":"bucket","object_key":"file.txt"}`))
	ctx := context.WithValue(req.Context(), "authenticated", true)
	ctx = context.WithValue(ctx, "user_sub", "user-123")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handler.CreateShareLinkHandler(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin request, got %d", rr.Code)
	}
}

func TestCreateShareLinkValidatesObjectAccess(t *testing.T) {
	tmpDir := t.TempDir()
	bucket := "test-bucket"
	objectKey := "documents/report.txt"

	objectPath := filepath.Join(tmpDir, bucket, objectKey)
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		t.Fatalf("failed to create object directory: %v", err)
	}
	if err := os.WriteFile(objectPath, []byte("test data"), 0o644); err != nil {
		t.Fatalf("failed to write object: %v", err)
	}

	backend, err := storage.NewFileSystemBackend(&config.FileSystemConfig{BaseDir: tmpDir})
	if err != nil {
		t.Fatalf("failed to create filesystem backend: %v", err)
	}

	handler := NewShareLinkHandler(backend, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	body := `{"bucket_name":"` + bucket + `","object_key":"` + objectKey + `","ttl_hours":1}`
	req := httptest.NewRequest("POST", "/api/share/create", strings.NewReader(body))
	ctx := context.WithValue(req.Context(), "authenticated", true)
	ctx = context.WithValue(ctx, "is_admin", true)
	ctx = context.WithValue(ctx, "user_sub", "admin-user-1")
	ctx = context.WithValue(ctx, "user", map[string]interface{}{"email": "admin@example.com"})
	req = req.WithContext(ctx)
	req.Header.Set("Host", "example.com")

	rr := httptest.NewRecorder()
	handler.CreateShareLinkHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid share link, got %d - response: %s", rr.Code, rr.Body.String())
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["created_by"] != "admin@example.com" {
		t.Fatalf("expected created_by to reflect admin email, got %v", resp["created_by"])
	}

	if shareURL, ok := resp["share_url"].(string); !ok || !strings.Contains(shareURL, "example.com") {
		t.Fatalf("expected share_url to contain host, got %v", resp["share_url"])
	}
}

func TestCreateShareLinkRejectsMissingObject(t *testing.T) {
	tmpDir := t.TempDir()
	bucket := "secret-bucket"

	backend, err := storage.NewFileSystemBackend(&config.FileSystemConfig{BaseDir: tmpDir})
	if err != nil {
		t.Fatalf("failed to create filesystem backend: %v", err)
	}

	handler := NewShareLinkHandler(backend, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	body := `{"bucket_name":"` + bucket + `","object_key":"missing.txt","ttl_hours":1}`
	req := httptest.NewRequest("POST", "/api/share/create", strings.NewReader(body))
	ctx := context.WithValue(req.Context(), "authenticated", true)
	ctx = context.WithValue(ctx, "is_admin", true)
	ctx = context.WithValue(ctx, "user_sub", "admin-user-1")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handler.CreateShareLinkHandler(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when object is missing, got %d", rr.Code)
	}
}

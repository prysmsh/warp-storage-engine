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
	"time"

	"github.com/prysmsh/warp-storage-engine/internal/config"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

func TestShareLinkManager_GetShareLink_Expired(t *testing.T) {
	m := NewShareLinkManager()
	link, err := m.CreateShareLink("b", "k", "u", 1, "", false) // 1ns TTL
	if err != nil {
		t.Fatalf("CreateShareLink: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	_, err = m.GetShareLink(link.ID)
	if err == nil {
		t.Fatal("expected error for expired link")
	}
	if err != nil && !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected expired error, got %v", err)
	}
}

func TestShareLinkManager_GetShareLink_MaxAccess(t *testing.T) {
	m := NewShareLinkManager()
	link, err := m.CreateShareLink("b", "k", "u", 24*time.Hour, "", true)
	if err != nil {
		t.Fatalf("CreateShareLink: %v", err)
	}
	_, _ = m.GetShareLink(link.ID)
	m.IncrementAccessCount(link.ID)
	_, err = m.GetShareLink(link.ID)
	if err == nil {
		t.Fatal("expected error after max access reached")
	}
}

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

func TestShareLinkManager_CreateGetIncrement(t *testing.T) {
	m := NewShareLinkManager()
	link, err := m.CreateShareLink("b", "k", "user1", 24*time.Hour, "", false)
	if err != nil {
		t.Fatalf("CreateShareLink: %v", err)
	}
	if link.ID == "" || link.BucketName != "b" || link.ObjectKey != "k" {
		t.Errorf("unexpected link: %+v", link)
	}

	got, err := m.GetShareLink(link.ID)
	if err != nil {
		t.Fatalf("GetShareLink: %v", err)
	}
	if got.ID != link.ID {
		t.Errorf("got id %s want %s", got.ID, link.ID)
	}

	_, err = m.GetShareLink("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent id")
	}

	m.IncrementAccessCount(link.ID)
	got2, _ := m.GetShareLink(link.ID)
	if got2.AccessCount != 1 {
		t.Errorf("AccessCount want 1 got %d", got2.AccessCount)
	}
}

func TestShareLinkManager_CreateWithPassword(t *testing.T) {
	m := NewShareLinkManager()
	link, err := m.CreateShareLink("b", "k", "user1", 0, "secret123", false)
	if err != nil {
		t.Fatalf("CreateShareLink: %v", err)
	}
	if link.PasswordHash == "" {
		t.Error("expected password hash when password set")
	}
}

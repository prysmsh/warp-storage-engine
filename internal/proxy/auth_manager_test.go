package proxy

import (
	"net/http/httptest"
	"testing"

	"github.com/prysmsh/warp-storage-engine/internal/auth"
	"github.com/prysmsh/warp-storage-engine/internal/config"
)

func TestNewAuthenticationManager(t *testing.T) {
	cfg := &config.Config{Auth: config.AuthConfig{Type: "none"}}
	am := NewAuthenticationManager(cfg, nil, nil)
	if am == nil {
		t.Fatal("NewAuthenticationManager returned nil")
	}
}

func TestAuthenticationManager_AuthNone(t *testing.T) {
	cfg := &config.Config{Auth: config.AuthConfig{Type: "none"}}
	am := NewAuthenticationManager(cfg, nil, nil)
	req := httptest.NewRequest("GET", "/", nil)
	authCtx, err := am.AuthenticateRequest(req)
	if err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	if !authCtx.Authenticated || authCtx.AuthMethod != "none" || !authCtx.IsAdmin {
		t.Errorf("unexpected auth context: %+v", authCtx)
	}
}

func TestAuthenticationManager_PublicPath(t *testing.T) {
	cfg := &config.Config{Auth: config.AuthConfig{Type: "basic", Identity: "u", Credential: "p"}}
	awsAuth, _ := auth.NewProvider(cfg.Auth)
	am := NewAuthenticationManager(cfg, nil, awsAuth)
	req := httptest.NewRequest("GET", "/health", nil)
	authCtx, err := am.AuthenticateRequest(req)
	if err != nil {
		t.Fatalf("AuthenticateRequest: %v", err)
	}
	if !authCtx.Authenticated || authCtx.AuthMethod != "public" {
		t.Errorf("public path /health: %+v", authCtx)
	}
}

func TestAuthenticationManager_ApplyAuthContext(t *testing.T) {
	am := NewAuthenticationManager(&config.Config{}, nil, nil)
	req := httptest.NewRequest("GET", "/", nil)
	authCtx := &AuthContext{Authenticated: true, UserSub: "user-1", IsAdmin: true}
	req2 := am.ApplyAuthContext(req, authCtx)
	if req2 == req {
		t.Error("expected new request")
	}
	if req2.Context().Value("authenticated") != true {
		t.Error("context should have authenticated")
	}
	if req2.Context().Value("user_sub") != "user-1" {
		t.Error("context should have user_sub")
	}
}

func TestAuthenticationManager_ApplyAuthContext_NotAuthenticated(t *testing.T) {
	am := NewAuthenticationManager(&config.Config{}, nil, nil)
	req := httptest.NewRequest("GET", "/", nil)
	authCtx := &AuthContext{Authenticated: false}
	req2 := am.ApplyAuthContext(req, authCtx)
	if req2 != req {
		t.Error("unauthenticated should return same request")
	}
}

func TestAuthenticationError_Error(t *testing.T) {
	e := &AuthenticationError{Message: "test error", Code: 401}
	if e.Error() != "test error" {
		t.Errorf("Error() = %q", e.Error())
	}
}

func TestAuthenticationError_Code(t *testing.T) {
	e := &AuthenticationError{Type: "auth_failed", Message: "forbidden", Code: 403}
	if e.Code != 403 {
		t.Errorf("Code = %d, want 403", e.Code)
	}
}

func TestAuthenticationManager_CleanHeaders(t *testing.T) {
	am := NewAuthenticationManager(&config.Config{}, nil, nil)
	req := httptest.NewRequest("GET", "/ui/", nil)
	req.Header.Set("Authorization", "Bearer x")
	req.Header.Set("X-Amz-Date", "y")
	am.CleanHeaders(req)
	if req.Header.Get("Authorization") != "" {
		t.Error("Authorization should be removed for UI path")
	}
	if req.Header.Get("X-Amz-Date") != "" {
		t.Error("X-Amz-Date should be removed for UI path")
	}
}

func TestAuthenticationManager_CleanHeaders_NonUIPath(t *testing.T) {
	am := NewAuthenticationManager(&config.Config{}, nil, nil)
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("Authorization", "Bearer x")
	req.Header.Set("X-Amz-Date", "y")
	am.CleanHeaders(req)
	if req.Header.Get("Authorization") != "Bearer x" {
		t.Error("Authorization should be preserved for non-UI path")
	}
	if req.Header.Get("X-Amz-Date") != "y" {
		t.Error("X-Amz-Date should be preserved for non-UI path")
	}
}

package auth

import (
	"net/http/httptest"
	"testing"

	"github.com/prysmsh/warp-storage-engine/internal/config"
)

func TestNewMultiProvider_RequiresIdentityAndCredential(t *testing.T) {
	_, err := NewMultiProvider(config.AuthConfig{})
	if err == nil {
		t.Fatal("expected error when identity/credential missing")
	}
	_, err = NewMultiProvider(config.AuthConfig{Identity: "ak"})
	if err == nil {
		t.Fatal("expected error when credential missing")
	}
	_, err = NewMultiProvider(config.AuthConfig{Credential: "sk"})
	if err == nil {
		t.Fatal("expected error when identity missing")
	}

	mp, err := NewMultiProvider(config.AuthConfig{Identity: "AKID", Credential: "secret"})
	if err != nil {
		t.Fatalf("expected success: %v", err)
	}
	if mp == nil {
		t.Fatal("expected non-nil MultiProvider")
	}
}

func TestMultiProvider_GetSecretKey(t *testing.T) {
	mp, _ := NewMultiProvider(config.AuthConfig{Identity: "MYKEY", Credential: "mysecret"})
	got, err := mp.GetSecretKey("MYKEY")
	if err != nil {
		t.Fatalf("GetSecretKey: %v", err)
	}
	if got != "mysecret" {
		t.Errorf("GetSecretKey = %q, want mysecret", got)
	}
	_, err = mp.GetSecretKey("unknown")
	if err == nil {
		t.Error("GetSecretKey(unknown) expected error")
	}
}

func TestMultiProvider_Authenticate_NoHeader(t *testing.T) {
	mp, _ := NewMultiProvider(config.AuthConfig{Identity: "k", Credential: "s"})
	req := httptest.NewRequest("GET", "/", nil)
	err := mp.Authenticate(req)
	if err != nil {
		t.Errorf("no auth header should allow anonymous: %v", err)
	}
}

func TestMultiProvider_Authenticate_UnsupportedMethod(t *testing.T) {
	mp, _ := NewMultiProvider(config.AuthConfig{Identity: "k", Credential: "s"})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer short")
	err := mp.Authenticate(req)
	if err == nil {
		t.Fatal("expected error for unsupported authorization method")
	}
}

// TestMultiProvider_Authenticate_AWSPermissive tests that request with x-amz-date but
// no valid Authorization is allowed through (permissive mode).
func TestMultiProvider_Authenticate_AWSPermissive(t *testing.T) {
	mp, _ := NewMultiProvider(config.AuthConfig{Identity: "k", Credential: "s"})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("x-amz-date", "20250101T120000Z")
	err := mp.Authenticate(req)
	if err != nil {
		t.Errorf("permissive mode with x-amz-date should allow: %v", err)
	}
}

package auth

import (
	"net/http/httptest"
	"testing"

	"github.com/einyx/foundation-storage-engine/internal/config"
)

func TestNewOPAProvider_Disabled(t *testing.T) {
	base := &NoneProvider{}
	cfg := config.AuthConfig{}
	opaCfg := config.OPAConfig{Enabled: false}
	p := NewOPAProvider(cfg, opaCfg, base)
	if p == nil {
		t.Fatal("NewOPAProvider returned nil")
	}
	// Authenticate when OPA disabled should just delegate to base
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	err := p.Authenticate(req)
	if err != nil {
		t.Errorf("Authenticate with OPA disabled: %v", err)
	}
}

func TestOPAProvider_GetSecretKey_Delegates(t *testing.T) {
	base := &BasicProvider{identity: "ak", credential: "sk"}
	p := NewOPAProvider(config.AuthConfig{}, config.OPAConfig{Enabled: false}, base)
	got, err := p.GetSecretKey("ak")
	if err != nil {
		t.Fatalf("GetSecretKey: %v", err)
	}
	if got != "sk" {
		t.Errorf("GetSecretKey = %q, want sk", got)
	}
}

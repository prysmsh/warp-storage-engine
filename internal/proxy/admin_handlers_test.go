package proxy

import (
	"testing"
)

func TestNewAdminHandlers(t *testing.T) {
	h := NewAdminHandlers(nil)
	if h == nil {
		t.Fatal("NewAdminHandlers returned nil")
	}
	if h.store == nil {
		t.Error("store should be initialized")
	}
}

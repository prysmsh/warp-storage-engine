package storage

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/einyx/foundation-storage-engine/internal/config"
)

func TestNewMultipartWrapper(t *testing.T) {
	fs, err := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewFileSystemBackend: %v", err)
	}
	w := NewMultipartWrapper(fs, 30*time.Second)
	if w == nil {
		t.Fatal("NewMultipartWrapper returned nil")
	}
	if w.Backend == nil {
		t.Error("Backend should be set")
	}
}

func TestMultipartWrapper_UploadPart_DelegatesToBackend(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	wrap := NewMultipartWrapper(fs, time.Second)
	ctx := context.Background()
	// No multipart upload exists - backend returns error; we only check wrapper doesn't panic
	_, err := wrap.UploadPart(ctx, "bucket", "key", "upload-id", 1, bytes.NewReader([]byte("x")), 1)
	_ = err
}

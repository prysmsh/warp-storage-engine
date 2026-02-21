package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/einyx/foundation-storage-engine/internal/config"
)

func TestNewBackend(t *testing.T) {
	tests := []struct {
		name        string
		cfg         config.StorageConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "filesystem provider",
			cfg: config.StorageConfig{
				Provider: "filesystem",
				FileSystem: &config.FileSystemConfig{
					BaseDir: "/tmp/test",
				},
			},
			wantErr: false,
		},
		{
			name: "s3 provider",
			cfg: config.StorageConfig{
				Provider: "s3",
				S3: &config.S3StorageConfig{
					Endpoint:  "http://localhost:9000",
					AccessKey: "test",
					SecretKey: "test", // pragma: allowlist secret
					Region:    "us-east-1",
				},
			},
			wantErr: false,
		},
		{
			name: "azure provider",
			cfg: config.StorageConfig{
				Provider: "azure",
				Azure: &config.AzureStorageConfig{
					AccountName:   "test",
					AccountKey:    "dGVzdA==", // base64 encoded "test" // pragma: allowlist secret
					ContainerName: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "azureblob provider alias",
			cfg: config.StorageConfig{
				Provider: "azureblob",
				Azure: &config.AzureStorageConfig{
					AccountName:   "test",
					AccountKey:    "dGVzdA==", // pragma: allowlist secret
					ContainerName: "test",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid provider",
			cfg: config.StorageConfig{
				Provider: "invalid",
			},
			wantErr:     true,
			errContains: "unsupported storage provider",
		},
		{
			name: "empty provider",
			cfg: config.StorageConfig{
				Provider: "",
			},
			wantErr:     true,
			errContains: "unsupported storage provider",
		},
		{
			name: "filesystem missing config",
			cfg: config.StorageConfig{
				Provider: "filesystem",
			},
			wantErr:     true,
			errContains: "filesystem configuration required",
		},
		{
			name: "s3 with invalid key",
			cfg: config.StorageConfig{
				Provider: "s3",
				S3: &config.S3StorageConfig{
					AccessKey: "test",
					SecretKey: "", // empty secret key should cause error during actual AWS client creation
				},
			},
			wantErr: false, // The factory doesn't validate S3 creds, AWS client creation would fail
		},
		{
			name: "azure with invalid key",
			cfg: config.StorageConfig{
				Provider: "azure",
				Azure: &config.AzureStorageConfig{
					AccountName:   "test",
					AccountKey:    "not-valid-base64!@#$", // pragma: allowlist secret
					ContainerName: "test",
				},
			},
			wantErr:     true,
			errContains: "invalid credentials",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, err := NewBackend(tt.cfg)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewBackend() expected error but got none")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewBackend() error = %v, want error containing %v", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("NewBackend() unexpected error = %v", err)
				}
				if backend == nil {
					t.Errorf("NewBackend() returned nil backend")
				}
			}
		})
	}
}

func TestNewMultiBackendSimple_NoBackends(t *testing.T) {
	cfg := &config.StorageConfig{}
	_, err := NewMultiBackendSimple(cfg)
	if err == nil {
		t.Fatal("NewMultiBackendSimple with no backends expected error")
	}
	if !contains(err.Error(), "no storage backends") {
		t.Errorf("error = %v, want 'no storage backends'", err)
	}
}

func TestMultiBackendSimple_GetObject_UnknownBucket(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.StorageConfig{
		FileSystem: &config.FileSystemConfig{BaseDir: dir},
	}
	// Use NewBackend with multi provider - but multi needs at least one backend.
	// So create filesystem backend and then a multi with only filesystem.
	fs, err := NewFileSystemBackend(cfg.FileSystem)
	if err != nil {
		t.Fatalf("NewFileSystemBackend: %v", err)
	}
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"filesystem": fs},
		routing:  map[string]string{"data": "filesystem"},
	}
	ctx := context.Background()
	_, err = mb.GetObject(ctx, "unknown-bucket", "key")
	if err == nil {
		t.Fatal("GetObject with unknown bucket expected error")
	}
	if !contains(err.Error(), "no backend found") {
		t.Errorf("error = %v", err)
	}
}

func TestMultiBackendSimple_BucketExists_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	exists, err := mb.BucketExists(ctx, "unknown-bucket")
	if err == nil {
		t.Fatal("BucketExists(unknown) expected error")
	}
	if exists {
		t.Error("exists should be false")
	}
}

func TestMultiBackendSimple_DeleteObject_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.DeleteObject(ctx, "unknown-bucket", "key")
	if err == nil {
		t.Fatal("DeleteObject(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_ListBuckets(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: dir})
	ctx := context.Background()
	_ = fs.CreateBucket(ctx, "b1")
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"b1": "fs"},
	}
	buckets, err := mb.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 1 {
		t.Errorf("ListBuckets len = %d, want 1", len(buckets))
	}
}

func TestMultiBackendSimple_CreateBucket_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.CreateBucket(ctx, "unknown-bucket")
	if err == nil {
		t.Fatal("CreateBucket(unknown) expected error")
	}
}

func TestMultiBackendSimple_DeleteBucket_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.DeleteBucket(ctx, "unknown-bucket")
	if err == nil {
		t.Fatal("DeleteBucket(unknown) expected error")
	}
}

func TestMultiBackendSimple_ListObjects_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.ListObjects(ctx, "unknown-bucket", "", "", 100)
	if err == nil {
		t.Fatal("ListObjects(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_ListObjectsWithDelimiter_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.ListObjectsWithDelimiter(ctx, "unknown-bucket", "", "", "/", 100)
	if err == nil {
		t.Fatal("ListObjectsWithDelimiter(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_PutObject_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.PutObject(ctx, "unknown-bucket", "key", bytes.NewReader(nil), 0, nil)
	if err == nil {
		t.Fatal("PutObject(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_HeadObject_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.HeadObject(ctx, "unknown-bucket", "key")
	if err == nil {
		t.Fatal("HeadObject(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_ListDeletedObjects_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.ListDeletedObjects(ctx, "unknown-bucket", "", "", 100)
	if err == nil {
		t.Fatal("ListDeletedObjects(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_RestoreObject_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.RestoreObject(ctx, "unknown-bucket", "key", "")
	if err == nil {
		t.Fatal("RestoreObject(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_InitiateMultipartUpload_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.InitiateMultipartUpload(ctx, "unknown-bucket", "key", nil)
	if err == nil {
		t.Fatal("InitiateMultipartUpload(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_UploadPart_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.UploadPart(ctx, "unknown-bucket", "key", "upload-id", 1, bytes.NewReader(nil), 0)
	if err == nil {
		t.Fatal("UploadPart(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_CompleteMultipartUpload_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.CompleteMultipartUpload(ctx, "unknown-bucket", "key", "upload-id", nil)
	if err == nil {
		t.Fatal("CompleteMultipartUpload(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_AbortMultipartUpload_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.AbortMultipartUpload(ctx, "unknown-bucket", "key", "upload-id")
	if err == nil {
		t.Fatal("AbortMultipartUpload(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_ListParts_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.ListParts(ctx, "unknown-bucket", "key", "upload-id", 10, 0)
	if err == nil {
		t.Fatal("ListParts(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_GetObjectACL_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	_, err := mb.GetObjectACL(ctx, "unknown-bucket", "key")
	if err == nil {
		t.Fatal("GetObjectACL(unknown bucket) expected error")
	}
}

func TestMultiBackendSimple_PutObjectACL_UnknownBucket(t *testing.T) {
	fs, _ := NewFileSystemBackend(&config.FileSystemConfig{BaseDir: t.TempDir()})
	mb := &MultiBackendSimple{
		backends: map[string]Backend{"fs": fs},
		routing:  map[string]string{"data": "fs"},
	}
	ctx := context.Background()
	err := mb.PutObjectACL(ctx, "unknown-bucket", "key", &ACL{})
	if err == nil {
		t.Fatal("PutObjectACL(unknown bucket) expected error")
	}
}

func TestNewBackend_MultiProvider_NoBackends(t *testing.T) {
	cfg := config.StorageConfig{
		Provider: "multi",
		// No S3, Azure, or FileSystem
	}
	_, err := NewBackend(cfg)
	if err == nil {
		t.Fatal("NewBackend(multi) with no backends expected error")
	}
	if !contains(err.Error(), "no storage backends") {
		t.Errorf("error = %v", err)
	}
}

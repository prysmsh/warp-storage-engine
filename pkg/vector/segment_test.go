//go:build vector

package vector

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

func newTestSegmentID() SegmentID {
	return ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
}

func TestGrowingSegment_InsertAndGet(t *testing.T) {
	gs := NewGrowingSegment(newTestSegmentID(), "test-col", 0, 3, Cosine)
	p := Point{ID: 42, Vector: []float32{1, 0, 0}, Payload: map[string]any{"color": "red"}}
	gs.Insert(p)

	got, ok := gs.Get(42)
	if !ok {
		t.Fatal("expected to find point 42")
	}
	if got.ID != 42 {
		t.Fatalf("expected ID 42, got %d", got.ID)
	}
	if got.Payload["color"] != "red" {
		t.Fatalf("expected color=red, got %v", got.Payload["color"])
	}
}

func TestGrowingSegment_Search(t *testing.T) {
	gs := NewGrowingSegment(newTestSegmentID(), "test-col", 0, 3, Cosine)
	gs.Insert(Point{ID: 1, Vector: []float32{1, 0, 0}})
	gs.Insert(Point{ID: 2, Vector: []float32{0, 1, 0}})
	gs.Insert(Point{ID: 3, Vector: []float32{0.9, 0.1, 0}})

	results := gs.Search([]float32{1, 0, 0}, 2, nil)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 1 {
		t.Fatalf("closest should be ID 1, got %d", results[0].ID)
	}
}

func TestGrowingSegment_Count(t *testing.T) {
	gs := NewGrowingSegment(newTestSegmentID(), "test-col", 0, 3, Cosine)
	if gs.Count() != 0 {
		t.Fatalf("expected 0, got %d", gs.Count())
	}
	gs.Insert(Point{ID: 1, Vector: []float32{1, 0, 0}})
	gs.Insert(Point{ID: 2, Vector: []float32{0, 1, 0}})
	if gs.Count() != 2 {
		t.Fatalf("expected 2, got %d", gs.Count())
	}
}

func TestSealedSegment_WriteAndRead(t *testing.T) {
	gs := NewGrowingSegment(newTestSegmentID(), "test-col", 0, 3, Cosine)
	rng := mrand.New(mrand.NewSource(42))
	for i := uint64(1); i <= 100; i++ {
		gs.Insert(Point{
			ID:      i,
			Vector:  randomVector(3, rng),
			Payload: map[string]any{"idx": fmt.Sprint(i)},
		})
	}

	be := newSegTestBackend()
	ctx := context.Background()

	sealed, err := SealSegment(ctx, gs, be, "warp-vectors", 16, 50)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}

	if sealed.Meta.PointCount != 100 {
		t.Fatalf("expected 100 points, got %d", sealed.Meta.PointCount)
	}
	if sealed.Meta.State != SegmentIndexed {
		t.Fatalf("expected state indexed, got %s", sealed.Meta.State)
	}

	query := randomVector(3, rng)
	results := sealed.Search(query, 5, 50, nil)
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}
}

func TestSealedSegment_LoadFromBackend(t *testing.T) {
	gs := NewGrowingSegment(newTestSegmentID(), "test-col", 0, 3, Cosine)
	rng := mrand.New(mrand.NewSource(42))
	for i := uint64(1); i <= 20; i++ {
		gs.Insert(Point{ID: i, Vector: randomVector(3, rng)})
	}

	be := newSegTestBackend()
	ctx := context.Background()
	sealed, err := SealSegment(ctx, gs, be, "warp-vectors", 4, 20)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	segID := sealed.Meta.ID

	loaded, err := LoadSealedSegment(ctx, be, "warp-vectors", "test-col", 0, segID, Cosine)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	query := randomVector(3, rng)
	r1 := sealed.Search(query, 5, 20, nil)
	r2 := loaded.Search(query, 5, 20, nil)
	if len(r1) != len(r2) {
		t.Fatalf("result count mismatch: %d vs %d", len(r1), len(r2))
	}
	for i := range r1 {
		if r1[i].ID != r2[i].ID {
			t.Fatalf("result %d: ID %d vs %d", i, r1[i].ID, r2[i].ID)
		}
	}
}

// --- test backend ---

type segTestBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newSegTestBackend() *segTestBackend {
	return &segTestBackend{objects: make(map[string][]byte)}
}

func (m *segTestBackend) PutObject(_ context.Context, bucket, k string, r io.Reader, _ int64, _ map[string]string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[bucket+"/"+k] = data
	return nil
}

func (m *segTestBackend) GetObject(_ context.Context, bucket, k string) (*storage.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[bucket+"/"+k]
	if !ok {
		return nil, fmt.Errorf("not found: %s/%s", bucket, k)
	}
	return &storage.Object{Body: io.NopCloser(bytes.NewReader(data)), Size: int64(len(data))}, nil
}

func (m *segTestBackend) HeadObject(_ context.Context, bucket, k string) (*storage.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[bucket+"/"+k]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return &storage.ObjectInfo{Key: k, Size: int64(len(data))}, nil
}

func (m *segTestBackend) ListObjects(_ context.Context, bucket, prefix, _ string, _ int) (*storage.ListObjectsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pfx := bucket + "/" + prefix
	var contents []storage.ObjectInfo
	for k, v := range m.objects {
		if strings.HasPrefix(k, pfx) {
			contents = append(contents, storage.ObjectInfo{Key: strings.TrimPrefix(k, bucket+"/"), Size: int64(len(v))})
		}
	}
	return &storage.ListObjectsResult{Contents: contents}, nil
}

func (m *segTestBackend) DeleteObject(_ context.Context, bucket, k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, bucket+"/"+k)
	return nil
}

// Unused interface methods
func (m *segTestBackend) ListBuckets(_ context.Context) ([]storage.BucketInfo, error) { return nil, nil }
func (m *segTestBackend) CreateBucket(_ context.Context, _ string) error              { return nil }
func (m *segTestBackend) DeleteBucket(_ context.Context, _ string) error              { return nil }
func (m *segTestBackend) BucketExists(_ context.Context, _ string) (bool, error)      { return true, nil }
func (m *segTestBackend) ListObjectsWithDelimiter(_ context.Context, _, _, _, _ string, _ int) (*storage.ListObjectsResult, error) {
	return nil, fmt.Errorf("unused")
}
func (m *segTestBackend) ListDeletedObjects(_ context.Context, _, _, _ string, _ int) (*storage.ListObjectsResult, error) {
	return nil, fmt.Errorf("unused")
}
func (m *segTestBackend) RestoreObject(_ context.Context, _, _, _ string) error { return fmt.Errorf("unused") }
func (m *segTestBackend) GetObjectACL(_ context.Context, _, _ string) (*storage.ACL, error) {
	return nil, fmt.Errorf("unused")
}
func (m *segTestBackend) PutObjectACL(_ context.Context, _, _ string, _ *storage.ACL) error {
	return fmt.Errorf("unused")
}
func (m *segTestBackend) InitiateMultipartUpload(_ context.Context, _, _ string, _ map[string]string) (string, error) {
	return "", fmt.Errorf("unused")
}
func (m *segTestBackend) UploadPart(_ context.Context, _, _, _ string, _ int, _ io.Reader, _ int64) (string, error) {
	return "", fmt.Errorf("unused")
}
func (m *segTestBackend) CompleteMultipartUpload(_ context.Context, _, _, _ string, _ []storage.CompletedPart) error {
	return fmt.Errorf("unused")
}
func (m *segTestBackend) AbortMultipartUpload(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("unused")
}
func (m *segTestBackend) ListParts(_ context.Context, _, _, _ string, _ int, _ int) (*storage.ListPartsResult, error) {
	return nil, fmt.Errorf("unused")
}

//go:build vector

package vector

import (
	"testing"

	"github.com/oklog/ulid/v2"
)

func TestSegmentCache_PutAndGet(t *testing.T) {
	cache := NewSegmentCache(1024 * 1024)
	meta := SegmentMeta{ID: ulid.Make(), SizeBytes: 1024, PointCount: 10, Distance: Cosine, Dimensions: 3}
	ss := &SealedSegment{Meta: meta}
	cache.Put(meta.ID, ss)

	got, ok := cache.Get(meta.ID)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.Meta.ID != meta.ID {
		t.Fatalf("ID mismatch")
	}
}

func TestSegmentCache_Eviction(t *testing.T) {
	cache := NewSegmentCache(2048)

	id1 := ulid.Make()
	id2 := ulid.Make()
	id3 := ulid.Make()

	cache.Put(id1, &SealedSegment{Meta: SegmentMeta{ID: id1, SizeBytes: 1000}})
	cache.Put(id2, &SealedSegment{Meta: SegmentMeta{ID: id2, SizeBytes: 1000}})

	if _, ok := cache.Get(id1); !ok {
		t.Fatal("id1 should be in cache")
	}
	if _, ok := cache.Get(id2); !ok {
		t.Fatal("id2 should be in cache")
	}

	cache.Put(id3, &SealedSegment{Meta: SegmentMeta{ID: id3, SizeBytes: 1000}})

	if _, ok := cache.Get(id1); ok {
		t.Fatal("id1 should have been evicted")
	}
	if _, ok := cache.Get(id3); !ok {
		t.Fatal("id3 should be in cache")
	}
}

func TestSegmentCache_Remove(t *testing.T) {
	cache := NewSegmentCache(1024 * 1024)
	id := ulid.Make()
	cache.Put(id, &SealedSegment{Meta: SegmentMeta{ID: id, SizeBytes: 100}})
	cache.Remove(id)
	if _, ok := cache.Get(id); ok {
		t.Fatal("expected miss after remove")
	}
}

func TestSegmentCache_Len(t *testing.T) {
	cache := NewSegmentCache(1024 * 1024)
	if cache.Len() != 0 {
		t.Fatalf("expected 0, got %d", cache.Len())
	}
	cache.Put(ulid.Make(), &SealedSegment{Meta: SegmentMeta{SizeBytes: 100}})
	cache.Put(ulid.Make(), &SealedSegment{Meta: SegmentMeta{SizeBytes: 100}})
	if cache.Len() != 2 {
		t.Fatalf("expected 2, got %d", cache.Len())
	}
}

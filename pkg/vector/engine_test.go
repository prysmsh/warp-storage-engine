//go:build vector

package vector

import (
	"context"
	mrand "math/rand"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	be := newSegTestBackend()
	cfg := VectorConfig{
		Bucket:           "warp-vectors",
		SegmentSizeBytes: 4096,
		WALFlushCount:    100,
		HNSWm:           4,
		HNSWefConstruct:  20,
		HNSWefSearch:     10,
		CacheMemoryBytes: 1024 * 1024,
	}
	e, err := NewEngine(be, cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestEngine_CreateCollection(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	err := e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	col, err := e.GetCollection(ctx, "test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if col.Name != "test" || col.Dimensions != 3 {
		t.Fatalf("unexpected collection: %+v", col)
	}
}

func TestEngine_CreateDuplicateCollection(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "dup", Dimensions: 3, Distance: Cosine})
	err := e.CreateCollection(ctx, Collection{Name: "dup", Dimensions: 3, Distance: Cosine})
	if err == nil {
		t.Fatal("expected error on duplicate collection")
	}
}

func TestEngine_InsertAndSearch(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	points := []Point{
		{ID: 1, Vector: []float32{1, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0}},
		{ID: 3, Vector: []float32{0.9, 0.1, 0}},
	}
	if err := e.Insert(ctx, "test", points); err != nil {
		t.Fatalf("insert: %v", err)
	}

	resp, err := e.Search(ctx, SearchRequest{Collection: "test", Vector: []float32{1, 0, 0}, TopK: 2})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Points) != 2 {
		t.Fatalf("expected 2 results, got %d", len(resp.Points))
	}
	if resp.Points[0].ID != 1 {
		t.Fatalf("closest should be ID 1, got %d", resp.Points[0].ID)
	}
}

func TestEngine_SearchWithFilter(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{
		Name: "test", Dimensions: 3, Distance: Cosine,
		Schema: []FieldSchema{{Name: "color", Type: FieldString, Indexed: true}},
	})
	e.Insert(ctx, "test", []Point{
		{ID: 1, Vector: []float32{1, 0, 0}, Payload: map[string]any{"color": "red"}},
		{ID: 2, Vector: []float32{0.9, 0.1, 0}, Payload: map[string]any{"color": "blue"}},
		{ID: 3, Vector: []float32{0.8, 0.2, 0}, Payload: map[string]any{"color": "red"}},
	})

	resp, err := e.Search(ctx, SearchRequest{
		Collection: "test", Vector: []float32{1, 0, 0}, TopK: 10,
		Filter: &Filter{Field: "color", Op: OpEq, Value: "red"},
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Points) != 2 {
		t.Fatalf("expected 2 red results, got %d", len(resp.Points))
	}
	for _, p := range resp.Points {
		if p.Payload["color"] != "red" {
			t.Fatalf("expected color=red, got %v", p.Payload["color"])
		}
	}
}

func TestEngine_Get(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	e.Insert(ctx, "test", []Point{{ID: 42, Vector: []float32{1, 0, 0}, Payload: map[string]any{"x": "y"}}})

	points, err := e.Get(ctx, "test", []PointID{42})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(points) != 1 || points[0].ID != 42 {
		t.Fatalf("unexpected: %+v", points)
	}
}

func TestEngine_Delete(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	e.Insert(ctx, "test", []Point{
		{ID: 1, Vector: []float32{1, 0, 0}},
		{ID: 2, Vector: []float32{0, 1, 0}},
	})

	if err := e.Delete(ctx, "test", []PointID{1}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	points, err := e.Get(ctx, "test", []PointID{1})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(points) != 0 {
		t.Fatalf("expected point 1 to be deleted, got %+v", points)
	}
}

func TestEngine_ListCollections(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "a", Dimensions: 3, Distance: Cosine})
	e.CreateCollection(ctx, Collection{Name: "b", Dimensions: 8, Distance: L2})

	cols, err := e.ListCollections(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2, got %d", len(cols))
	}
}

func TestEngine_DeleteCollection(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	if err := e.DeleteCollection(ctx, "test"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err := e.GetCollection(ctx, "test")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestEngine_LargeInsertAndSearch(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()
	dim := 32

	e.CreateCollection(ctx, Collection{Name: "big", Dimensions: dim, Distance: Cosine})

	rng := mrand.New(mrand.NewSource(42))
	batch := make([]Point, 200)
	for i := range batch {
		batch[i] = Point{ID: uint64(i + 1), Vector: randomVector(dim, rng)}
	}
	if err := e.Insert(ctx, "big", batch); err != nil {
		t.Fatalf("insert: %v", err)
	}

	resp, err := e.Search(ctx, SearchRequest{Collection: "big", Vector: randomVector(dim, rng), TopK: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Points) != 10 {
		t.Fatalf("expected 10 results, got %d", len(resp.Points))
	}
	for i := 1; i < len(resp.Points); i++ {
		if resp.Points[i].Score < resp.Points[i-1].Score {
			t.Fatalf("results not sorted at %d", i)
		}
	}
}

func TestEngine_SearchNonexistentCollection(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	_, err := e.Search(ctx, SearchRequest{Collection: "nope", Vector: []float32{1}, TopK: 1})
	if err == nil {
		t.Fatal("expected error for nonexistent collection")
	}
}

func TestEngine_InsertWrongDimensions(t *testing.T) {
	e := newTestEngine(t)
	ctx := context.Background()

	e.CreateCollection(ctx, Collection{Name: "test", Dimensions: 3, Distance: Cosine})
	err := e.Insert(ctx, "test", []Point{{ID: 1, Vector: []float32{1, 0}}})
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

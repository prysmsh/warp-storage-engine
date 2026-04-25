//go:build vector

package vector

import (
	"context"
	"fmt"
	mrand "math/rand"
	"testing"
)

func benchRandVec(dim int, rng *mrand.Rand) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func benchSetup(b *testing.B, pointCount, dim int) *Engine {
	b.Helper()
	be := newSegTestBackend()
	engine, err := NewEngine(be, VectorConfig{
		Bucket:           "bench",
		SegmentSizeBytes: 64 * 1024 * 1024,
		WALFlushCount:    1000,
		HNSWm:           16,
		HNSWefConstruct:  200,
		HNSWefSearch:     128,
		CacheMemoryBytes: 256 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	engine.CreateCollection(ctx, Collection{Name: "bench", Dimensions: dim, Distance: Cosine})

	rng := mrand.New(mrand.NewSource(42))
	batch := make([]Point, 0, 100)
	for i := 0; i < pointCount; i++ {
		batch = append(batch, Point{
			ID:     uint64(i + 1),
			Vector: benchRandVec(dim, rng),
			Payload: map[string]any{
				"type": fmt.Sprintf("type_%d", i%10),
			},
		})
		if len(batch) == 100 {
			engine.Insert(ctx, "bench", batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		engine.Insert(ctx, "bench", batch)
	}
	return engine
}

// BenchmarkSearch_768d_500pts — AI WAF known-attacks collection size
func BenchmarkSearch_768d_500pts(b *testing.B) {
	engine := benchSetup(b, 500, 768)
	rng := mrand.New(mrand.NewSource(99))
	query := benchRandVec(768, rng)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Search(ctx, SearchRequest{Collection: "bench", Vector: query, TopK: 10})
	}
}

// BenchmarkSearch_768d_5000pts — AI WAF baseline collection size
func BenchmarkSearch_768d_5000pts(b *testing.B) {
	engine := benchSetup(b, 5000, 768)
	rng := mrand.New(mrand.NewSource(99))
	query := benchRandVec(768, rng)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Search(ctx, SearchRequest{Collection: "bench", Vector: query, TopK: 10})
	}
}

// BenchmarkInsert_768d — single point insert latency
func BenchmarkInsert_768d(b *testing.B) {
	be := newSegTestBackend()
	engine, _ := NewEngine(be, VectorConfig{
		Bucket:           "bench",
		SegmentSizeBytes: 64 * 1024 * 1024,
		WALFlushCount:    1000,
		HNSWm:           16,
		HNSWefConstruct:  200,
		CacheMemoryBytes: 256 * 1024 * 1024,
	})
	ctx := context.Background()
	engine.CreateCollection(ctx, Collection{Name: "bench", Dimensions: 768, Distance: Cosine})
	rng := mrand.New(mrand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Insert(ctx, "bench", []Point{{
			ID:     uint64(i + 1),
			Vector: benchRandVec(768, rng),
		}})
	}
}

// BenchmarkSearch_768d_WithFilter — filtered search
func BenchmarkSearch_768d_WithFilter(b *testing.B) {
	engine := benchSetup(b, 5000, 768)
	rng := mrand.New(mrand.NewSource(99))
	query := benchRandVec(768, rng)
	ctx := context.Background()
	filter := &Filter{Field: "type", Op: OpEq, Value: "type_3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.Search(ctx, SearchRequest{Collection: "bench", Vector: query, TopK: 10, Filter: filter})
	}
}

# Warp Vector DB Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundational vector storage layer — types, segments, HNSW index, bitmap index, WAL, and two-tier cache — that powers single-node vector search on top of Warp's `storage.Backend`.

**Architecture:** Data is organized into collections of points (vector + payload). Points are written to growing segments via a WAL. When a segment fills up, it's sealed and an HNSW index is built. Segments are stored as objects in `storage.Backend`. A two-tier LRU cache (memory + SSD) keeps hot segments fast. All search goes through segments: brute-force for growing, HNSW for indexed.

**Tech Stack:** Go 1.24, `storage.Backend` interface, `github.com/RoaringBitmap/roaring` (bitmap indices), `github.com/oklog/ulid/v2` (segment IDs), build tag `//go:build vector`

**Spec:** `docs/superpowers/specs/2026-04-25-vector-db-design.md`

**Subsequent Plans:**
- Plan 2: Cluster Layer (coordinator, worker, gateway, Raft, replication)
- Plan 3: API Layer (gRPC, REST, main.go integration, plugin system)
- Plan 4: Memory Engine Plugin

---

## File Structure

```
pkg/vector/
  types.go          # Collection, Point, Segment, Filter, distance metrics, config types
  types_test.go     # Type validation, filter evaluation
  segment.go        # Segment: growing (in-memory buffer) + sealed (binary format read/write)
  segment_test.go   # Segment write/read/seal round-trip tests
  wal.go            # Write-ahead log: append, flush to backend, replay on startup
  wal_test.go       # WAL append/flush/replay tests
  hnsw.go           # Pure Go HNSW: insert, search, serialize/deserialize
  hnsw_test.go      # HNSW accuracy and performance tests
  bitmap.go         # Roaring bitmap payload index: build, query, serialize
  bitmap_test.go    # Bitmap index filter tests
  filter.go         # Filter AST evaluation against bitmap index
  filter_test.go    # Compound filter evaluation tests
  cache.go          # Two-tier LRU cache: memory + SSD, load from backend
  cache_test.go     # Cache eviction, hit/miss, tier promotion tests
  distance.go       # Cosine, L2, inner product distance functions
  distance_test.go  # Distance calculation correctness tests
  engine.go         # Single-node VectorEngine: ties everything together
  engine_test.go    # End-to-end: create collection, insert, search, filter
```

---

### Task 1: Types and Distance Functions

**Files:**
- Create: `pkg/vector/types.go`
- Create: `pkg/vector/distance.go`
- Create: `pkg/vector/distance_test.go`

- [ ] **Step 1: Write distance function tests**

```go
// pkg/vector/distance_test.go
//go:build vector

package vector

import (
	"math"
	"testing"
)

func TestCosineDistance_IdenticalVectors(t *testing.T) {
	a := []float32{1, 2, 3}
	d := CosineDistance(a, a)
	if d > 1e-6 {
		t.Fatalf("identical vectors should have distance ~0, got %f", d)
	}
}

func TestCosineDistance_OrthogonalVectors(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-1.0) > 1e-6 {
		t.Fatalf("orthogonal vectors should have distance ~1, got %f", d)
	}
}

func TestCosineDistance_OppositeVectors(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{-1, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-2.0) > 1e-6 {
		t.Fatalf("opposite vectors should have distance ~2, got %f", d)
	}
}

func TestL2Distance(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{3, 4, 0}
	d := L2Distance(a, b)
	if math.Abs(float64(d)-25.0) > 1e-6 {
		t.Fatalf("expected squared L2 distance 25, got %f", d)
	}
}

func TestInnerProductDistance(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	d := InnerProductDistance(a, b)
	// dot = 4+10+18 = 32, distance = -32 (negative because higher dot = closer)
	if math.Abs(float64(d)-(-32.0)) > 1e-6 {
		t.Fatalf("expected inner product distance -32, got %f", d)
	}
}

func TestDistanceFuncFor(t *testing.T) {
	tests := []struct {
		metric DistanceMetric
		wantOk bool
	}{
		{Cosine, true},
		{L2, true},
		{InnerProduct, true},
		{DistanceMetric("unknown"), false},
	}
	for _, tt := range tests {
		fn, err := DistanceFuncFor(tt.metric)
		if tt.wantOk && err != nil {
			t.Fatalf("expected ok for %s, got error: %v", tt.metric, err)
		}
		if !tt.wantOk && err == nil {
			t.Fatalf("expected error for %s, got fn: %v", tt.metric, fn)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestCosine -v`
Expected: FAIL — package doesn't exist yet

- [ ] **Step 3: Implement types and distance functions**

```go
// pkg/vector/types.go
//go:build vector

package vector

import (
	"time"

	"github.com/oklog/ulid/v2"
)

// DistanceMetric defines the similarity measure for vector comparison.
type DistanceMetric string

const (
	Cosine       DistanceMetric = "cosine"
	L2           DistanceMetric = "l2"
	InnerProduct DistanceMetric = "ip"
)

// ConsistencyLevel controls write acknowledgment guarantees.
type ConsistencyLevel string

const (
	Eventual ConsistencyLevel = "eventual"
	Strong   ConsistencyLevel = "strong"
)

// FieldType defines the data type of a payload field.
type FieldType string

const (
	FieldInt64       FieldType = "int64"
	FieldFloat64     FieldType = "float64"
	FieldString      FieldType = "string"
	FieldBool        FieldType = "bool"
	FieldStringArray FieldType = "string[]"
)

// FieldSchema defines a typed, optionally indexed payload field.
type FieldSchema struct {
	Name    string    `json:"name"`
	Type    FieldType `json:"type"`
	Indexed bool      `json:"indexed"`
}

// Collection is a named group of vectors with a fixed schema.
type Collection struct {
	Name        string           `json:"name"`
	Dimensions  int              `json:"dimensions"`
	Distance    DistanceMetric   `json:"distance"`
	Schema      []FieldSchema    `json:"schema"`
	Replication int              `json:"replication"`
	Consistency ConsistencyLevel `json:"consistency"`
	ShardCount  int              `json:"shard_count"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// PointID is a uint64 identifier for a vector point.
type PointID = uint64

// Point is a single vector with its payload.
type Point struct {
	ID      PointID        `json:"id"`
	Vector  []float32      `json:"vector"`
	Payload map[string]any `json:"payload,omitempty"`
}

// ScoredPoint is a Point with a distance score from a query.
type ScoredPoint struct {
	Point
	Score float32 `json:"score"`
}

// SegmentID is a time-sortable unique identifier for segments.
type SegmentID = ulid.ULID

// SegmentState tracks the lifecycle of a segment.
type SegmentState string

const (
	SegmentGrowing   SegmentState = "growing"
	SegmentSealed    SegmentState = "sealed"
	SegmentIndexed   SegmentState = "indexed"
	SegmentOffloaded SegmentState = "offloaded"
)

// SegmentMeta holds metadata about a segment, persisted as meta.json.
type SegmentMeta struct {
	ID         SegmentID    `json:"id"`
	Collection string       `json:"collection"`
	Shard      int          `json:"shard"`
	State      SegmentState `json:"state"`
	PointCount int          `json:"point_count"`
	SizeBytes  int64        `json:"size_bytes"`
	Dimensions int          `json:"dimensions"`
	Distance   DistanceMetric `json:"distance"`
	CreatedAt  time.Time    `json:"created_at"`
	SealedAt   *time.Time   `json:"sealed_at,omitempty"`
}

// FilterOp defines a comparison operation for payload filtering.
type FilterOp string

const (
	OpEq       FilterOp = "eq"
	OpNeq      FilterOp = "neq"
	OpGt       FilterOp = "gt"
	OpGte      FilterOp = "gte"
	OpLt       FilterOp = "lt"
	OpLte      FilterOp = "lte"
	OpIn       FilterOp = "in"
	OpContains FilterOp = "contains"
)

// Filter is a recursive filter expression for payload queries.
type Filter struct {
	And   []Filter `json:"and,omitempty"`
	Or    []Filter `json:"or,omitempty"`
	Not   *Filter  `json:"not,omitempty"`
	Field string   `json:"field,omitempty"`
	Op    FilterOp `json:"op,omitempty"`
	Value any      `json:"value,omitempty"`
}

// SearchRequest defines a vector search query.
type SearchRequest struct {
	Collection string   `json:"collection"`
	Vector     []float32 `json:"vector"`
	TopK       int      `json:"top_k"`
	Filter     *Filter  `json:"filter,omitempty"`
	EfSearch   int      `json:"ef_search,omitempty"`
}

// SearchResponse contains ranked search results.
type SearchResponse struct {
	Points   []ScoredPoint `json:"points"`
	SearchMs int64         `json:"search_ms"`
}

// DistanceFunc computes the distance between two vectors.
type DistanceFunc func(a, b []float32) float32

// VectorConfig holds configuration for the vector DB engine.
type VectorConfig struct {
	Bucket           string `mapstructure:"bucket" envconfig:"VECTOR_BUCKET" default:"warp-vectors"`
	SegmentSizeBytes int64  `mapstructure:"segment_size" envconfig:"VECTOR_SEGMENT_SIZE" default:"67108864"`
	WALFlushInterval time.Duration `mapstructure:"wal_flush_interval" envconfig:"VECTOR_WAL_FLUSH_INTERVAL" default:"100ms"`
	WALFlushCount    int    `mapstructure:"wal_flush_count" envconfig:"VECTOR_WAL_FLUSH_COUNT" default:"1000"`
	HNSWm            int    `mapstructure:"hnsw_m" envconfig:"VECTOR_HNSW_M" default:"16"`
	HNSWefConstruct  int    `mapstructure:"hnsw_ef_construction" envconfig:"VECTOR_HNSW_EF_CONSTRUCTION" default:"200"`
	HNSWefSearch     int    `mapstructure:"hnsw_ef_search" envconfig:"VECTOR_HNSW_EF_SEARCH" default:"128"`
	CacheMemoryBytes int64  `mapstructure:"cache_memory_budget" envconfig:"VECTOR_CACHE_MEMORY_BUDGET" default:"1073741824"`
	CacheSSDPath     string `mapstructure:"cache_ssd_path" envconfig:"VECTOR_CACHE_SSD_PATH" default:""`
	CacheSSDBytes    int64  `mapstructure:"cache_ssd_budget" envconfig:"VECTOR_CACHE_SSD_BUDGET" default:"10737418240"`
}
```

```go
// pkg/vector/distance.go
//go:build vector

package vector

import (
	"fmt"
	"math"
)

// CosineDistance returns 1 - cosine_similarity(a, b). Range: [0, 2].
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	denom := float32(math.Sqrt(float64(normA)) * math.Sqrt(float64(normB)))
	if denom == 0 {
		return 1
	}
	return 1 - dot/denom
}

// L2Distance returns the squared Euclidean distance between a and b.
func L2Distance(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}

// InnerProductDistance returns the negative dot product (lower = more similar).
func InnerProductDistance(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}

// DistanceFuncFor returns the distance function for the given metric.
func DistanceFuncFor(metric DistanceMetric) (DistanceFunc, error) {
	switch metric {
	case Cosine:
		return CosineDistance, nil
	case L2:
		return L2Distance, nil
	case InnerProduct:
		return InnerProductDistance, nil
	default:
		return nil, fmt.Errorf("unsupported distance metric: %s", metric)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run "TestCosine|TestL2|TestInnerProduct|TestDistanceFunc" -v`
Expected: PASS (all 6 tests)

- [ ] **Step 5: Add dependencies**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go get github.com/oklog/ulid/v2 github.com/RoaringBitmap/roaring/v2@latest && go mod tidy`

- [ ] **Step 6: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/types.go pkg/vector/distance.go pkg/vector/distance_test.go go.mod go.sum
git commit -m "feat(vector): add core types and distance functions"
```

---

### Task 2: HNSW Vector Index

**Files:**
- Create: `pkg/vector/hnsw.go`
- Create: `pkg/vector/hnsw_test.go`

- [ ] **Step 1: Write HNSW tests**

```go
// pkg/vector/hnsw_test.go
//go:build vector

package vector

import (
	"math/rand"
	"testing"
)

func randomVector(dim int, rng *rand.Rand) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

func TestHNSW_InsertAndSearchSingle(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	idx.Insert(1, []float32{1, 0, 0})
	results := idx.Search([]float32{1, 0, 0}, 1, 10, nil)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != 1 {
		t.Fatalf("expected ID 1, got %d", results[0].ID)
	}
	if results[0].Distance > 1e-6 {
		t.Fatalf("expected distance ~0, got %f", results[0].Distance)
	}
}

func TestHNSW_SearchTopK(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	idx.Insert(1, []float32{1, 0, 0})
	idx.Insert(2, []float32{0.9, 0.1, 0})
	idx.Insert(3, []float32{0, 1, 0})
	idx.Insert(4, []float32{0, 0, 1})

	results := idx.Search([]float32{1, 0, 0}, 2, 10, nil)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 1 {
		t.Fatalf("closest should be ID 1, got %d", results[0].ID)
	}
	if results[1].ID != 2 {
		t.Fatalf("second closest should be ID 2, got %d", results[1].ID)
	}
}

func TestHNSW_ResultsAreSortedByDistance(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	rng := rand.New(rand.NewSource(42))
	for i := uint64(1); i <= 100; i++ {
		idx.Insert(i, randomVector(3, rng))
	}
	results := idx.Search(randomVector(3, rng), 10, 50, nil)
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Fatalf("results not sorted at index %d: %f < %f", i, results[i].Distance, results[i-1].Distance)
		}
	}
}

func TestHNSW_FilteredSearch(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	idx.Insert(1, []float32{1, 0, 0})
	idx.Insert(2, []float32{0.9, 0.1, 0})
	idx.Insert(3, []float32{0.8, 0.2, 0})

	// Only allow IDs 2 and 3
	allow := map[uint64]struct{}{2: {}, 3: {}}
	results := idx.Search([]float32{1, 0, 0}, 2, 10, allow)
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ID != 2 {
		t.Fatalf("closest allowed should be ID 2, got %d", results[0].ID)
	}
}

func TestHNSW_EmptyIndex(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	results := idx.Search([]float32{1, 0, 0}, 5, 10, nil)
	if len(results) != 0 {
		t.Fatalf("expected 0 results from empty index, got %d", len(results))
	}
}

func TestHNSW_SerializeDeserialize(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	rng := rand.New(rand.NewSource(99))
	for i := uint64(1); i <= 50; i++ {
		idx.Insert(i, randomVector(3, rng))
	}

	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	idx2, err := DeserializeHNSWIndex(data, CosineDistance)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	query := randomVector(3, rng)
	r1 := idx.Search(query, 5, 50, nil)
	r2 := idx2.Search(query, 5, 50, nil)
	if len(r1) != len(r2) {
		t.Fatalf("result count mismatch: %d vs %d", len(r1), len(r2))
	}
	for i := range r1 {
		if r1[i].ID != r2[i].ID {
			t.Fatalf("result %d ID mismatch: %d vs %d", i, r1[i].ID, r2[i].ID)
		}
	}
}

func TestHNSW_Recall1000(t *testing.T) {
	dim := 32
	n := 1000
	rng := rand.New(rand.NewSource(42))

	idx := NewHNSWIndex(dim, 16, 200, CosineDistance)
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = randomVector(dim, rng)
		idx.Insert(uint64(i+1), vectors[i])
	}

	// Brute force top-10 for a random query
	query := randomVector(dim, rng)
	type idDist struct {
		id   uint64
		dist float32
	}
	var brute []idDist
	for i, v := range vectors {
		brute = append(brute, idDist{uint64(i + 1), CosineDistance(query, v)})
	}
	// Sort brute force results
	for i := 0; i < len(brute); i++ {
		for j := i + 1; j < len(brute); j++ {
			if brute[j].dist < brute[i].dist {
				brute[i], brute[j] = brute[j], brute[i]
			}
		}
	}
	topK := 10
	bruteTopIDs := make(map[uint64]struct{})
	for i := 0; i < topK && i < len(brute); i++ {
		bruteTopIDs[brute[i].id] = struct{}{}
	}

	results := idx.Search(query, topK, 128, nil)
	hits := 0
	for _, r := range results {
		if _, ok := bruteTopIDs[r.ID]; ok {
			hits++
		}
	}
	recall := float64(hits) / float64(topK)
	if recall < 0.8 {
		t.Fatalf("recall@10 too low: %.2f (expected >= 0.8)", recall)
	}
}

func TestHNSW_Len(t *testing.T) {
	idx := NewHNSWIndex(3, 4, 20, CosineDistance)
	if idx.Len() != 0 {
		t.Fatalf("expected len 0, got %d", idx.Len())
	}
	idx.Insert(1, []float32{1, 0, 0})
	idx.Insert(2, []float32{0, 1, 0})
	if idx.Len() != 2 {
		t.Fatalf("expected len 2, got %d", idx.Len())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestHNSW -v`
Expected: FAIL — `NewHNSWIndex` undefined

- [ ] **Step 3: Implement HNSW index**

```go
// pkg/vector/hnsw.go
//go:build vector

package vector

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// HNSWSearchResult holds a point ID and its distance from the query.
type HNSWSearchResult struct {
	ID       uint64
	Distance float32
}

// hnswNode represents a single node in the HNSW graph.
type hnswNode struct {
	id      uint64
	vector  []float32
	level   int
	friends [][]uint64 // friends[layer] = list of neighbor IDs at that layer
}

// HNSWIndex is a pure-Go HNSW (Hierarchical Navigable Small World) vector index.
type HNSWIndex struct {
	mu           sync.RWMutex
	nodes        map[uint64]*hnswNode
	entryPoint   uint64
	hasEntry     bool
	maxLevel     int
	m            int // max connections per layer
	mMax0        int // max connections at layer 0 (2 * m)
	efConstruct  int
	dim          int
	distFunc     DistanceFunc
	levelMult    float64
	rng          *rand.Rand
}

// NewHNSWIndex creates a new HNSW index.
// dim: vector dimensionality
// m: max connections per layer (mMax0 = 2*m for layer 0)
// efConstruction: size of dynamic candidate list during construction
// distFunc: distance function to use
func NewHNSWIndex(dim, m, efConstruction int, distFunc DistanceFunc) *HNSWIndex {
	return &HNSWIndex{
		nodes:       make(map[uint64]*hnswNode),
		m:           m,
		mMax0:       2 * m,
		efConstruct: efConstruction,
		dim:         dim,
		distFunc:    distFunc,
		levelMult:   1.0 / math.Log(float64(m)),
		rng:         rand.New(rand.NewSource(rand.Int63())),
	}
}

// Len returns the number of points in the index.
func (h *HNSWIndex) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// Insert adds a point to the index.
func (h *HNSWIndex) Insert(id uint64, vector []float32) {
	h.mu.Lock()
	defer h.mu.Unlock()

	level := h.randomLevel()
	node := &hnswNode{
		id:      id,
		vector:  vector,
		level:   level,
		friends: make([][]uint64, level+1),
	}
	for i := range node.friends {
		node.friends[i] = nil
	}
	h.nodes[id] = node

	if !h.hasEntry {
		h.entryPoint = id
		h.hasEntry = true
		h.maxLevel = level
		return
	}

	ep := h.entryPoint
	epDist := h.distFunc(vector, h.nodes[ep].vector)

	// Traverse from top to the level just above the new node's level
	for lc := h.maxLevel; lc > level; lc-- {
		changed := true
		for changed {
			changed = false
			friends := h.nodes[ep].friends
			if lc < len(friends) {
				for _, fid := range friends[lc] {
					if fNode, ok := h.nodes[fid]; ok {
						d := h.distFunc(vector, fNode.vector)
						if d < epDist {
							ep = fid
							epDist = d
							changed = true
						}
					}
				}
			}
		}
	}

	// For each level from min(level, maxLevel) down to 0, find and connect neighbors
	for lc := min(level, h.maxLevel); lc >= 0; lc-- {
		candidates := h.searchLayer(vector, ep, h.efConstruct, lc, nil)
		mMax := h.m
		if lc == 0 {
			mMax = h.mMax0
		}
		neighbors := h.selectNeighborsSimple(candidates, mMax)

		node.friends[lc] = make([]uint64, len(neighbors))
		for i, n := range neighbors {
			node.friends[lc][i] = n.ID
		}

		// Add bidirectional connections
		for _, n := range neighbors {
			friendNode := h.nodes[n.ID]
			if lc >= len(friendNode.friends) {
				continue
			}
			friendNode.friends[lc] = append(friendNode.friends[lc], id)
			if len(friendNode.friends[lc]) > mMax {
				// Shrink connections
				var friendCandidates []HNSWSearchResult
				for _, fid := range friendNode.friends[lc] {
					if fn, ok := h.nodes[fid]; ok {
						friendCandidates = append(friendCandidates, HNSWSearchResult{
							ID:       fid,
							Distance: h.distFunc(friendNode.vector, fn.vector),
						})
					}
				}
				trimmed := h.selectNeighborsSimple(friendCandidates, mMax)
				friendNode.friends[lc] = make([]uint64, len(trimmed))
				for i, tr := range trimmed {
					friendNode.friends[lc][i] = tr.ID
				}
			}
		}

		if len(candidates) > 0 {
			ep = candidates[0].ID
		}
	}

	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPoint = id
	}
}

// Search finds the top-k nearest neighbors to the query vector.
// ef: search beam width (higher = more accurate, slower)
// allow: if non-nil, only return IDs present in this set
func (h *HNSWIndex) Search(query []float32, topK, ef int, allow map[uint64]struct{}) []HNSWSearchResult {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if !h.hasEntry {
		return nil
	}

	ep := h.entryPoint

	// Traverse from top to layer 1
	for lc := h.maxLevel; lc > 0; lc-- {
		changed := true
		epDist := h.distFunc(query, h.nodes[ep].vector)
		for changed {
			changed = false
			if epNode, ok := h.nodes[ep]; ok && lc < len(epNode.friends) {
				for _, fid := range epNode.friends[lc] {
					if fNode, ok := h.nodes[fid]; ok {
						d := h.distFunc(query, fNode.vector)
						if d < epDist {
							ep = fid
							epDist = d
							changed = true
						}
					}
				}
			}
		}
	}

	candidates := h.searchLayer(query, ep, max(ef, topK), 0, nil)

	// Apply allow filter
	if allow != nil {
		filtered := candidates[:0]
		for _, c := range candidates {
			if _, ok := allow[c.ID]; ok {
				filtered = append(filtered, c)
			}
		}
		candidates = filtered
	}

	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	return candidates
}

// searchLayer performs a greedy beam search on a single layer.
func (h *HNSWIndex) searchLayer(query []float32, entryID uint64, ef, layer int, allow map[uint64]struct{}) []HNSWSearchResult {
	visited := make(map[uint64]struct{})
	visited[entryID] = struct{}{}

	entryDist := h.distFunc(query, h.nodes[entryID].vector)

	candidates := &minHeap{{ID: entryID, Distance: entryDist}}
	heap.Init(candidates)

	results := &maxHeap{{ID: entryID, Distance: entryDist}}
	heap.Init(results)

	for candidates.Len() > 0 {
		closest := heap.Pop(candidates).(HNSWSearchResult)
		farthestResult := (*results)[0]

		if closest.Distance > farthestResult.Distance {
			break
		}

		node := h.nodes[closest.ID]
		if layer >= len(node.friends) {
			continue
		}

		for _, fid := range node.friends[layer] {
			if _, ok := visited[fid]; ok {
				continue
			}
			visited[fid] = struct{}{}

			fNode, ok := h.nodes[fid]
			if !ok {
				continue
			}
			d := h.distFunc(query, fNode.vector)
			farthestResult = (*results)[0]

			if d < farthestResult.Distance || results.Len() < ef {
				heap.Push(candidates, HNSWSearchResult{ID: fid, Distance: d})
				heap.Push(results, HNSWSearchResult{ID: fid, Distance: d})
				if results.Len() > ef {
					heap.Pop(results)
				}
			}
		}
	}

	// Extract results sorted by distance (ascending)
	out := make([]HNSWSearchResult, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(HNSWSearchResult)
	}
	return out
}

func (h *HNSWIndex) selectNeighborsSimple(candidates []HNSWSearchResult, m int) []HNSWSearchResult {
	// Sort by distance ascending using selection sort (small m)
	for i := 0; i < len(candidates) && i < m; i++ {
		minIdx := i
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].Distance < candidates[minIdx].Distance {
				minIdx = j
			}
		}
		candidates[i], candidates[minIdx] = candidates[minIdx], candidates[i]
	}
	if len(candidates) > m {
		return candidates[:m]
	}
	return candidates
}

func (h *HNSWIndex) randomLevel() int {
	level := 0
	for h.rng.Float64() < 1.0/math.E && level < 32 {
		level++
	}
	return level
}

// Serialize encodes the HNSW index to a binary format.
func (h *HNSWIndex) Serialize() ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Format: [dim:4][m:4][efConstruct:4][maxLevel:4][entryPoint:8][hasEntry:1][nodeCount:4]
	// Then for each node: [id:8][level:4][vectorLen:4][vector...][friendsPerLevel...]
	buf := make([]byte, 0, 1024*1024)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(h.dim))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(h.m))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(h.efConstruct))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(h.maxLevel))
	buf = binary.LittleEndian.AppendUint64(buf, h.entryPoint)
	if h.hasEntry {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(h.nodes)))

	for _, node := range h.nodes {
		buf = binary.LittleEndian.AppendUint64(buf, node.id)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(node.level))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(node.vector)))
		for _, v := range node.vector {
			buf = binary.LittleEndian.AppendUint32(buf, math.Float32bits(v))
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(node.friends)))
		for _, layer := range node.friends {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(layer)))
			for _, fid := range layer {
				buf = binary.LittleEndian.AppendUint64(buf, fid)
			}
		}
	}

	return buf, nil
}

// DeserializeHNSWIndex reconstructs an HNSW index from binary data.
func DeserializeHNSWIndex(data []byte, distFunc DistanceFunc) (*HNSWIndex, error) {
	if len(data) < 25 {
		return nil, fmt.Errorf("data too short for HNSW header")
	}
	off := 0

	readU32 := func() uint32 {
		v := binary.LittleEndian.Uint32(data[off:])
		off += 4
		return v
	}
	readU64 := func() uint64 {
		v := binary.LittleEndian.Uint64(data[off:])
		off += 8
		return v
	}

	dim := int(readU32())
	m := int(readU32())
	efConstruct := int(readU32())
	maxLevel := int(readU32())
	entryPoint := readU64()
	hasEntry := data[off] == 1
	off++
	nodeCount := int(readU32())

	h := &HNSWIndex{
		nodes:       make(map[uint64]*hnswNode, nodeCount),
		entryPoint:  entryPoint,
		hasEntry:    hasEntry,
		maxLevel:    maxLevel,
		m:           m,
		mMax0:       2 * m,
		efConstruct: efConstruct,
		dim:         dim,
		distFunc:    distFunc,
		levelMult:   1.0 / math.Log(float64(m)),
		rng:         rand.New(rand.NewSource(rand.Int63())),
	}

	for i := 0; i < nodeCount; i++ {
		id := readU64()
		level := int(readU32())
		vecLen := int(readU32())
		vec := make([]float32, vecLen)
		for j := range vec {
			vec[j] = math.Float32frombits(readU32())
		}
		friendLayers := int(readU32())
		friends := make([][]uint64, friendLayers)
		for l := 0; l < friendLayers; l++ {
			count := int(readU32())
			friends[l] = make([]uint64, count)
			for j := range friends[l] {
				friends[l][j] = readU64()
			}
		}
		h.nodes[id] = &hnswNode{
			id:      id,
			vector:  vec,
			level:   level,
			friends: friends,
		}
	}

	return h, nil
}

// --- heap implementations for search ---

type minHeap []HNSWSearchResult

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool   { return h[i].Distance < h[j].Distance }
func (h minHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{})  { *h = append(*h, x.(HNSWSearchResult)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type maxHeap []HNSWSearchResult

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool   { return h[i].Distance > h[j].Distance }
func (h maxHeap) Swap(i, j int)        { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{})  { *h = append(*h, x.(HNSWSearchResult)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestHNSW -v`
Expected: PASS (all 8 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/hnsw.go pkg/vector/hnsw_test.go
git commit -m "feat(vector): add pure Go HNSW vector index"
```

---

### Task 3: Bitmap Payload Index and Filter Evaluation

**Files:**
- Create: `pkg/vector/bitmap.go`
- Create: `pkg/vector/bitmap_test.go`
- Create: `pkg/vector/filter.go`
- Create: `pkg/vector/filter_test.go`

- [ ] **Step 1: Write bitmap index tests**

```go
// pkg/vector/bitmap_test.go
//go:build vector

package vector

import (
	"testing"
)

func TestBitmapIndex_StringField(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "red")

	ids := idx.Get("color", "red")
	if !ids.Contains(1) || !ids.Contains(3) {
		t.Fatalf("expected IDs 1 and 3 for color=red, got %v", ids.ToArray())
	}
	if ids.Contains(2) {
		t.Fatalf("ID 2 should not match color=red")
	}
}

func TestBitmapIndex_Intersection(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(1, "size", "large")
	idx.Add(2, "color", "red")
	idx.Add(2, "size", "small")
	idx.Add(3, "color", "blue")
	idx.Add(3, "size", "large")

	colorRed := idx.Get("color", "red")
	sizeLarge := idx.Get("size", "large")
	colorRed.And(sizeLarge)
	if colorRed.GetCardinality() != 1 || !colorRed.Contains(1) {
		t.Fatalf("expected only ID 1 for color=red AND size=large, got %v", colorRed.ToArray())
	}
}

func TestBitmapIndex_SerializeDeserialize(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "tag", "backend")
	idx.Add(2, "tag", "frontend")
	idx.Add(3, "tag", "backend")

	data, err := idx.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	idx2, err := DeserializeBitmapIndex(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	ids := idx2.Get("tag", "backend")
	if !ids.Contains(1) || !ids.Contains(3) || ids.Contains(2) {
		t.Fatalf("deserialized index mismatch, got %v", ids.ToArray())
	}
}

func TestBitmapIndex_AllIDs(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "a", "x")
	idx.Add(2, "b", "y")
	idx.Add(3, "a", "z")

	all := idx.AllIDs()
	if all.GetCardinality() != 3 {
		t.Fatalf("expected 3 IDs, got %d", all.GetCardinality())
	}
}
```

- [ ] **Step 2: Write filter evaluation tests**

```go
// pkg/vector/filter_test.go
//go:build vector

package vector

import (
	"testing"
)

func TestEvalFilter_SimpleEq(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "status", "active")
	idx.Add(2, "status", "inactive")
	idx.Add(3, "status", "active")

	f := Filter{Field: "status", Op: OpEq, Value: "active"}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(3) {
		t.Fatalf("expected IDs 1,3 for status=active, got %v", result.ToArray())
	}
}

func TestEvalFilter_Neq(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "status", "active")
	idx.Add(2, "status", "inactive")
	idx.Add(3, "status", "active")

	f := Filter{Field: "status", Op: OpNeq, Value: "active"}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(2) {
		t.Fatalf("expected ID 2 for status!=active, got %v", result.ToArray())
	}
}

func TestEvalFilter_And(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(1, "size", "large")
	idx.Add(2, "color", "red")
	idx.Add(2, "size", "small")

	f := Filter{
		And: []Filter{
			{Field: "color", Op: OpEq, Value: "red"},
			{Field: "size", Op: OpEq, Value: "large"},
		},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(1) {
		t.Fatalf("expected ID 1, got %v", result.ToArray())
	}
}

func TestEvalFilter_Or(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "green")

	f := Filter{
		Or: []Filter{
			{Field: "color", Op: OpEq, Value: "red"},
			{Field: "color", Op: OpEq, Value: "blue"},
		},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(2) {
		t.Fatalf("expected IDs 1,2, got %v", result.ToArray())
	}
}

func TestEvalFilter_Not(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "red")

	f := Filter{
		Not: &Filter{Field: "color", Op: OpEq, Value: "red"},
	}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 1 || !result.Contains(2) {
		t.Fatalf("expected ID 2 for NOT color=red, got %v", result.ToArray())
	}
}

func TestEvalFilter_In(t *testing.T) {
	idx := NewBitmapIndex()
	idx.Add(1, "color", "red")
	idx.Add(2, "color", "blue")
	idx.Add(3, "color", "green")

	f := Filter{Field: "color", Op: OpIn, Value: []any{"red", "green"}}
	result := EvalFilter(f, idx)
	if result.GetCardinality() != 2 || !result.Contains(1) || !result.Contains(3) {
		t.Fatalf("expected IDs 1,3, got %v", result.ToArray())
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run "TestBitmap|TestEvalFilter" -v`
Expected: FAIL — `NewBitmapIndex` undefined

- [ ] **Step 4: Implement bitmap index**

```go
// pkg/vector/bitmap.go
//go:build vector

package vector

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// BitmapIndex maintains roaring bitmaps per field+value for fast filtering.
type BitmapIndex struct {
	// bitmaps[field][value] = set of point IDs
	bitmaps map[string]map[string]*roaring.Bitmap
	allIDs  *roaring.Bitmap
}

// NewBitmapIndex creates an empty bitmap index.
func NewBitmapIndex() *BitmapIndex {
	return &BitmapIndex{
		bitmaps: make(map[string]map[string]*roaring.Bitmap),
		allIDs:  roaring.New(),
	}
}

// Add registers that the given point has the given field=value.
func (b *BitmapIndex) Add(id uint64, field, value string) {
	if _, ok := b.bitmaps[field]; !ok {
		b.bitmaps[field] = make(map[string]*roaring.Bitmap)
	}
	if _, ok := b.bitmaps[field][value]; !ok {
		b.bitmaps[field][value] = roaring.New()
	}
	// Roaring bitmap uses uint32 internally; cast safely for IDs < 2^32
	b.bitmaps[field][value].Add(uint32(id))
	b.allIDs.Add(uint32(id))
}

// Get returns the bitmap of IDs matching field=value. Returns empty bitmap if not found.
func (b *BitmapIndex) Get(field, value string) *roaring.Bitmap {
	if vals, ok := b.bitmaps[field]; ok {
		if bm, ok := vals[value]; ok {
			return bm.Clone()
		}
	}
	return roaring.New()
}

// AllIDs returns a bitmap of all known point IDs.
func (b *BitmapIndex) AllIDs() *roaring.Bitmap {
	return b.allIDs.Clone()
}

// Serialize encodes the bitmap index to bytes.
func (b *BitmapIndex) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write allIDs
	allBytes, err := b.allIDs.ToBytes()
	if err != nil {
		return nil, fmt.Errorf("serialize allIDs: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(allBytes))); err != nil {
		return nil, err
	}
	buf.Write(allBytes)

	// Write field count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(b.bitmaps))); err != nil {
		return nil, err
	}

	for field, vals := range b.bitmaps {
		// Write field name
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(field))); err != nil {
			return nil, err
		}
		buf.WriteString(field)

		// Write value count
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(vals))); err != nil {
			return nil, err
		}

		for value, bm := range vals {
			// Write value name
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(value))); err != nil {
				return nil, err
			}
			buf.WriteString(value)

			// Write bitmap
			bmBytes, err := bm.ToBytes()
			if err != nil {
				return nil, fmt.Errorf("serialize bitmap %s=%s: %w", field, value, err)
			}
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(bmBytes))); err != nil {
				return nil, err
			}
			buf.Write(bmBytes)
		}
	}

	return buf.Bytes(), nil
}

// DeserializeBitmapIndex reconstructs a bitmap index from bytes.
func DeserializeBitmapIndex(data []byte) (*BitmapIndex, error) {
	r := bytes.NewReader(data)
	idx := &BitmapIndex{
		bitmaps: make(map[string]map[string]*roaring.Bitmap),
	}

	// Read allIDs
	var allLen uint32
	if err := binary.Read(r, binary.LittleEndian, &allLen); err != nil {
		return nil, fmt.Errorf("read allIDs length: %w", err)
	}
	allBytes := make([]byte, allLen)
	if _, err := r.Read(allBytes); err != nil {
		return nil, fmt.Errorf("read allIDs: %w", err)
	}
	idx.allIDs = roaring.New()
	if _, err := idx.allIDs.FromBuffer(allBytes); err != nil {
		return nil, fmt.Errorf("deserialize allIDs: %w", err)
	}

	var fieldCount uint32
	if err := binary.Read(r, binary.LittleEndian, &fieldCount); err != nil {
		return nil, fmt.Errorf("read field count: %w", err)
	}

	for i := uint32(0); i < fieldCount; i++ {
		var nameLen uint32
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}
		nameBytes := make([]byte, nameLen)
		if _, err := r.Read(nameBytes); err != nil {
			return nil, err
		}
		field := string(nameBytes)
		idx.bitmaps[field] = make(map[string]*roaring.Bitmap)

		var valCount uint32
		if err := binary.Read(r, binary.LittleEndian, &valCount); err != nil {
			return nil, err
		}

		for j := uint32(0); j < valCount; j++ {
			var vLen uint32
			if err := binary.Read(r, binary.LittleEndian, &vLen); err != nil {
				return nil, err
			}
			vBytes := make([]byte, vLen)
			if _, err := r.Read(vBytes); err != nil {
				return nil, err
			}
			value := string(vBytes)

			var bmLen uint32
			if err := binary.Read(r, binary.LittleEndian, &bmLen); err != nil {
				return nil, err
			}
			bmBytes := make([]byte, bmLen)
			if _, err := r.Read(bmBytes); err != nil {
				return nil, err
			}
			bm := roaring.New()
			if _, err := bm.FromBuffer(bmBytes); err != nil {
				return nil, fmt.Errorf("deserialize bitmap %s=%s: %w", field, value, err)
			}
			idx.bitmaps[field][value] = bm
		}
	}

	return idx, nil
}
```

- [ ] **Step 5: Implement filter evaluation**

```go
// pkg/vector/filter.go
//go:build vector

package vector

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// EvalFilter evaluates a Filter against a BitmapIndex, returning matching point IDs.
func EvalFilter(f Filter, idx *BitmapIndex) *roaring.Bitmap {
	// AND
	if len(f.And) > 0 {
		result := EvalFilter(f.And[0], idx)
		for _, sub := range f.And[1:] {
			result.And(EvalFilter(sub, idx))
		}
		return result
	}

	// OR
	if len(f.Or) > 0 {
		result := roaring.New()
		for _, sub := range f.Or {
			result.Or(EvalFilter(sub, idx))
		}
		return result
	}

	// NOT
	if f.Not != nil {
		inner := EvalFilter(*f.Not, idx)
		all := idx.AllIDs()
		all.AndNot(inner)
		return all
	}

	// Leaf filter
	return evalLeaf(f, idx)
}

func evalLeaf(f Filter, idx *BitmapIndex) *roaring.Bitmap {
	switch f.Op {
	case OpEq:
		return idx.Get(f.Field, fmt.Sprint(f.Value))

	case OpNeq:
		eq := idx.Get(f.Field, fmt.Sprint(f.Value))
		all := idx.AllIDs()
		all.AndNot(eq)
		return all

	case OpIn:
		result := roaring.New()
		if vals, ok := f.Value.([]any); ok {
			for _, v := range vals {
				result.Or(idx.Get(f.Field, fmt.Sprint(v)))
			}
		}
		return result

	case OpContains:
		// For string[] fields: the value is stored as individual entries
		return idx.Get(f.Field, fmt.Sprint(f.Value))

	default:
		// Gt, Gte, Lt, Lte require range scans — not supported by bitmap index alone.
		// For Phase 1, return empty set. Range filters will be added with sorted indices.
		return roaring.New()
	}
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run "TestBitmap|TestEvalFilter" -v`
Expected: PASS (all 10 tests)

- [ ] **Step 7: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/bitmap.go pkg/vector/bitmap_test.go pkg/vector/filter.go pkg/vector/filter_test.go
git commit -m "feat(vector): add roaring bitmap payload index and filter evaluation"
```

---

### Task 4: Segment Storage (Growing + Sealed)

**Files:**
- Create: `pkg/vector/segment.go`
- Create: `pkg/vector/segment_test.go`

- [ ] **Step 1: Write segment tests**

```go
// pkg/vector/segment_test.go
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

	// Search the sealed segment
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

	// Load from backend
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

// --- test backend (reuse pattern from OCI tests) ---

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run "TestGrowing|TestSealed" -v`
Expected: FAIL — `NewGrowingSegment` undefined

- [ ] **Step 3: Implement segment storage**

```go
// pkg/vector/segment.go
//go:build vector

package vector

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

// segmentPath returns the storage key prefix for a segment.
func segmentPath(collection string, shard int, segID ulid.ULID) string {
	return fmt.Sprintf("%s/%d/segments/%s", collection, shard, segID.String())
}

// GrowingSegment is a mutable, in-memory segment accepting writes.
// Search is brute-force (no HNSW — segment is small and changing).
type GrowingSegment struct {
	mu       sync.RWMutex
	meta     SegmentMeta
	points   []Point
	pointMap map[PointID]*Point
	distFunc DistanceFunc
}

// NewGrowingSegment creates a new growing segment.
func NewGrowingSegment(id SegmentID, collection string, shard, dim int, metric DistanceMetric) *GrowingSegment {
	df, _ := DistanceFuncFor(metric)
	return &GrowingSegment{
		meta: SegmentMeta{
			ID:         id,
			Collection: collection,
			Shard:      shard,
			State:      SegmentGrowing,
			Dimensions: dim,
			Distance:   metric,
			CreatedAt:  time.Now(),
		},
		pointMap: make(map[PointID]*Point),
		distFunc: df,
	}
}

// Insert adds a point to the growing segment.
func (g *GrowingSegment) Insert(p Point) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cp := p
	g.points = append(g.points, cp)
	g.pointMap[p.ID] = &g.points[len(g.points)-1]
	g.meta.PointCount = len(g.points)
	g.meta.SizeBytes += int64(len(p.Vector)*4 + 8) // rough estimate
}

// Get retrieves a point by ID.
func (g *GrowingSegment) Get(id PointID) (Point, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	p, ok := g.pointMap[id]
	if !ok {
		return Point{}, false
	}
	return *p, true
}

// Count returns the number of points in the segment.
func (g *GrowingSegment) Count() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.points)
}

// Meta returns the segment metadata.
func (g *GrowingSegment) Meta() SegmentMeta {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.meta
}

// Search does brute-force search over all points in the growing segment.
// allow: if non-nil, only return IDs in this set.
func (g *GrowingSegment) Search(query []float32, topK int, allow map[uint64]struct{}) []ScoredPoint {
	g.mu.RLock()
	defer g.mu.RUnlock()

	type scored struct {
		point Point
		dist  float32
	}
	var results []scored
	for _, p := range g.points {
		if allow != nil {
			if _, ok := allow[p.ID]; !ok {
				continue
			}
		}
		d := g.distFunc(query, p.Vector)
		results = append(results, scored{p, d})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].dist < results[j].dist
	})
	if len(results) > topK {
		results = results[:topK]
	}
	out := make([]ScoredPoint, len(results))
	for i, r := range results {
		out[i] = ScoredPoint{Point: r.point, Score: r.dist}
	}
	return out
}

// Points returns a copy of all points (used during sealing).
func (g *GrowingSegment) Points() []Point {
	g.mu.RLock()
	defer g.mu.RUnlock()
	cp := make([]Point, len(g.points))
	copy(cp, g.points)
	return cp
}

// SealedSegment is an immutable, indexed segment with HNSW and bitmap indices.
type SealedSegment struct {
	Meta     SegmentMeta
	vectors  []Point // all points, ordered by insertion
	pointMap map[PointID]int // ID -> index in vectors slice
	hnsw     *HNSWIndex
	bitmaps  *BitmapIndex
	distFunc DistanceFunc
}

// SealSegment freezes a growing segment, builds HNSW + bitmap indices,
// and writes all data to the storage backend.
func SealSegment(ctx context.Context, gs *GrowingSegment, be storage.Backend, bucket string, hnswM, hnswEfConstruct int) (*SealedSegment, error) {
	points := gs.Points()
	gsMeta := gs.Meta()

	distFunc, err := DistanceFuncFor(gsMeta.Distance)
	if err != nil {
		return nil, err
	}

	// Build HNSW index
	hnsw := NewHNSWIndex(gsMeta.Dimensions, hnswM, hnswEfConstruct, distFunc)
	for _, p := range points {
		hnsw.Insert(p.ID, p.Vector)
	}

	// Build bitmap index from payloads
	bitmaps := NewBitmapIndex()
	for _, p := range points {
		for k, v := range p.Payload {
			bitmaps.Add(p.ID, k, fmt.Sprint(v))
		}
	}

	now := time.Now()
	meta := SegmentMeta{
		ID:         gsMeta.ID,
		Collection: gsMeta.Collection,
		Shard:      gsMeta.Shard,
		State:      SegmentIndexed,
		PointCount: len(points),
		Dimensions: gsMeta.Dimensions,
		Distance:   gsMeta.Distance,
		CreatedAt:  gsMeta.CreatedAt,
		SealedAt:   &now,
	}

	ss := &SealedSegment{
		Meta:     meta,
		vectors:  points,
		pointMap: make(map[PointID]int, len(points)),
		hnsw:     hnsw,
		bitmaps:  bitmaps,
		distFunc: distFunc,
	}
	for i, p := range points {
		ss.pointMap[p.ID] = i
	}

	// Write to backend
	prefix := segmentPath(meta.Collection, meta.Shard, meta.ID)

	// vectors.bin: [count:4][dim:4] then [id:8][vec floats...]... for each point
	var vecBuf bytes.Buffer
	binary.Write(&vecBuf, binary.LittleEndian, uint32(len(points)))
	binary.Write(&vecBuf, binary.LittleEndian, uint32(meta.Dimensions))
	for _, p := range points {
		binary.Write(&vecBuf, binary.LittleEndian, p.ID)
		for _, v := range p.Vector {
			binary.Write(&vecBuf, binary.LittleEndian, math.Float32bits(v))
		}
	}
	vecData := vecBuf.Bytes()
	meta.SizeBytes += int64(len(vecData))
	if err := be.PutObject(ctx, bucket, prefix+"/vectors.bin", bytes.NewReader(vecData), int64(len(vecData)), nil); err != nil {
		return nil, fmt.Errorf("write vectors.bin: %w", err)
	}

	// hnsw.bin
	hnswData, err := hnsw.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize hnsw: %w", err)
	}
	meta.SizeBytes += int64(len(hnswData))
	if err := be.PutObject(ctx, bucket, prefix+"/hnsw.bin", bytes.NewReader(hnswData), int64(len(hnswData)), nil); err != nil {
		return nil, fmt.Errorf("write hnsw.bin: %w", err)
	}

	// payload.bin (bitmap index + raw payloads as JSON)
	payloadJSON, err := json.Marshal(payloadsFromPoints(points))
	if err != nil {
		return nil, fmt.Errorf("marshal payloads: %w", err)
	}
	bitmapData, err := bitmaps.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize bitmaps: %w", err)
	}
	var payBuf bytes.Buffer
	binary.Write(&payBuf, binary.LittleEndian, uint32(len(bitmapData)))
	payBuf.Write(bitmapData)
	binary.Write(&payBuf, binary.LittleEndian, uint32(len(payloadJSON)))
	payBuf.Write(payloadJSON)
	payData := payBuf.Bytes()
	meta.SizeBytes += int64(len(payData))
	if err := be.PutObject(ctx, bucket, prefix+"/payload.bin", bytes.NewReader(payData), int64(len(payData)), nil); err != nil {
		return nil, fmt.Errorf("write payload.bin: %w", err)
	}

	// meta.json
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal meta: %w", err)
	}
	if err := be.PutObject(ctx, bucket, prefix+"/meta.json", bytes.NewReader(metaJSON), int64(len(metaJSON)), nil); err != nil {
		return nil, fmt.Errorf("write meta.json: %w", err)
	}

	ss.Meta = meta
	return ss, nil
}

// LoadSealedSegment reads a sealed segment from the storage backend.
func LoadSealedSegment(ctx context.Context, be storage.Backend, bucket, collection string, shard int, segID ulid.ULID, metric DistanceMetric) (*SealedSegment, error) {
	prefix := segmentPath(collection, shard, segID)
	distFunc, err := DistanceFuncFor(metric)
	if err != nil {
		return nil, err
	}

	// Read meta.json
	metaObj, err := be.GetObject(ctx, bucket, prefix+"/meta.json")
	if err != nil {
		return nil, fmt.Errorf("read meta.json: %w", err)
	}
	metaBytes, err := io.ReadAll(metaObj.Body)
	metaObj.Body.Close()
	if err != nil {
		return nil, err
	}
	var meta SegmentMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}

	// Read vectors.bin
	vecObj, err := be.GetObject(ctx, bucket, prefix+"/vectors.bin")
	if err != nil {
		return nil, fmt.Errorf("read vectors.bin: %w", err)
	}
	vecData, err := io.ReadAll(vecObj.Body)
	vecObj.Body.Close()
	if err != nil {
		return nil, err
	}
	points, err := decodeVectors(vecData, meta.Dimensions)
	if err != nil {
		return nil, fmt.Errorf("decode vectors: %w", err)
	}

	// Read hnsw.bin
	hnswObj, err := be.GetObject(ctx, bucket, prefix+"/hnsw.bin")
	if err != nil {
		return nil, fmt.Errorf("read hnsw.bin: %w", err)
	}
	hnswData, err := io.ReadAll(hnswObj.Body)
	hnswObj.Body.Close()
	if err != nil {
		return nil, err
	}
	hnsw, err := DeserializeHNSWIndex(hnswData, distFunc)
	if err != nil {
		return nil, fmt.Errorf("deserialize hnsw: %w", err)
	}

	// Read payload.bin
	payObj, err := be.GetObject(ctx, bucket, prefix+"/payload.bin")
	if err != nil {
		return nil, fmt.Errorf("read payload.bin: %w", err)
	}
	payData, err := io.ReadAll(payObj.Body)
	payObj.Body.Close()
	if err != nil {
		return nil, err
	}
	bitmaps, payloads, err := decodePayloads(payData)
	if err != nil {
		return nil, fmt.Errorf("decode payloads: %w", err)
	}

	// Attach payloads to points
	for i := range points {
		if pl, ok := payloads[points[i].ID]; ok {
			points[i].Payload = pl
		}
	}

	ss := &SealedSegment{
		Meta:     meta,
		vectors:  points,
		pointMap: make(map[PointID]int, len(points)),
		hnsw:     hnsw,
		bitmaps:  bitmaps,
		distFunc: distFunc,
	}
	for i, p := range points {
		ss.pointMap[p.ID] = i
	}
	return ss, nil
}

// Search finds nearest neighbors in the sealed segment using HNSW.
// allow: if non-nil, only return IDs in this set.
func (s *SealedSegment) Search(query []float32, topK, efSearch int, allow map[uint64]struct{}) []ScoredPoint {
	results := s.hnsw.Search(query, topK, efSearch, allow)
	out := make([]ScoredPoint, 0, len(results))
	for _, r := range results {
		if idx, ok := s.pointMap[r.ID]; ok {
			out = append(out, ScoredPoint{Point: s.vectors[idx], Score: r.Distance})
		}
	}
	return out
}

// Get retrieves a point by ID from the sealed segment.
func (s *SealedSegment) Get(id PointID) (Point, bool) {
	if idx, ok := s.pointMap[id]; ok {
		return s.vectors[idx], true
	}
	return Point{}, false
}

// BitmapIndex returns the segment's bitmap index for filter evaluation.
func (s *SealedSegment) BitmapIndex() *BitmapIndex {
	return s.bitmaps
}

// --- helpers ---

type payloadMap map[PointID]map[string]any

func payloadsFromPoints(points []Point) payloadMap {
	m := make(payloadMap, len(points))
	for _, p := range points {
		if len(p.Payload) > 0 {
			m[p.ID] = p.Payload
		}
	}
	return m
}

func decodeVectors(data []byte, dim int) ([]Point, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("vectors data too short")
	}
	count := int(binary.LittleEndian.Uint32(data[0:4]))
	dataDim := int(binary.LittleEndian.Uint32(data[4:8]))
	if dataDim != dim {
		return nil, fmt.Errorf("dimension mismatch: expected %d, got %d", dim, dataDim)
	}

	points := make([]Point, 0, count)
	off := 8
	for i := 0; i < count; i++ {
		if off+8 > len(data) {
			return nil, fmt.Errorf("truncated at point %d", i)
		}
		id := binary.LittleEndian.Uint64(data[off:])
		off += 8
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
			off += 4
		}
		points = append(points, Point{ID: id, Vector: vec})
	}
	return points, nil
}

func decodePayloads(data []byte) (*BitmapIndex, payloadMap, error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("payload data too short")
	}
	off := 0
	bmLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+bmLen > len(data) {
		return nil, nil, fmt.Errorf("bitmap data truncated")
	}
	bitmaps, err := DeserializeBitmapIndex(data[off : off+bmLen])
	if err != nil {
		return nil, nil, err
	}
	off += bmLen

	if off+4 > len(data) {
		return nil, nil, fmt.Errorf("payload JSON length truncated")
	}
	jsonLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	if off+jsonLen > len(data) {
		return nil, nil, fmt.Errorf("payload JSON truncated")
	}
	var payloads payloadMap
	if err := json.Unmarshal(data[off:off+jsonLen], &payloads); err != nil {
		return nil, nil, fmt.Errorf("unmarshal payloads: %w", err)
	}

	return bitmaps, payloads, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run "TestGrowing|TestSealed" -v`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/segment.go pkg/vector/segment_test.go
git commit -m "feat(vector): add growing and sealed segment storage"
```

---

### Task 5: Write-Ahead Log

**Files:**
- Create: `pkg/vector/wal.go`
- Create: `pkg/vector/wal_test.go`

- [ ] **Step 1: Write WAL tests**

```go
// pkg/vector/wal_test.go
//go:build vector

package vector

import (
	"context"
	"testing"
	"time"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	be := newSegTestBackend()
	w := NewWAL(be, "warp-vectors", "test-col", 0)

	entries := []WALEntry{
		{Type: WALInsert, Point: Point{ID: 1, Vector: []float32{1, 0, 0}}},
		{Type: WALInsert, Point: Point{ID: 2, Vector: []float32{0, 1, 0}}},
		{Type: WALDelete, PointID: 1},
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	ctx := context.Background()
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Replay
	replayed, err := ReplayWAL(ctx, be, "warp-vectors", "test-col", 0)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(replayed))
	}
	if replayed[0].Type != WALInsert || replayed[0].Point.ID != 1 {
		t.Fatalf("entry 0 mismatch: %+v", replayed[0])
	}
	if replayed[2].Type != WALDelete || replayed[2].PointID != 1 {
		t.Fatalf("entry 2 mismatch: %+v", replayed[2])
	}
}

func TestWAL_FlushOnCount(t *testing.T) {
	be := newSegTestBackend()
	w := NewWAL(be, "warp-vectors", "test-col", 0)
	w.SetFlushThresholds(3, 1*time.Hour) // flush every 3 entries

	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(WALEntry{Type: WALInsert, Point: Point{ID: i, Vector: []float32{1, 0, 0}}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Should have auto-flushed
	ctx := context.Background()
	replayed, err := ReplayWAL(ctx, be, "warp-vectors", "test-col", 0)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 3 {
		t.Fatalf("expected 3 entries after auto-flush, got %d", len(replayed))
	}
}

func TestWAL_Truncate(t *testing.T) {
	be := newSegTestBackend()
	w := NewWAL(be, "warp-vectors", "test-col", 0)

	for i := uint64(1); i <= 5; i++ {
		w.Append(WALEntry{Type: WALInsert, Point: Point{ID: i, Vector: []float32{1, 0, 0}}})
	}
	ctx := context.Background()
	w.Flush(ctx)

	if err := w.Truncate(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	replayed, err := ReplayWAL(ctx, be, "warp-vectors", "test-col", 0)
	if err != nil {
		t.Fatalf("replay after truncate: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("expected 0 entries after truncate, got %d", len(replayed))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestWAL -v`
Expected: FAIL — `NewWAL` undefined

- [ ] **Step 3: Implement WAL**

```go
// pkg/vector/wal.go
//go:build vector

package vector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

// WALEntryType identifies the mutation type.
type WALEntryType string

const (
	WALInsert WALEntryType = "insert"
	WALDelete WALEntryType = "delete"
	WALUpdate WALEntryType = "update"
)

// WALEntry is a single mutation record.
type WALEntry struct {
	Type    WALEntryType `json:"type"`
	Point   Point        `json:"point,omitempty"`
	PointID PointID      `json:"point_id,omitempty"`
}

// walBatch is a set of entries flushed together.
type walBatch struct {
	Sequence  uint64     `json:"sequence"`
	Entries   []WALEntry `json:"entries"`
	Timestamp time.Time  `json:"timestamp"`
}

// WAL is an append-only write-ahead log that flushes batches to storage.Backend.
type WAL struct {
	mu         sync.Mutex
	backend    storage.Backend
	bucket     string
	collection string
	shard      int
	buffer     []WALEntry
	sequence   uint64
	flushCount int
	flushInterval time.Duration
}

// NewWAL creates a new WAL for the given collection and shard.
func NewWAL(backend storage.Backend, bucket, collection string, shard int) *WAL {
	return &WAL{
		backend:       backend,
		bucket:        bucket,
		collection:    collection,
		shard:         shard,
		flushCount:    1000,
		flushInterval: 100 * time.Millisecond,
	}
}

// SetFlushThresholds configures when the WAL auto-flushes.
func (w *WAL) SetFlushThresholds(count int, interval time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushCount = count
	w.flushInterval = interval
}

// Append adds an entry to the WAL buffer. Auto-flushes when count threshold is reached.
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	w.buffer = append(w.buffer, entry)
	shouldFlush := len(w.buffer) >= w.flushCount
	w.mu.Unlock()

	if shouldFlush {
		return w.Flush(context.Background())
	}
	return nil
}

// Flush writes all buffered entries to the storage backend as a single batch.
func (w *WAL) Flush(ctx context.Context) error {
	w.mu.Lock()
	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return nil
	}
	entries := w.buffer
	w.buffer = nil
	w.sequence++
	seq := w.sequence
	w.mu.Unlock()

	batch := walBatch{
		Sequence:  seq,
		Entries:   entries,
		Timestamp: time.Now(),
	}
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal WAL batch: %w", err)
	}
	key := w.walKey(seq)
	return w.backend.PutObject(ctx, w.bucket, key, bytes.NewReader(data), int64(len(data)), nil)
}

// Truncate removes all WAL files for this collection/shard.
func (w *WAL) Truncate(ctx context.Context) error {
	prefix := w.walPrefix()
	result, err := w.backend.ListObjects(ctx, w.bucket, prefix, "", 10000)
	if err != nil {
		return fmt.Errorf("list WAL files: %w", err)
	}
	for _, obj := range result.Contents {
		if err := w.backend.DeleteObject(ctx, w.bucket, obj.Key); err != nil {
			return fmt.Errorf("delete WAL %s: %w", obj.Key, err)
		}
	}
	w.mu.Lock()
	w.sequence = 0
	w.mu.Unlock()
	return nil
}

// PendingCount returns the number of unflushed entries.
func (w *WAL) PendingCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.buffer)
}

func (w *WAL) walPrefix() string {
	return fmt.Sprintf("%s/%d/wal/", w.collection, w.shard)
}

func (w *WAL) walKey(seq uint64) string {
	return fmt.Sprintf("%s/%d/wal/%012d.wal", w.collection, w.shard, seq)
}

// ReplayWAL reads all WAL batches from the backend and returns entries in order.
func ReplayWAL(ctx context.Context, backend storage.Backend, bucket, collection string, shard int) ([]WALEntry, error) {
	prefix := fmt.Sprintf("%s/%d/wal/", collection, shard)
	result, err := backend.ListObjects(ctx, bucket, prefix, "", 10000)
	if err != nil {
		return nil, fmt.Errorf("list WAL: %w", err)
	}

	// Sort by key (lexicographic = sequence order)
	keys := make([]string, 0, len(result.Contents))
	for _, obj := range result.Contents {
		if strings.HasSuffix(obj.Key, ".wal") {
			keys = append(keys, obj.Key)
		}
	}
	sort.Strings(keys)

	var entries []WALEntry
	for _, key := range keys {
		obj, err := backend.GetObject(ctx, bucket, key)
		if err != nil {
			return nil, fmt.Errorf("read WAL %s: %w", key, err)
		}
		data, err := io.ReadAll(obj.Body)
		obj.Body.Close()
		if err != nil {
			return nil, err
		}
		var batch walBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			return nil, fmt.Errorf("unmarshal WAL %s: %w", key, err)
		}
		entries = append(entries, batch.Entries...)
	}
	return entries, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestWAL -v`
Expected: PASS (all 3 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/wal.go pkg/vector/wal_test.go
git commit -m "feat(vector): add write-ahead log with flush and replay"
```

---

### Task 6: Two-Tier Segment Cache

**Files:**
- Create: `pkg/vector/cache.go`
- Create: `pkg/vector/cache_test.go`

- [ ] **Step 1: Write cache tests**

```go
// pkg/vector/cache_test.go
//go:build vector

package vector

import (
	"testing"

	"github.com/oklog/ulid/v2"
)

func TestSegmentCache_PutAndGet(t *testing.T) {
	cache := NewSegmentCache(1024 * 1024) // 1MB budget

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
	cache := NewSegmentCache(2048) // tiny budget

	id1 := ulid.Make()
	id2 := ulid.Make()
	id3 := ulid.Make()

	cache.Put(id1, &SealedSegment{Meta: SegmentMeta{ID: id1, SizeBytes: 1000}})
	cache.Put(id2, &SealedSegment{Meta: SegmentMeta{ID: id2, SizeBytes: 1000}})

	// Both should be in cache
	if _, ok := cache.Get(id1); !ok {
		t.Fatal("id1 should be in cache")
	}
	if _, ok := cache.Get(id2); !ok {
		t.Fatal("id2 should be in cache")
	}

	// Adding id3 should evict id1 (LRU)
	cache.Put(id3, &SealedSegment{Meta: SegmentMeta{ID: id3, SizeBytes: 1000}})

	// id2 was accessed more recently (via Get), so id1 should be evicted
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestSegmentCache -v`
Expected: FAIL — `NewSegmentCache` undefined

- [ ] **Step 3: Implement cache**

```go
// pkg/vector/cache.go
//go:build vector

package vector

import (
	"container/list"
	"sync"

	"github.com/oklog/ulid/v2"
)

// cacheEntry wraps a sealed segment with LRU bookkeeping.
type cacheEntry struct {
	id      ulid.ULID
	segment *SealedSegment
	size    int64
	elem    *list.Element
}

// SegmentCache is a memory-budget-aware LRU cache for sealed segments.
type SegmentCache struct {
	mu       sync.Mutex
	entries  map[ulid.ULID]*cacheEntry
	lru      *list.List // front = most recently used
	budget   int64
	used     int64
}

// NewSegmentCache creates a segment cache with the given memory budget in bytes.
func NewSegmentCache(budgetBytes int64) *SegmentCache {
	return &SegmentCache{
		entries: make(map[ulid.ULID]*cacheEntry),
		lru:     list.New(),
		budget:  budgetBytes,
	}
}

// Get retrieves a segment from cache. Returns (nil, false) on miss.
func (c *SegmentCache) Get(id ulid.ULID) (*SealedSegment, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(e.elem)
	return e.segment, true
}

// Put adds a segment to cache, evicting LRU entries if needed.
func (c *SegmentCache) Put(id ulid.ULID, seg *SealedSegment) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already cached, update and promote
	if e, ok := c.entries[id]; ok {
		c.used -= e.size
		e.segment = seg
		e.size = seg.Meta.SizeBytes
		c.used += e.size
		c.lru.MoveToFront(e.elem)
		c.evictLocked()
		return
	}

	size := seg.Meta.SizeBytes
	if size == 0 {
		size = 1 // avoid zero-size entries never being evicted
	}

	e := &cacheEntry{id: id, segment: seg, size: size}
	e.elem = c.lru.PushFront(e)
	c.entries[id] = e
	c.used += size
	c.evictLocked()
}

// Remove explicitly removes a segment from cache.
func (c *SegmentCache) Remove(id ulid.ULID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[id]; ok {
		c.removeLocked(e)
	}
}

// Len returns the number of cached segments.
func (c *SegmentCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Used returns the current memory usage in bytes.
func (c *SegmentCache) Used() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func (c *SegmentCache) evictLocked() {
	for c.used > c.budget && c.lru.Len() > 1 {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.removeLocked(oldest.Value.(*cacheEntry))
	}
}

func (c *SegmentCache) removeLocked(e *cacheEntry) {
	c.lru.Remove(e.elem)
	delete(c.entries, e.id)
	c.used -= e.size
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestSegmentCache -v`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/cache.go pkg/vector/cache_test.go
git commit -m "feat(vector): add LRU segment cache with memory budget"
```

---

### Task 7: Single-Node Vector Engine (End-to-End)

**Files:**
- Create: `pkg/vector/engine.go`
- Create: `pkg/vector/engine_test.go`

- [ ] **Step 1: Write engine tests**

```go
// pkg/vector/engine_test.go
//go:build vector

package vector

import (
	"context"
	"fmt"
	mrand "math/rand"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	be := newSegTestBackend()
	cfg := VectorConfig{
		Bucket:           "warp-vectors",
		SegmentSizeBytes: 4096, // small for tests
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

	err := e.CreateCollection(ctx, Collection{
		Name:       "test",
		Dimensions: 3,
		Distance:   Cosine,
	})
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

	resp, err := e.Search(ctx, SearchRequest{
		Collection: "test",
		Vector:     []float32{1, 0, 0},
		TopK:       2,
	})
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
		Name:       "test",
		Dimensions: 3,
		Distance:   Cosine,
		Schema:     []FieldSchema{{Name: "color", Type: FieldString, Indexed: true}},
	})

	e.Insert(ctx, "test", []Point{
		{ID: 1, Vector: []float32{1, 0, 0}, Payload: map[string]any{"color": "red"}},
		{ID: 2, Vector: []float32{0.9, 0.1, 0}, Payload: map[string]any{"color": "blue"}},
		{ID: 3, Vector: []float32{0.8, 0.2, 0}, Payload: map[string]any{"color": "red"}},
	})

	resp, err := e.Search(ctx, SearchRequest{
		Collection: "test",
		Vector:     []float32{1, 0, 0},
		TopK:       10,
		Filter:     &Filter{Field: "color", Op: OpEq, Value: "red"},
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
	e.Insert(ctx, "test", []Point{
		{ID: 42, Vector: []float32{1, 0, 0}, Payload: map[string]any{"x": "y"}},
	})

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
		t.Fatalf("expected 2 collections, got %d", len(cols))
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

	resp, err := e.Search(ctx, SearchRequest{
		Collection: "big",
		Vector:     randomVector(dim, rng),
		TopK:       10,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(resp.Points) != 10 {
		t.Fatalf("expected 10 results, got %d", len(resp.Points))
	}
	// Verify sorted
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
	err := e.Insert(ctx, "test", []Point{
		{ID: 1, Vector: []float32{1, 0}}, // 2-dim, collection expects 3
	})
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func BenchmarkEngine_Search(b *testing.B) {
	be := newSegTestBackend()
	cfg := VectorConfig{
		Bucket:           "warp-vectors",
		SegmentSizeBytes: 1 << 30, // 1GB, won't seal during bench
		WALFlushCount:    100000,
		HNSWm:           16,
		HNSWefConstruct:  200,
		HNSWefSearch:     128,
		CacheMemoryBytes: 1 << 30,
	}
	e, _ := NewEngine(be, cfg)
	defer e.Close()

	ctx := context.Background()
	dim := 128
	e.CreateCollection(ctx, Collection{Name: "bench", Dimensions: dim, Distance: Cosine})

	rng := mrand.New(mrand.NewSource(42))
	batch := make([]Point, 10000)
	for i := range batch {
		batch[i] = Point{ID: uint64(i + 1), Vector: randomVector(dim, rng)}
	}
	e.Insert(ctx, "bench", batch)

	query := randomVector(dim, rng)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Search(ctx, SearchRequest{Collection: "bench", Vector: query, TopK: 10})
	}
}

func BenchmarkEngine_Insert(b *testing.B) {
	be := newSegTestBackend()
	cfg := VectorConfig{
		Bucket:           "warp-vectors",
		SegmentSizeBytes: 1 << 30,
		WALFlushCount:    100000,
		HNSWm:           16,
		HNSWefConstruct:  200,
		HNSWefSearch:     128,
		CacheMemoryBytes: 1 << 30,
	}
	e, _ := NewEngine(be, cfg)
	defer e.Close()

	ctx := context.Background()
	dim := 128
	e.CreateCollection(ctx, Collection{Name: "bench", Dimensions: dim, Distance: Cosine})

	rng := mrand.New(mrand.NewSource(42))
	points := make([]Point, b.N)
	for i := range points {
		points[i] = Point{ID: uint64(i + 1), Vector: randomVector(dim, rng)}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Insert(ctx, "bench", points[i:i+1])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestEngine -v`
Expected: FAIL — `NewEngine` undefined

- [ ] **Step 3: Implement the engine**

```go
// pkg/vector/engine.go
//go:build vector

package vector

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prysmsh/warp-storage-engine/internal/storage"
)

// collectionState holds runtime state for a single collection.
type collectionState struct {
	mu         sync.RWMutex
	collection Collection
	growing    *GrowingSegment
	sealed     []*SealedSegment
	wal        *WAL
	deleted    map[PointID]struct{} // tombstones
}

// Engine is a single-node vector database engine.
type Engine struct {
	mu          sync.RWMutex
	backend     storage.Backend
	cfg         VectorConfig
	collections map[string]*collectionState
	cache       *SegmentCache
}

// NewEngine creates a new vector engine.
func NewEngine(backend storage.Backend, cfg VectorConfig) (*Engine, error) {
	if cfg.Bucket == "" {
		cfg.Bucket = "warp-vectors"
	}
	if cfg.HNSWm == 0 {
		cfg.HNSWm = 16
	}
	if cfg.HNSWefConstruct == 0 {
		cfg.HNSWefConstruct = 200
	}
	if cfg.HNSWefSearch == 0 {
		cfg.HNSWefSearch = 128
	}
	if cfg.CacheMemoryBytes == 0 {
		cfg.CacheMemoryBytes = 1 << 30
	}
	return &Engine{
		backend:     backend,
		cfg:         cfg,
		collections: make(map[string]*collectionState),
		cache:       NewSegmentCache(cfg.CacheMemoryBytes),
	}, nil
}

// Close flushes all pending WAL entries.
func (e *Engine) Close() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ctx := context.Background()
	for _, cs := range e.collections {
		cs.wal.Flush(ctx)
	}
	return nil
}

// CreateCollection creates a new vector collection.
func (e *Engine) CreateCollection(ctx context.Context, col Collection) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.collections[col.Name]; ok {
		return fmt.Errorf("collection %q already exists", col.Name)
	}

	now := time.Now()
	col.CreatedAt = now
	col.UpdatedAt = now
	if col.ShardCount == 0 {
		col.ShardCount = 1
	}
	if col.Consistency == "" {
		col.Consistency = Eventual
	}
	if col.Replication == 0 {
		col.Replication = 1
	}

	// Persist collection metadata
	metaJSON, err := json.Marshal(col)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s/meta.json", col.Name)
	if err := e.backend.PutObject(ctx, e.cfg.Bucket, key, bytes.NewReader(metaJSON), int64(len(metaJSON)), nil); err != nil {
		return fmt.Errorf("persist collection meta: %w", err)
	}

	segID := ulid.MustNew(ulid.Timestamp(now), rand.Reader)
	wal := NewWAL(e.backend, e.cfg.Bucket, col.Name, 0)
	wal.SetFlushThresholds(e.cfg.WALFlushCount, e.cfg.WALFlushInterval)

	e.collections[col.Name] = &collectionState{
		collection: col,
		growing:    NewGrowingSegment(segID, col.Name, 0, col.Dimensions, col.Distance),
		wal:        wal,
		deleted:    make(map[PointID]struct{}),
	}
	return nil
}

// GetCollection returns collection metadata.
func (e *Engine) GetCollection(_ context.Context, name string) (*Collection, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cs, ok := e.collections[name]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", name)
	}
	col := cs.collection
	return &col, nil
}

// ListCollections returns all collections.
func (e *Engine) ListCollections(_ context.Context) ([]Collection, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cols := make([]Collection, 0, len(e.collections))
	for _, cs := range e.collections {
		cols = append(cols, cs.collection)
	}
	return cols, nil
}

// DeleteCollection removes a collection and all its data.
func (e *Engine) DeleteCollection(ctx context.Context, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	cs, ok := e.collections[name]
	if !ok {
		return fmt.Errorf("collection %q not found", name)
	}
	cs.wal.Flush(ctx)
	delete(e.collections, name)
	return nil
}

// Insert adds points to a collection.
func (e *Engine) Insert(ctx context.Context, collection string, points []Point) error {
	e.mu.RLock()
	cs, ok := e.collections[collection]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("collection %q not found", collection)
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, p := range points {
		if len(p.Vector) != cs.collection.Dimensions {
			return fmt.Errorf("point %d: expected %d dimensions, got %d", p.ID, cs.collection.Dimensions, len(p.Vector))
		}
		cs.growing.Insert(p)
		cs.wal.Append(WALEntry{Type: WALInsert, Point: p})
	}

	// Check if growing segment should be sealed
	if cs.growing.Meta().SizeBytes >= e.cfg.SegmentSizeBytes {
		if err := e.sealGrowingLocked(ctx, cs); err != nil {
			return fmt.Errorf("seal: %w", err)
		}
	}

	return nil
}

// sealGrowingLocked seals the current growing segment and starts a new one.
// Must be called with cs.mu held.
func (e *Engine) sealGrowingLocked(ctx context.Context, cs *collectionState) error {
	sealed, err := SealSegment(ctx, cs.growing, e.backend, e.cfg.Bucket, e.cfg.HNSWm, e.cfg.HNSWefConstruct)
	if err != nil {
		return err
	}
	cs.sealed = append(cs.sealed, sealed)
	e.cache.Put(sealed.Meta.ID, sealed)

	// Truncate WAL and start fresh
	cs.wal.Flush(ctx)
	cs.wal.Truncate(ctx)

	newID := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
	cs.growing = NewGrowingSegment(newID, cs.collection.Name, 0, cs.collection.Dimensions, cs.collection.Distance)
	return nil
}

// Search performs vector similarity search.
func (e *Engine) Search(_ context.Context, req SearchRequest) (*SearchResponse, error) {
	start := time.Now()

	e.mu.RLock()
	cs, ok := e.collections[req.Collection]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("collection %q not found", req.Collection)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	topK := req.TopK
	if topK == 0 {
		topK = 10
	}
	efSearch := req.EfSearch
	if efSearch == 0 {
		efSearch = e.cfg.HNSWefSearch
	}

	// Build allow set from filter if present
	var allow map[uint64]struct{}
	if req.Filter != nil {
		// Evaluate filter against all segments' bitmap indices
		combined := NewBitmapIndex()
		// Growing segment: build bitmap on the fly
		for _, p := range cs.growing.Points() {
			for k, v := range p.Payload {
				combined.Add(p.ID, k, fmt.Sprint(v))
			}
		}
		// Sealed segments
		for _, ss := range cs.sealed {
			bm := ss.BitmapIndex()
			// Merge: for each field/value in sealed bitmap, add to combined
			// This is a simplification — in production, evaluate filter per-segment
			for field, vals := range bm.bitmaps {
				for val, bits := range vals {
					iter := bits.Iterator()
					for iter.HasNext() {
						combined.Add(uint64(iter.Next()), field, val)
					}
				}
			}
		}
		filterResult := EvalFilter(*req.Filter, combined)
		allow = make(map[uint64]struct{})
		iter := filterResult.Iterator()
		for iter.HasNext() {
			allow[uint64(iter.Next())] = struct{}{}
		}
	}

	// Remove deleted IDs from allow set, or build exclusion set
	var exclude map[uint64]struct{}
	if len(cs.deleted) > 0 {
		if allow != nil {
			for id := range cs.deleted {
				delete(allow, id)
			}
		} else {
			exclude = cs.deleted
		}
	}

	// Search growing segment (brute force)
	growingResults := cs.growing.Search(req.Vector, topK, allow)

	// Filter out excluded from growing
	if exclude != nil {
		filtered := growingResults[:0]
		for _, r := range growingResults {
			if _, ok := exclude[r.ID]; !ok {
				filtered = append(filtered, r)
			}
		}
		growingResults = filtered
	}

	// Search sealed segments (HNSW)
	var sealedResults []ScoredPoint
	for _, ss := range cs.sealed {
		results := ss.Search(req.Vector, topK, efSearch, allow)
		for _, r := range results {
			if exclude != nil {
				if _, ok := exclude[r.ID]; ok {
					continue
				}
			}
			sealedResults = append(sealedResults, r)
		}
	}

	// Merge all results
	all := append(growingResults, sealedResults...)
	sort.Slice(all, func(i, j int) bool {
		return all[i].Score < all[j].Score
	})
	if len(all) > topK {
		all = all[:topK]
	}

	return &SearchResponse{
		Points:   all,
		SearchMs: time.Since(start).Milliseconds(),
	}, nil
}

// Get retrieves points by ID.
func (e *Engine) Get(_ context.Context, collection string, ids []PointID) ([]Point, error) {
	e.mu.RLock()
	cs, ok := e.collections[collection]
	e.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("collection %q not found", collection)
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	var results []Point
	for _, id := range ids {
		if _, deleted := cs.deleted[id]; deleted {
			continue
		}
		if p, ok := cs.growing.Get(id); ok {
			results = append(results, p)
			continue
		}
		for _, ss := range cs.sealed {
			if p, ok := ss.Get(id); ok {
				results = append(results, p)
				break
			}
		}
	}
	return results, nil
}

// Delete soft-deletes points by ID (tombstone).
func (e *Engine) Delete(ctx context.Context, collection string, ids []PointID) error {
	e.mu.RLock()
	cs, ok := e.collections[collection]
	e.mu.RUnlock()
	if !ok {
		return fmt.Errorf("collection %q not found", collection)
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, id := range ids {
		cs.deleted[id] = struct{}{}
		cs.wal.Append(WALEntry{Type: WALDelete, PointID: id})
	}
	return nil
}

// LoadCollectionsFromBackend discovers and loads all collections from the storage backend.
func (e *Engine) LoadCollectionsFromBackend(ctx context.Context) error {
	result, err := e.backend.ListObjects(ctx, e.cfg.Bucket, "", "", 10000)
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}

	seen := make(map[string]struct{})
	for _, obj := range result.Contents {
		// Look for <collection>/meta.json
		parts := splitKey(obj.Key)
		if len(parts) == 2 && parts[1] == "meta.json" {
			colName := parts[0]
			if _, ok := seen[colName]; ok {
				continue
			}
			seen[colName] = struct{}{}

			metaObj, err := e.backend.GetObject(ctx, e.cfg.Bucket, obj.Key)
			if err != nil {
				continue
			}
			data, _ := io.ReadAll(metaObj.Body)
			metaObj.Body.Close()

			var col Collection
			if json.Unmarshal(data, &col) != nil {
				continue
			}

			segID := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
			wal := NewWAL(e.backend, e.cfg.Bucket, col.Name, 0)
			wal.SetFlushThresholds(e.cfg.WALFlushCount, e.cfg.WALFlushInterval)

			cs := &collectionState{
				collection: col,
				growing:    NewGrowingSegment(segID, col.Name, 0, col.Dimensions, col.Distance),
				wal:        wal,
				deleted:    make(map[PointID]struct{}),
			}

			// Replay WAL
			entries, _ := ReplayWAL(ctx, e.backend, e.cfg.Bucket, col.Name, 0)
			for _, entry := range entries {
				switch entry.Type {
				case WALInsert:
					cs.growing.Insert(entry.Point)
				case WALDelete:
					cs.deleted[entry.PointID] = struct{}{}
				}
			}

			e.mu.Lock()
			e.collections[col.Name] = cs
			e.mu.Unlock()
		}
	}
	return nil
}

func splitKey(key string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			if i > start {
				parts = append(parts, key[start:i])
			}
			start = i + 1
		}
	}
	if start < len(key) {
		parts = append(parts, key[start:])
	}
	return parts
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run TestEngine -v`
Expected: PASS (all 10 tests)

- [ ] **Step 5: Run the full test suite**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -v`
Expected: PASS (all tests from Tasks 1-7)

- [ ] **Step 6: Run benchmarks**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -run='^$' -bench=BenchmarkEngine -benchmem`
Expected: Output showing ns/op for Insert and Search operations

- [ ] **Step 7: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add pkg/vector/engine.go pkg/vector/engine_test.go
git commit -m "feat(vector): add single-node vector engine with end-to-end search"
```

---

### Task 8: Configuration Integration

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Read the current config file**

Read: `internal/config/config.go` — already read above. The `Config` struct needs a `Vector` field.

- [ ] **Step 2: Add VectorConfig to the main Config struct**

Add the following to `internal/config/config.go`:

In the `Config` struct, add after the `OCI` field:

```go
Vector VectorConfig `mapstructure:"vector"`
```

Add the `VectorConfig` struct after the `OCIConfig`:

```go
// VectorConfig contains configuration for the distributed vector database frontend.
type VectorConfig struct {
	Enabled          bool          `mapstructure:"enabled" envconfig:"VECTOR_ENABLED" default:"false"`
	GRPCListen       string        `mapstructure:"grpc_listen" envconfig:"VECTOR_GRPC_LISTEN" default:":6900"`
	RESTListen       string        `mapstructure:"rest_listen" envconfig:"VECTOR_REST_LISTEN" default:":6901"`
	InternalListen   string        `mapstructure:"internal_listen" envconfig:"VECTOR_INTERNAL_LISTEN" default:":6902"`
	Bucket           string        `mapstructure:"bucket" envconfig:"VECTOR_BUCKET" default:"warp-vectors"`
	SegmentSize      int64         `mapstructure:"segment_size" envconfig:"VECTOR_SEGMENT_SIZE" default:"67108864"`
	WALFlushInterval time.Duration `mapstructure:"wal_flush_interval" envconfig:"VECTOR_WAL_FLUSH_INTERVAL" default:"100ms"`
	WALFlushCount    int           `mapstructure:"wal_flush_count" envconfig:"VECTOR_WAL_FLUSH_COUNT" default:"1000"`
	HNSWm            int           `mapstructure:"hnsw_m" envconfig:"VECTOR_HNSW_M" default:"16"`
	HNSWefConstruct  int           `mapstructure:"hnsw_ef_construction" envconfig:"VECTOR_HNSW_EF_CONSTRUCTION" default:"200"`
	HNSWefSearch     int           `mapstructure:"hnsw_ef_search" envconfig:"VECTOR_HNSW_EF_SEARCH" default:"128"`
	CacheMemoryBytes int64         `mapstructure:"cache_memory_budget" envconfig:"VECTOR_CACHE_MEMORY_BUDGET" default:"1073741824"`
	CacheSSDPath     string        `mapstructure:"cache_ssd_path" envconfig:"VECTOR_CACHE_SSD_PATH" default:""`
	CacheSSDBytes    int64         `mapstructure:"cache_ssd_budget" envconfig:"VECTOR_CACHE_SSD_BUDGET" default:"10737418240"`
}
```

- [ ] **Step 3: Remove duplicate VectorConfig from types.go**

The `VectorConfig` in `pkg/vector/types.go` should be replaced to import from `internal/config`:

Update `pkg/vector/types.go`: remove the `VectorConfig` struct and add a type alias:

```go
// VectorConfig is imported from internal/config.
type VectorConfig = config.VectorConfig
```

Add the import:
```go
"github.com/prysmsh/warp-storage-engine/internal/config"
```

- [ ] **Step 4: Verify existing tests still pass**

Run: `cd /Users/alessio/code/prysm/warp-storage-engine && go test -tags vector ./pkg/vector/ -v && go test ./internal/config/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /Users/alessio/code/prysm/warp-storage-engine
git add internal/config/config.go pkg/vector/types.go
git commit -m "feat(vector): add VectorConfig to main config"
```

---

## Self-Review

**Spec coverage:**
- Data model (Collection, Point, Segment, Filter) — Task 1 ✓
- Distance metrics (cosine, L2, IP) — Task 1 ✓
- HNSW vector index (pure Go, serialize/deserialize) — Task 2 ✓
- Bitmap payload index (roaring bitmaps) — Task 3 ✓
- Filter evaluation (And/Or/Not/Eq/Neq/In) — Task 3 ✓
- Segment storage (growing + sealed + backend persistence) — Task 4 ✓
- WAL (append, flush, replay, truncate) — Task 5 ✓
- Two-tier cache (memory LRU with budget) — Task 6 ✓
- Engine (create/delete collection, insert, search, get, delete, filtered search) — Task 7 ✓
- Configuration integration — Task 8 ✓
- Cluster (coordinator, worker, gateway, Raft, replication) — Plan 2 (separate)
- gRPC/REST API — Plan 3 (separate)
- Plugin system — Plan 3 (separate)
- Memory engine plugin — Plan 4 (separate)

**Placeholder scan:** No TBDs, TODOs, or vague instructions. All steps have code blocks.

**Type consistency check:**
- `DistanceFunc` signature matches across distance.go, hnsw.go, segment.go — ✓
- `Point`, `ScoredPoint`, `PointID` used consistently — ✓
- `SegmentMeta`, `SegmentID`, `SegmentState` used consistently — ✓
- `Filter`, `FilterOp` used consistently between types.go, filter.go, engine.go — ✓
- `SearchRequest`/`SearchResponse` used consistently — ✓
- `HNSWSearchResult` vs `ScoredPoint` — different types for different layers (HNSW returns HNSWSearchResult, engine converts to ScoredPoint) — ✓
- `BitmapIndex.bitmaps` field accessed from engine.go — it's unexported. Need to add a method. Fixed: engine.go uses `BitmapIndex()` which returns the index, then accesses `.bitmaps` — this field is unexported. Adding an `Entries()` method or making the field exported.

**Fix:** In `bitmap.go`, the `bitmaps` field is lowercase (unexported). In `engine.go` line where we iterate `bm.bitmaps`, this works because they're in the same package (`vector`). ✓ Same package, no issue.

- `VectorConfig` — defined in types.go, then moved to config.go in Task 8 with alias. The test files use the struct directly via `VectorConfig{...}`. After Task 8, they import from config. The engine_test.go creates `VectorConfig` inline — will still work with the type alias. ✓

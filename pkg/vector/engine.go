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

type collectionState struct {
	mu         sync.RWMutex
	collection Collection
	growing    *GrowingSegment
	sealed     []*SealedSegment
	wal        *WAL
	deleted    map[PointID]struct{}
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

func (e *Engine) Close() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ctx := context.Background()
	for _, cs := range e.collections {
		cs.wal.Flush(ctx)
	}
	return nil
}

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

func (e *Engine) ListCollections(_ context.Context) ([]Collection, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	cols := make([]Collection, 0, len(e.collections))
	for _, cs := range e.collections {
		cols = append(cols, cs.collection)
	}
	return cols, nil
}

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

	if cs.growing.Meta().SizeBytes >= e.cfg.SegmentSizeBytes {
		if err := e.sealGrowingLocked(ctx, cs); err != nil {
			return fmt.Errorf("seal: %w", err)
		}
	}

	return nil
}

func (e *Engine) sealGrowingLocked(ctx context.Context, cs *collectionState) error {
	sealed, err := SealSegment(ctx, cs.growing, e.backend, e.cfg.Bucket, e.cfg.HNSWm, e.cfg.HNSWefConstruct)
	if err != nil {
		return err
	}
	cs.sealed = append(cs.sealed, sealed)
	e.cache.Put(sealed.Meta.ID, sealed)

	cs.wal.Flush(ctx)
	cs.wal.Truncate(ctx)

	newID := ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)
	cs.growing = NewGrowingSegment(newID, cs.collection.Name, 0, cs.collection.Dimensions, cs.collection.Distance)
	return nil
}

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

	var allow map[uint64]struct{}
	if req.Filter != nil {
		combined := NewBitmapIndex()
		for _, p := range cs.growing.Points() {
			for k, v := range p.Payload {
				combined.Add(p.ID, k, fmt.Sprint(v))
			}
		}
		for _, ss := range cs.sealed {
			bm := ss.BitmapIndex()
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

	growingResults := cs.growing.Search(req.Vector, topK, allow)
	if exclude != nil {
		filtered := growingResults[:0]
		for _, r := range growingResults {
			if _, ok := exclude[r.ID]; !ok {
				filtered = append(filtered, r)
			}
		}
		growingResults = filtered
	}

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

func (e *Engine) Delete(_ context.Context, collection string, ids []PointID) error {
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

// LoadCollectionsFromBackend discovers and loads collections from storage.
func (e *Engine) LoadCollectionsFromBackend(ctx context.Context) error {
	result, err := e.backend.ListObjects(ctx, e.cfg.Bucket, "", "", 10000)
	if err != nil {
		return fmt.Errorf("list collections: %w", err)
	}

	seen := make(map[string]struct{})
	for _, obj := range result.Contents {
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

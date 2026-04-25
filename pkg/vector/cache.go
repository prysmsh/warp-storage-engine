//go:build vector

package vector

import (
	"container/list"
	"sync"

	"github.com/oklog/ulid/v2"
)

type cacheEntry struct {
	id      ulid.ULID
	segment *SealedSegment
	size    int64
	elem    *list.Element
}

// SegmentCache is a memory-budget-aware LRU cache for sealed segments.
type SegmentCache struct {
	mu      sync.Mutex
	entries map[ulid.ULID]*cacheEntry
	lru     *list.List
	budget  int64
	used    int64
}

// NewSegmentCache creates a segment cache with the given memory budget in bytes.
func NewSegmentCache(budgetBytes int64) *SegmentCache {
	return &SegmentCache{
		entries: make(map[ulid.ULID]*cacheEntry),
		lru:     list.New(),
		budget:  budgetBytes,
	}
}

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

func (c *SegmentCache) Put(id ulid.ULID, seg *SealedSegment) {
	c.mu.Lock()
	defer c.mu.Unlock()

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
		size = 1
	}

	e := &cacheEntry{id: id, segment: seg, size: size}
	e.elem = c.lru.PushFront(e)
	c.entries[id] = e
	c.used += size
	c.evictLocked()
}

func (c *SegmentCache) Remove(id ulid.ULID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[id]; ok {
		c.removeLocked(e)
	}
}

func (c *SegmentCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
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

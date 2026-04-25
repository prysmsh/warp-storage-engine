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

type walBatch struct {
	Sequence  uint64     `json:"sequence"`
	Entries   []WALEntry `json:"entries"`
	Timestamp time.Time  `json:"timestamp"`
}

// WAL is an append-only write-ahead log that flushes batches to storage.Backend.
type WAL struct {
	mu            sync.Mutex
	backend       storage.Backend
	bucket        string
	collection    string
	shard         int
	buffer        []WALEntry
	sequence      uint64
	flushCount    int
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

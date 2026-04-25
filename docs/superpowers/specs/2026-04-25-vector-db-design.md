# Warp Vector DB: Distributed Vector Database for Warp Storage Engine

**Date:** 2026-04-25
**Status:** Draft
**Depends on:** Warp Storage Engine (`storage.Backend` interface)
**Related:** [Memory Engine Design](2026-04-25-memory-engine-design.md) (first plugin)

## Overview

A distributed vector database built as a new frontend for Warp Storage Engine. Uses Warp's existing `storage.Backend` interface for segment persistence, enabling deployment on S3, Azure, or filesystem without local disk dependency. Designed for full scale (tens of millions+ vectors) with clustering, sharding, and replication.

The vector DB is the foundation layer. Higher-level features (cognition-aware memory, knowledge graphs) are delivered as plugins. The first plugin is the Memory Engine.

## Architecture

```
                    Client
                      |
              +-------+-------+
              |               |
          gRPC (:6900)   REST (:6901)
              |               |
              +-------+-------+
                      |
                   Gateway
                      |
              +-------+-------+
              |               |
         Coordinator      Workers (N)
         (Raft group)        |
              |          +---+---+
              |          |       |
              |       Shard 0  Shard N
              |          |       |
              +----------+------+
                         |
                   storage.Backend
                   (S3 / Azure / FS)
```

Three node roles run in a single binary by default. For scale, separate them:

- **Coordinator** — Raft-replicated metadata (collections, shard assignments, segment lifecycle). Handles query fan-out and top-k merge. Small group (3-5 nodes).
- **Worker** — Owns shards, serves reads from local segment cache, accepts writes to growing segments, builds HNSW indices on sealed segments. Stateless from cluster perspective (all state reconstructable from storage.Backend).
- **Gateway** — Terminates gRPC/REST connections, routes to coordinator for metadata ops, to workers for data ops. Stateless, horizontally scalable.

## Data Model

### Collection

A named group of vectors with a fixed schema. Analogous to a table.

```go
type Collection struct {
    Name          string            // Unique per tenant
    Dimensions    int               // Vector dimensionality (e.g., 384, 1536)
    Distance      DistanceMetric    // cosine, l2, ip (inner product)
    Schema        []FieldSchema     // Typed payload fields for filtering
    Replication   int               // Replica count (default 1)
    Consistency   ConsistencyLevel  // strong, eventual (default eventual)
    ShardCount    int               // Number of shards (default auto based on cluster size)
    CreatedAt     time.Time
    UpdatedAt     time.Time
}

type DistanceMetric string
const (
    Cosine      DistanceMetric = "cosine"
    L2          DistanceMetric = "l2"
    InnerProduct DistanceMetric = "ip"
)

type ConsistencyLevel string
const (
    Eventual ConsistencyLevel = "eventual"
    Strong   ConsistencyLevel = "strong"
)
```

### FieldSchema

Typed payload fields, indexed for filtering.

```go
type FieldSchema struct {
    Name    string
    Type    FieldType
    Indexed bool        // Whether to build a bitmap filter index
}

type FieldType string
const (
    FieldInt64    FieldType = "int64"
    FieldFloat64  FieldType = "float64"
    FieldString   FieldType = "string"
    FieldBool     FieldType = "bool"
    FieldStringArray FieldType = "string[]"
)
```

### Point

A single vector with its payload.

```go
type PointID = uint64

type Point struct {
    ID      PointID               // User-provided or auto-generated
    Vector  []float32             // Dense vector
    Payload map[string]any        // Typed fields matching collection schema
}
```

### Segment

The physical storage unit. Immutable once sealed.

```go
type SegmentID = ulid.ULID

type SegmentState string
const (
    SegmentGrowing   SegmentState = "growing"    // Accepting writes
    SegmentSealed    SegmentState = "sealed"      // Full, no more writes
    SegmentIndexed   SegmentState = "indexed"     // HNSW built
    SegmentOffloaded SegmentState = "offloaded"   // Pushed to storage.Backend
)

type Segment struct {
    ID          SegmentID
    Collection  string
    Shard       int
    State       SegmentState
    PointCount  int
    SizeBytes   int64
    HNSWBuilt   bool
    CreatedAt   time.Time
    SealedAt    *time.Time
}
```

### Filter

Query-time payload filtering.

```go
type Filter struct {
    And    []Filter          // All must match
    Or     []Filter          // Any must match
    Not    *Filter           // Negate
    Field  string            // Field name
    Op     FilterOp          // eq, neq, gt, gte, lt, lte, in, contains
    Value  any               // Comparison value
}

type FilterOp string
const (
    OpEq       FilterOp = "eq"
    OpNeq      FilterOp = "neq"
    OpGt       FilterOp = "gt"
    OpGte      FilterOp = "gte"
    OpLt       FilterOp = "lt"
    OpLte      FilterOp = "lte"
    OpIn       FilterOp = "in"
    OpContains FilterOp = "contains"   // For string[] fields
)
```

## Sharding

Hybrid sharding: consistent hashing for data distribution, local HNSW per shard.

- Point ID is hashed to determine shard assignment (`shard = hash(point_id) % shard_count`)
- Each shard maintains its own segments and HNSW indices
- Search queries fan out to all shards in parallel; each shard prunes locally
- Coordinator merges top-k results across shards

Shard-to-worker assignment is managed by the coordinator. When workers join or leave, the coordinator rebalances shard assignments and triggers segment transfers.

## Write Path

```
Client → Gateway → Coordinator (route by shard) → Worker (owning shard)
```

1. **Gateway** validates request, authenticates, resolves collection
2. **Coordinator** hashes point ID to determine shard, routes to owning worker
3. **Worker** appends to WAL, inserts into growing segment's in-memory buffer
4. **WAL flush** — batched writes to `storage.Backend` (default: 100ms or 1000 entries, whichever first)
5. **Segment seal** — when growing segment hits size threshold (default 64MB / 50k vectors), it's sealed. Background goroutine builds HNSW index, then uploads indexed segment to `storage.Backend`
6. **Replication** — for `replication > 1`, coordinator assigns replica shards to other workers. Growing segments replicate via WAL forwarding. Sealed segments are copied to replica nodes.

### Consistency Modes

- **Eventual (default):** Write returns after WAL flush on primary. Replicas catch up async.
- **Strong:** Write returns after WAL flush on primary + quorum of replicas. Coordinator tracks acknowledgments.

### Batch Inserts

Dedicated `BatchInsert` RPC with streaming. Worker buffers and distributes to shards. Target: 50k+ vectors/sec per worker on commodity hardware.

## Read Path (Search)

```
Client → Gateway → Coordinator (fan-out) → Workers (parallel) → Coordinator (merge) → Client
```

1. **Gateway** receives search request (query vector, filters, top-k, collection)
2. **Coordinator** fans out to all workers owning shards for the collection, in parallel
3. **Workers** search locally across all segments in their assigned shards:
   - **Growing segments:** brute-force scan (small, in memory)
   - **Indexed segments:** HNSW search with pre-filtering
   - Merge results across local segments, return top-k to coordinator
4. **Coordinator** merges top-k across all shard results, returns to client

### Pre-filtered Search

Filters evaluated first using payload bitmap indices (roaring bitmaps per indexed field). Filtered ID set passed to HNSW, which only visits matching nodes. For highly selective filters (<1% of data), falls back to brute-force on filtered set.

### Segment Cache (Two-Tier)

```
Hot:   Growing segments + recently queried indexed segments (in memory)
Warm:  Indexed segments on local SSD (mmap'd)
Cold:  Segments in storage.Backend (pulled on demand)
```

Workers maintain an LRU cache. Cache budget is configurable per tier. Cold segment pulls are cached locally for subsequent queries.

## Index Architecture

### HNSW Vector Index

- Pure Go implementation (no CGO)
- Built per-segment after sealing (not global — each segment has its own graph)
- Configurable M (default 16), efConstruction (default 200), efSearch (default 128, overridable per query)
- Supports all three distance metrics: cosine, L2, inner product
- Serialized as `hnsw.bin` in segment directory
- Concurrent reads during search (no locking needed — segments are immutable)

### Bitmap Payload Index

- Roaring bitmap sets per indexed field value
- Built per-segment alongside HNSW
- Fast intersection/union for compound boolean filters
- Serialized as part of `payload.bin` in segment directory

## Storage Layout

All data stored as objects via `storage.Backend`:

```
warp-vectors/
  {collection}/
    meta.json                                    # Collection schema + config
    {shard}/
      wal/
        {sequence}.wal                           # Write-ahead log entries
      segments/
        {segment_id}/
          vectors.bin                            # Raw float32 vectors, contiguous
          hnsw.bin                               # HNSW graph structure
          payload.bin                            # Payload data + bitmap indices
          meta.json                              # Segment metadata (count, state, etc.)
  _cluster/
    raft/                                        # Raft log + snapshots (if using storage.Backend for Raft)
    membership.json                              # Current cluster topology
```

## Cluster Coordination

### Raft Consensus

Coordinators form a Raft group for replicated metadata:
- Collection definitions and schemas
- Shard-to-worker assignment table
- Segment lifecycle state (growing → sealed → indexed → offloaded)
- Node membership

Uses `github.com/hashicorp/raft` with `storage.Backend` as the log/snapshot store (or local filesystem for lower latency).

### Node Membership

- Workers heartbeat to coordinator every 1s with load metrics (CPU, memory, segment count, cache usage)
- Coordinator detects worker failure after 3 missed heartbeats (configurable)
- On worker failure: coordinator reassigns affected shards to healthy workers
- On worker join: coordinator assigns shards to balance load
- Graceful drain: worker notifies coordinator, which migrates shards before shutdown

### Shard Rebalancing

When workers join or leave:
1. Coordinator computes new shard assignments to minimize data movement
2. New assignments published via Raft
3. Workers receiving new shards pull segments from `storage.Backend`
4. Workers losing shards stop serving them after transfer completes
5. Segment data doesn't move — only cache state changes (segments always exist in storage.Backend)

## Plugin System

### Plugin Interface

```go
type Plugin interface {
    Name() string
    Version() string

    // Lifecycle
    Init(ctx context.Context, vectorDB VectorDB) error
    Close() error

    // Optional: extend the API surface
    RegisterRoutes(router *mux.Router)
    RegisterGRPC(server *grpc.Server)
    RegisterMCP(registry *MCPRegistry)
}
```

### VectorDB Interface (What Plugins Get)

```go
type VectorDB interface {
    CreateCollection(ctx context.Context, cfg CollectionConfig) error
    DeleteCollection(ctx context.Context, name string) error
    GetCollection(ctx context.Context, name string) (*Collection, error)
    ListCollections(ctx context.Context) ([]Collection, error)

    Insert(ctx context.Context, collection string, points []Point) error
    Search(ctx context.Context, req SearchRequest) (*SearchResponse, error)
    Get(ctx context.Context, collection string, ids []PointID) ([]Point, error)
    Delete(ctx context.Context, collection string, ids []PointID) error
    Update(ctx context.Context, collection string, points []Point) error

    SearchWithFilter(ctx context.Context, req FilteredSearchRequest) (*SearchResponse, error)
    DeleteByFilter(ctx context.Context, collection string, filter Filter) (int64, error)

    BatchInsert(ctx context.Context, collection string) (BatchWriter, error)
}

type BatchWriter interface {
    Write(points []Point) error
    Close() error   // Flushes remaining buffer
}
```

### Plugin Loading

Compiled-in via build tags. No dynamic loading.

```go
//go:build memory

func init() {
    vector.RegisterPlugin(&memory.Plugin{})
}
```

## gRPC API

### Client-Facing Service

```protobuf
service WarpVectorDB {
    // Collections
    rpc CreateCollection(CreateCollectionRequest) returns (CreateCollectionResponse);
    rpc DeleteCollection(DeleteCollectionRequest) returns (DeleteCollectionResponse);
    rpc GetCollection(GetCollectionRequest) returns (CollectionResponse);
    rpc ListCollections(ListCollectionsRequest) returns (ListCollectionsResponse);

    // Points
    rpc Insert(InsertRequest) returns (InsertResponse);
    rpc Search(SearchRequest) returns (SearchResponse);
    rpc Get(GetRequest) returns (GetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Update(UpdateRequest) returns (UpdateResponse);

    // Streaming batch
    rpc BatchInsert(stream BatchInsertRequest) returns (BatchInsertResponse);
    rpc BatchSearch(BatchSearchRequest) returns (BatchSearchResponse);

    // Filtered
    rpc SearchWithFilter(FilteredSearchRequest) returns (SearchResponse);
    rpc DeleteByFilter(DeleteByFilterRequest) returns (DeleteByFilterResponse);
}
```

### Internal Cluster Service

```protobuf
service WarpInternal {
    rpc ForwardInsert(ForwardInsertRequest) returns (ForwardInsertResponse);
    rpc ShardSearch(ShardSearchRequest) returns (ShardSearchResponse);
    rpc ReplicateWAL(stream WALEntry) returns (ReplicateResponse);
    rpc PullSegment(PullSegmentRequest) returns (stream SegmentChunk);
    rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

## REST API

Maps to gRPC services for convenience and management:

```
POST   /v1/collections                          CreateCollection
GET    /v1/collections                          ListCollections
GET    /v1/collections/{name}                   GetCollection
DELETE /v1/collections/{name}                   DeleteCollection

POST   /v1/collections/{name}/points            Insert
POST   /v1/collections/{name}/search            Search
POST   /v1/collections/{name}/search/filter     SearchWithFilter
GET    /v1/collections/{name}/points/{id}       Get
PUT    /v1/collections/{name}/points/{id}       Update
DELETE /v1/collections/{name}/points/{id}       Delete
POST   /v1/collections/{name}/points/batch      BatchInsert
POST   /v1/collections/{name}/points/delete     DeleteByFilter

GET    /v1/cluster/status                       Cluster health + topology
GET    /v1/cluster/nodes                        Node list with roles/load
POST   /v1/cluster/nodes/{id}/drain             Drain a worker node
```

Plugin endpoints are registered dynamically under their own prefixes (e.g., memory plugin adds `/v1/spaces/...`).

## Authentication

Pluggable, same pattern as existing Warp auth middleware:

- **Bearer** — JWT with HMAC signing, per-collection ACLs
- **Basic** — username/password
- **None** — for development

Tenant context flows through the existing middleware stack. Collections are tenant-scoped when multi-tenancy is enabled.

## Configuration

```yaml
vector:
  enabled: true
  grpc_listen: ":6900"
  rest_listen: ":6901"
  internal_listen: ":6902"

  cluster:
    roles: [coordinator, worker, gateway]   # all-in-one default
    node_id: ""                             # auto-generated if empty
    peers: []                               # seed nodes for joining
    raft_dir: "/var/lib/warp/raft"
    heartbeat_interval: 1s
    election_timeout: 5s

  storage:
    bucket: "warp-vectors"
    segment_size: 67108864                  # 64MB seal threshold
    wal_flush_interval: 100ms
    wal_flush_count: 1000

  cache:
    memory_budget: 1073741824               # 1GB in-memory segment cache
    ssd_path: "/var/lib/warp/cache"
    ssd_budget: 10737418240                 # 10GB SSD cache
    eviction: lru

  index:
    hnsw_m: 16
    hnsw_ef_construction: 200
    hnsw_ef_search: 128

  plugins:
    memory:
      enabled: false
      listen: ":6903"
      # ... memory engine config from existing spec
```

Environment variable overrides: `VECTOR_ENABLED`, `VECTOR_GRPC_LISTEN`, `VECTOR_CLUSTER_ROLES`, etc.

## main.go Integration

Follows the existing OCI frontend pattern:

```go
if appConfig.Vector.Enabled {
    vectorDB, err := vector.NewNode(proxyServer.Storage(), appConfig.Vector)

    // gRPC server (client-facing)
    vectorGRPC := vector.NewGRPCServer(vectorDB, appConfig.Vector)
    go vectorGRPC.Serve(grpcListener)

    // REST gateway
    vectorREST := vector.NewRESTHandler(vectorDB, appConfig.Vector)
    vectorRESTSrv = &http.Server{Addr: appConfig.Vector.RESTListen, Handler: vectorREST}
    go vectorRESTSrv.ListenAndServe()

    // Internal cluster RPC
    go vectorDB.ServeInternal(internalListener)

    // Initialize plugins
    for _, p := range vectorDB.Plugins() {
        p.Init(ctx, vectorDB)
    }
}
```

Shutdown adds to Warp's existing 3-phase pattern:
1. Stop accepting new vector requests (drain gateway)
2. Flush growing segments, write final WAL entries
3. Leave cluster gracefully (coordinator transfers shard assignments)
4. Close gRPC servers, plugin cleanup, release resources

## Port Layout

| Port | Service |
|------|---------|
| `:8080` | S3 API (existing) |
| `:5000` | OCI registry (existing) |
| `:6900` | Vector DB gRPC |
| `:6901` | Vector DB REST |
| `:6902` | Internal cluster gRPC |
| `:6903` | Memory plugin (MCP + REST, if enabled) |

## Package Structure

```
pkg/vector/
  node.go              # Node lifecycle, role management, plugin loading
  config.go            # VectorConfig, validation
  types.go             # Collection, Point, Segment, Filter types

  segment.go           # Segment read/write, seal, format
  wal.go               # Write-ahead log, flush, replay
  cache.go             # Two-tier cache: memory LRU + SSD LRU

  hnsw.go              # Pure Go HNSW implementation
  bitmap.go            # Roaring bitmap payload index
  filter.go            # Filter evaluation, bitmap intersection

  coordinator.go       # Raft state machine, shard assignment, query fan-out
  worker.go            # Shard ownership, segment management, local search
  gateway.go           # Client-facing request routing
  raft.go              # Raft consensus wrapper
  replication.go       # WAL forwarding, segment replication
  membership.go        # Node join/leave, heartbeat, failure detection

  grpc.go              # gRPC server, service implementation
  rest.go              # REST handler, gorilla/mux router
  proto/
    vector.proto       # Client-facing protobuf definitions
    internal.proto     # Cluster-internal protobuf definitions

  plugin.go            # Plugin interface, VectorDB interface, registry

pkg/memory/            # Memory engine plugin (from existing spec)
  plugin.go            # Implements vector.Plugin interface
  engine.go            # MemoryEngine, uses VectorDB for indexing
  graph.go             # Knowledge graph (CSR/CSC)
  extractor.go         # Claude/Noop extraction
  embedder.go          # ONNX MiniLM
  retrieval.go         # Three-pass retrieval
  context.go           # Context assembly, U-curve ordering
  belief.go            # Contradiction detection
  mcp.go               # MCP tool definitions
  handler.go           # REST endpoints for memory API
  types.go             # Memory, Entity, Edge types
  space.go             # Space management
  wal.go               # Memory-level WAL (graph mutations)
  buffer_pool.go       # CLOCK buffer pool for memory content
```

### Build Tags

- `//go:build vector` — vector DB core (gRPC deps, Raft)
- `//go:build memory` — memory plugin (ONNX, Anthropic SDK)
- Default build: neither (lean S3+OCI binary)

## Performance Targets

| Operation | Target |
|-----------|--------|
| Point lookup by ID (hot) | < 100us |
| k-NN search, 1M vectors (hot segments) | < 10ms |
| k-NN search, 10M vectors (warm/SSD segments) | < 50ms |
| k-NN search, cold segment pull | < 500ms |
| Insert single point | < 1ms |
| Batch insert throughput per worker | > 50k vectors/sec |
| Segment seal + HNSW build (50k vectors, 384-dim) | < 5s |
| Fan-out merge overhead per shard | < 2ms |
| Cold start (worker pulling segments) | < 10s per GB |

## Dependencies

New Go dependencies required:

| Dependency | Purpose |
|------------|---------|
| `google.golang.org/grpc` | gRPC server and client |
| `google.golang.org/protobuf` | Protobuf code generation |
| `github.com/hashicorp/raft` | Raft consensus for coordinators |
| `github.com/hashicorp/raft-boltdb/v2` | Raft log storage |
| `github.com/RoaringBitmap/roaring` | Roaring bitmap for payload indices |
| `github.com/oklog/ulid/v2` | Time-sortable segment/point IDs |

Memory plugin adds:
| `github.com/yalue/onnxruntime_go` | ONNX Runtime for MiniLM |
| `github.com/anthropics/anthropic-sdk-go` | Claude API for extraction |
| `github.com/golang-jwt/jwt/v5` | JWT auth for memory API |

## Phase 2 Roadmap

Features intentionally deferred from Phase 1, to be built as plugins or core extensions:

- **Sparse vectors** — BM25-style sparse representations for hybrid search (dense + sparse fusion)
- **Multi-vector support** — multiple named vectors per point (e.g., title embedding + body embedding)
- **Full-text search plugin** — inverted index with BM25 scoring, integrated with vector search for hybrid retrieval
- **Geospatial indexing plugin** — R-tree or S2-based spatial index for location-aware vector search (nearest vectors within a geographic bounding box, distance-boosted scoring, geo-filtered k-NN). High priority — enables location-aware RAG, map-based search, and spatial recommendation use cases
- **GPU acceleration** — CUDA/Metal kernels for brute-force search and HNSW construction on large segments
- **Cross-collection joins** — search across multiple collections with result fusion
- **Quantization** — product quantization (PQ) and scalar quantization (SQ) for memory-efficient storage of billions of vectors
- **Disk-based HNSW** — HNSW graphs that page from SSD rather than requiring full in-memory residence

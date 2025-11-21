# Distributed Services Architecture Design

## Executive Summary

This document outlines the design for distributed, Spark-aware implementations of ADK's service interfaces (SessionService, ArtifactService, MemoryService) while preserving the simple ADK-compatible API surface.

**Key Principle**: ADK-compatible interfaces with distributed implementations underneath.

---

## Design Goals

### 1. ADK Compatibility ✅
- Implement exact same interfaces as ADK
- Drop-in replacement for ADK services
- Agents don't need to know about distribution

### 2. Spark Distribution ✅
- Partition-aware storage and retrieval
- Data locality optimization
- Horizontal scalability

### 3. Performance ✅
- Partition-local caching
- Minimal cross-partition communication
- Efficient state aggregation

### 4. Fault Tolerance ✅
- Persist to durable storage
- Support Spark checkpoint/recovery
- Handle partition failures gracefully

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│ ADK-Compatible Interface Layer                          │
│ (SessionService, ArtifactService, MemoryService)       │
└────────────────────────┬────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ Distributed Service Coordinator                         │
│ - Partition routing                                     │
│ - Request aggregation                                   │
│ - Cache management                                      │
└────────────────────────┬────────────────────────────────┘
                         ↓
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Partition 0  │ │ Partition 1  │ │ Partition 2  │
│ Local Cache  │ │ Local Cache  │ │ Local Cache  │
│ + Storage    │ │ + Storage    │ │ + Storage    │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       └────────────────┴────────────────┘
                        ↓
              ┌──────────────────┐
              │ Backend Storage  │
              │ (Pluggable)      │
              │ - In-Memory      │
              │ - Redis          │
              │ - PostgreSQL     │
              │ - S3/Object      │
              └──────────────────┘
```

---

## Service Designs

### 1. SessionService

**Purpose**: Persist conversation history and state across invocations

**ADK Interface**:
```go
type SessionService interface {
    GetSession(ctx context.Context, sessionID string) (*Session, error)
    UpdateSession(ctx context.Context, session *Session) error
    ListSessions(ctx context.Context, userID string) ([]*Session, error)
}

type Session struct {
    ID           string
    UserID       string
    State        map[string]interface{}
    EventHistory []Event
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

**Distributed Implementation**:
```go
type DistributedSessionService struct {
    // Partition-local caches (one per partition)
    localCaches map[string]*SessionCache

    // Backend storage (shared across cluster)
    backend SessionBackend

    // Partitioning strategy
    partitioner PartitionStrategy

    // Coordination
    coordinator *ServiceCoordinator
}

// Partition strategy
type PartitionStrategy interface {
    // Determine which partition owns this session
    GetPartition(sessionID string, numPartitions int) int

    // Route request to correct partition
    Route(sessionID string) string
}
```

**Key Features**:
- **Partition Affinity**: Sessions stick to partitions by hash(sessionID)
- **Local Cache**: Each partition caches its sessions
- **Lazy Loading**: Load from backend on first access
- **Write-Through**: Write to backend immediately (configurable)
- **Cross-Partition Queries**: ListSessions aggregates across partitions

**Partitioning Strategy**:
```go
// Hash-based partitioning (default)
func HashPartitioner(sessionID string, numPartitions int) int {
    hash := fnv.New32a()
    hash.Write([]byte(sessionID))
    return int(hash.Sum32() % uint32(numPartitions))
}
```

**Performance**:
- ✅ O(1) local access for partition-local sessions
- ✅ Single backend call for cross-partition access
- ✅ Batch updates to minimize network calls
- ⚠️ ListSessions requires cross-partition communication

---

### 2. ArtifactService

**Purpose**: Store and retrieve files/artifacts (images, documents, code)

**ADK Interface**:
```go
type ArtifactService interface {
    SaveArtifact(ctx context.Context, sessionID, name string, data []byte) error
    GetArtifact(ctx context.Context, sessionID, name string) ([]byte, error)
    ListArtifacts(ctx context.Context, sessionID string) ([]string, error)
    DeleteArtifact(ctx context.Context, sessionID, name string) error
}
```

**Distributed Implementation**:
```go
type DistributedArtifactService struct {
    // Partition-local caches
    localCaches map[string]*ArtifactCache

    // Backend storage (S3, object storage, or filesystem)
    backend ArtifactBackend

    // Partitioning (same as sessions)
    partitioner PartitionStrategy

    // Configuration
    maxCacheSize  int64  // Per-partition cache size
    enableCache   bool
    compressionLevel int
}

// Backend abstraction
type ArtifactBackend interface {
    Put(key string, data []byte) error
    Get(key string) ([]byte, error)
    Delete(key string) error
    List(prefix string) ([]string, error)
}

// Implementations
type S3Backend struct { /* ... */ }
type FileSystemBackend struct { /* ... */ }
type InMemoryBackend struct { /* ... */ }
```

**Key Features**:
- **Partition Affinity**: Artifacts partition by sessionID (same as sessions)
- **LRU Cache**: Each partition has LRU cache for hot artifacts
- **Compression**: Optional compression for large artifacts
- **Streaming**: Support streaming large artifacts (avoid loading into memory)
- **Backend Pluggable**: S3, filesystem, in-memory, etc.

**Optimization for Large Files**:
```go
// Stream large artifacts
type StreamingArtifact interface {
    Reader() io.ReadCloser
    Writer() io.WriteCloser
    Size() int64
}

func (s *DistributedArtifactService) StreamArtifact(
    ctx context.Context,
    sessionID, name string,
) (StreamingArtifact, error)
```

**Performance**:
- ✅ Local cache for frequently accessed artifacts
- ✅ Direct S3/storage access (no intermediate hops)
- ✅ Compression reduces network transfer
- ✅ Streaming prevents memory exhaustion

---

### 3. MemoryService

**Purpose**: Long-term memory with semantic search (RAG pattern)

**ADK Interface**:
```go
type MemoryService interface {
    Store(ctx context.Context, userID string, memory *Memory) error
    Query(ctx context.Context, userID string, query string, limit int) ([]*Memory, error)
    Delete(ctx context.Context, userID string, memoryID string) error
}

type Memory struct {
    ID        string
    UserID    string
    Content   string
    Embedding []float64  // Vector embedding
    Metadata  map[string]interface{}
    CreatedAt time.Time
}
```

**Distributed Implementation**:
```go
type DistributedMemoryService struct {
    // Partition-local vector indices
    localIndices map[string]*VectorIndex

    // Backend storage for memories
    backend MemoryBackend

    // Embedding generator
    embedder EmbeddingGenerator

    // Partitioning
    partitioner PartitionStrategy

    // Vector search config
    indexType    VectorIndexType  // HNSW, IVF, etc.
    distanceFunc DistanceFunction // Cosine, Euclidean, Dot
}

// Vector index for semantic search
type VectorIndex interface {
    Add(id string, embedding []float64, metadata map[string]interface{}) error
    Search(query []float64, k int) ([]SearchResult, error)
    Delete(id string) error
    Size() int
}

type SearchResult struct {
    ID       string
    Score    float64  // Similarity score
    Metadata map[string]interface{}
}

// Embedding generator
type EmbeddingGenerator interface {
    Embed(text string) ([]float64, error)
    EmbedBatch(texts []string) ([][]float64, error)
}
```

**Key Features**:
- **Partition-Local Indices**: Each partition has its own vector index
- **Cross-Partition Search**: Query aggregates top-k from each partition
- **Approximate Search**: HNSW or IVF for fast retrieval
- **Embedding Cache**: Cache embeddings to avoid recomputation
- **Metadata Filtering**: Pre-filter by metadata before vector search

**Vector Index Implementation**:
```go
// HNSW (Hierarchical Navigable Small World) index
type HNSWIndex struct {
    vectors    map[string][]float64
    graph      map[string][]string
    entryPoint string
    M          int     // Max connections per node
    efConstruct int    // Construction time
    efSearch   int     // Search time
}

// Efficient approximate nearest neighbor search
func (h *HNSWIndex) Search(query []float64, k int) ([]SearchResult, error)
```

**Cross-Partition Search**:
```go
// Query all partitions and merge results
func (s *DistributedMemoryService) Query(
    ctx context.Context,
    userID string,
    query string,
    limit int,
) ([]*Memory, error) {
    // 1. Generate embedding for query
    embedding, _ := s.embedder.Embed(query)

    // 2. Search each partition (parallel)
    var wg sync.WaitGroup
    results := make([][]SearchResult, len(s.localIndices))

    for partID, index := range s.localIndices {
        wg.Add(1)
        go func(pid int, idx *VectorIndex) {
            defer wg.Done()
            results[pid], _ = idx.Search(embedding, limit)
        }(partID, index)
    }

    wg.Wait()

    // 3. Merge top-k from all partitions
    merged := mergeTopK(results, limit)

    // 4. Fetch full Memory objects from backend
    return s.fetchMemories(merged)
}
```

**Performance**:
- ✅ Partition-local indices for fast search
- ✅ Parallel cross-partition queries
- ✅ HNSW provides O(log n) search
- ✅ Embedding caching reduces redundant computation
- ⚠️ Cross-partition search requires coordination

---

## Partitioning Strategies

### 1. Hash-Based (Default)
```go
partition = hash(sessionID) % numPartitions
```
**Pros**: Uniform distribution, predictable
**Cons**: No data locality guarantees

### 2. Range-Based
```go
partition = rangeIndex(sessionID, ranges)
```
**Pros**: Supports range queries
**Cons**: Potential hotspots

### 3. Consistent Hashing
```go
partition = consistentHash(sessionID, ring)
```
**Pros**: Minimal reshuffling on rebalance
**Cons**: More complex implementation

**Recommendation**: Start with hash-based, support pluggable strategies

---

## Backend Storage Options

### 1. In-Memory (Development/Testing)
```go
type InMemoryBackend struct {
    sessions  map[string]*Session
    artifacts map[string][]byte
    memories  map[string]*Memory
    mu        sync.RWMutex
}
```
**Pros**: Fast, simple
**Cons**: Not durable, limited capacity

### 2. Redis (Low-Latency Production)
```go
type RedisBackend struct {
    client *redis.Client
    prefix string  // Key prefix
    ttl    time.Duration
}
```
**Pros**: Fast, distributed, durable
**Cons**: Requires Redis cluster

### 3. PostgreSQL (Durable Production)
```go
type PostgreSQLBackend struct {
    db *sql.DB
}

// Schema
CREATE TABLE sessions (
    id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    state JSONB,
    event_history JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX idx_sessions_user_id ON sessions(user_id);
```
**Pros**: ACID, queryable, mature
**Cons**: Slower than Redis

### 4. S3 (Large-Scale Production)
```go
type S3Backend struct {
    client *s3.Client
    bucket string
}

// Key format: {prefix}/{partitionID}/{sessionID}
```
**Pros**: Unlimited scale, cheap
**Cons**: Higher latency

**Recommendation**:
- Development: InMemory
- Production: Redis + S3 (Redis for hot data, S3 for cold storage)

---

## Service Coordination

### Coordinator Pattern
```go
type ServiceCoordinator struct {
    // Partition registry
    partitions map[string]*PartitionInfo

    // Request routing
    router *RequestRouter

    // Cache invalidation
    invalidator *CacheInvalidator

    // Health monitoring
    healthCheck *HealthChecker
}

type PartitionInfo struct {
    ID       string
    Address  string
    Healthy  bool
    Load     float64
}

// Route requests to correct partition
func (c *ServiceCoordinator) Route(
    sessionID string,
) (*PartitionInfo, error)

// Invalidate caches across partitions
func (c *ServiceCoordinator) InvalidateCache(
    sessionID string,
) error
```

### Cache Invalidation
```go
// When session is updated on partition 0
session.UpdatedAt = time.Now()
backend.Update(session)

// Invalidate caches on other partitions
coordinator.InvalidateCache(session.ID)
```

**Strategies**:
1. **Write-Through**: Update backend immediately, invalidate caches
2. **Write-Back**: Update local, flush to backend periodically
3. **Event-Based**: Use event bus for cache invalidation

---

## Configuration

```go
type ServiceConfig struct {
    // Backend
    BackendType    string  // "inmemory", "redis", "postgres", "s3"
    BackendConfig  map[string]interface{}

    // Partitioning
    NumPartitions  int
    PartitionStrategy string  // "hash", "range", "consistent"

    // Caching
    EnableCache    bool
    CacheSize      int64    // Per partition
    CacheTTL       time.Duration

    // Performance
    MaxConcurrency int
    BatchSize      int

    // Fault Tolerance
    RetryAttempts  int
    RetryBackoff   time.Duration
}
```

---

## Migration Path

### Phase 1: Simple In-Memory (Week 1)
- In-memory backend
- Hash partitioning
- Basic caching
- ADK-compatible interfaces

### Phase 2: Redis Backend (Week 2)
- Redis integration
- Write-through caching
- Cache invalidation
- Production-ready

### Phase 3: Advanced Features (Week 3)
- Vector search for MemoryService
- S3 backend for ArtifactService
- Streaming support
- Advanced partitioning

### Phase 4: Optimization (Week 4)
- Performance tuning
- Load testing
- Production deployment
- Documentation

---

## Testing Strategy

### Unit Tests
- Each service in isolation
- Mock backends
- Partitioning logic
- Cache behavior

### Integration Tests
- Service + Backend
- Cross-partition operations
- Cache invalidation
- Fault tolerance

### Performance Tests
- Latency benchmarks
- Throughput testing
- Cache hit rates
- Scalability

### Spark Integration Tests
- Multi-partition execution
- Checkpoint/recovery
- Distributed queries

---

## Success Metrics

- ✅ 100% ADK API compatibility
- ✅ <10ms latency for local cache hits
- ✅ <100ms latency for backend reads
- ✅ >1000 ops/sec per partition
- ✅ Linear scalability with partitions
- ✅ 99.9% availability

---

## Next Steps

1. Implement InMemoryBackend (simple, fast)
2. Implement DistributedSessionService
3. Implement DistributedArtifactService
4. Implement DistributedMemoryService (basic, no vector search initially)
5. Write comprehensive tests
6. Add Redis backend
7. Add vector search to MemoryService
8. Performance optimization

**Timeline**: 3-4 weeks for full implementation and testing

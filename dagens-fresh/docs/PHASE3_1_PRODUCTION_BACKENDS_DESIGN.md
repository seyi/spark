# Phase 3.1: Production Backends Design

**Goal**: Implement production-ready backend storage for distributed services using Redis and PostgreSQL.

## Overview

Phase 3.1 extends the distributed services (Phase 3) with production-grade persistent storage backends. This enables:
- **Redis**: Low-latency caching and vector search (RedisSearch/RediSearch)
- **PostgreSQL**: Durable ACID storage with pgvector for semantic search
- **S3** (optional): Large-scale artifact storage

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Service Layer (ADK)                     │
│  SessionService │ ArtifactService │ MemoryService       │
└─────────────────┬───────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│             Backend Abstraction Layer                    │
│  SessionBackend │ ArtifactBackend │ MemoryBackend       │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────┼─────────┐
        │         │         │
┌───────▼───┐ ┌──▼──────┐ ┌▼─────────┐
│  InMemory │ │  Redis  │ │ Postgres │
│  Backend  │ │ Backend │ │ Backend  │
└───────────┘ └─────────┘ └──────────┘
```

## Redis Backend

### Technology Stack
- **Library**: `github.com/redis/go-redis/v9` (official Redis client)
- **Vector Search**: Redis Stack with RediSearch
- **Connection Pool**: Built-in connection pooling in go-redis
- **Serialization**: JSON or MessagePack

### RedisSessionBackend

**Storage Strategy**:
```
Key Format: "session:{sessionID}"
Value: JSON-serialized Session object
TTL: Configurable (default: 24 hours)
Index: "session:user:{userID}" → Set of session IDs
```

**Operations**:
- `Get`: `GET session:{sessionID}` → deserialize JSON
- `Put`: `SET session:{sessionID} {json}` + `SADD session:user:{userID} {sessionID}`
- `Delete`: `DEL session:{sessionID}` + `SREM session:user:{userID} {sessionID}`
- `List`: `SMEMBERS session:user:{userID}` → `MGET` all session IDs

**Configuration**:
```go
type RedisSessionConfig struct {
    Addr     string        // Redis address (e.g., "localhost:6379")
    Password string        // Redis password
    DB       int           // Redis database (0-15)
    PoolSize int           // Max connections (default: 10)
    TTL      time.Duration // Session TTL (default: 24h)
}
```

### RedisArtifactBackend

**Storage Strategy**:
```
Key Format: "artifact:{sessionID}:{name}"
Value: Binary data (byte array)
TTL: Configurable (default: 7 days)
Index: "artifact:session:{sessionID}" → Set of artifact names
```

**Operations**:
- `Get`: `GET artifact:{sessionID}:{name}`
- `Put`: `SET artifact:{sessionID}:{name} {bytes}` + `SADD artifact:session:{sessionID} {name}`
- `Delete`: `DEL artifact:{sessionID}:{name}` + `SREM artifact:session:{sessionID} {name}`
- `List`: `SMEMBERS artifact:session:{sessionID}`

**Large Artifacts**:
- For artifacts > 100MB, redirect to S3
- Store S3 URL in Redis as metadata
- Transparent to service layer

### RedisMemoryBackend with Vector Search

**Storage Strategy**:
```
Key Format: "memory:{memoryID}"
Value: JSON-serialized Memory object
Vector Index: RediSearch index with HNSW
```

**RediSearch Index Schema**:
```
FT.CREATE memory_idx
  ON JSON
  PREFIX 1 "memory:"
  SCHEMA
    $.id AS id TAG
    $.user_id AS user_id TAG
    $.content AS content TEXT
    $.embedding AS embedding VECTOR HNSW 6
      TYPE FLOAT32
      DIM 1536
      DISTANCE_METRIC COSINE
    $.created_at AS created_at NUMERIC SORTABLE
```

**Operations**:
- `Put`: `JSON.SET memory:{memoryID} $ {json}` (auto-indexed)
- `Get`: `JSON.GET memory:{memoryID}`
- `Delete`: `JSON.DEL memory:{memoryID}`
- `List`: `FT.SEARCH memory_idx "@user_id:{userID}"`
- `Query` (Vector Search):
  ```
  FT.SEARCH memory_idx
    "@user_id:{userID} @embedding:[VECTOR_RANGE $radius $vec]"
    PARAMS 4 radius 0.8 vec <blob>
    SORTBY __embedding_score
    LIMIT 0 10
  ```

**Configuration**:
```go
type RedisMemoryConfig struct {
    Addr         string
    Password     string
    DB           int
    PoolSize     int
    EmbeddingDim int           // Vector dimension (default: 1536)
    IndexName    string        // RediSearch index name
    M            int           // HNSW M parameter (default: 16)
    EfConstruct  int           // HNSW ef_construction (default: 200)
}
```

## PostgreSQL Backend

### Technology Stack
- **Library**: `github.com/jackc/pgx/v5` (high-performance Postgres driver)
- **Connection Pool**: `pgxpool` (built-in connection pooling)
- **Vector Search**: `pgvector` extension
- **Migrations**: SQL scripts or `golang-migrate`

### Schema

#### Sessions Table
```sql
CREATE TABLE sessions (
    id           VARCHAR(255) PRIMARY KEY,
    user_id      VARCHAR(255) NOT NULL,
    state        JSONB,
    event_history JSONB,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_updated_at ON sessions(updated_at);
```

#### Artifacts Table
```sql
CREATE TABLE artifacts (
    session_id   VARCHAR(255) NOT NULL,
    name         VARCHAR(255) NOT NULL,
    data         BYTEA NOT NULL,
    size         BIGINT NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, name)
);

CREATE INDEX idx_artifacts_session_id ON artifacts(session_id);
CREATE INDEX idx_artifacts_size ON artifacts(size);
```

#### Memories Table (with pgvector)
```sql
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE memories (
    id          VARCHAR(255) PRIMARY KEY,
    user_id     VARCHAR(255) NOT NULL,
    content     TEXT NOT NULL,
    embedding   vector(1536),  -- pgvector column
    metadata    JSONB,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_memories_user_id ON memories(user_id);
CREATE INDEX idx_memories_embedding ON memories USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);  -- IVFFlat index for fast ANN search
```

### PostgresSessionBackend

**Operations**:
- `Get`: `SELECT * FROM sessions WHERE id = $1`
- `Put`: `INSERT INTO sessions ... ON CONFLICT (id) DO UPDATE ...`
- `Delete`: `DELETE FROM sessions WHERE id = $1`
- `List`: `SELECT * FROM sessions WHERE user_id = $1 ORDER BY updated_at DESC`

**Configuration**:
```go
type PostgresSessionConfig struct {
    Host            string
    Port            int
    Database        string
    User            string
    Password        string
    MaxConns        int           // Max connections (default: 20)
    MinConns        int           // Min connections (default: 5)
    ConnMaxLifetime time.Duration // Max connection lifetime
}
```

### PostgresArtifactBackend

**Operations**:
- `Get`: `SELECT data FROM artifacts WHERE session_id = $1 AND name = $2`
- `Put`: `INSERT INTO artifacts ... ON CONFLICT (session_id, name) DO UPDATE ...`
- `Delete`: `DELETE FROM artifacts WHERE session_id = $1 AND name = $2`
- `List`: `SELECT name FROM artifacts WHERE session_id = $1`

**Large Artifacts**:
- Store artifacts < 10MB in PostgreSQL BYTEA
- For artifacts >= 10MB, use TOAST (Postgres auto-compression)
- For artifacts > 100MB, store in S3 and keep URL in metadata

### PostgresMemoryBackend with pgvector

**Operations**:
- `Put`: `INSERT INTO memories ... ON CONFLICT (id) DO UPDATE ...`
- `Get`: `SELECT * FROM memories WHERE id = $1`
- `Delete`: `DELETE FROM memories WHERE id = $1`
- `List`: `SELECT * FROM memories WHERE user_id = $1 ORDER BY created_at DESC`
- `Query` (Vector Search):
  ```sql
  SELECT id, content, metadata, embedding <=> $1 AS distance
  FROM memories
  WHERE user_id = $2
  ORDER BY embedding <=> $1
  LIMIT $3
  ```
  - `<=>` is cosine distance operator in pgvector
  - Returns top K nearest neighbors

**Configuration**:
```go
type PostgresMemoryConfig struct {
    Host            string
    Port            int
    Database        string
    User            string
    Password        string
    MaxConns        int
    EmbeddingDim    int    // Vector dimension (default: 1536)
    IndexType       string // "ivfflat" or "hnsw" (Postgres 16+)
    IVFFlatLists    int    // Number of lists for IVFFlat (default: 100)
}
```

## Connection Management

### Redis Connection Pool
```go
client := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
    PoolSize: 10,
    MinIdleConns: 5,
    MaxRetries: 3,
    PoolTimeout: 4 * time.Second,
    IdleTimeout: 5 * time.Minute,
})
```

### PostgreSQL Connection Pool
```go
config, _ := pgxpool.ParseConfig("postgres://user:pass@localhost/db")
config.MaxConns = 20
config.MinConns = 5
config.MaxConnLifetime = 1 * time.Hour
config.MaxConnIdleTime = 30 * time.Minute

pool, _ := pgxpool.NewWithConfig(context.Background(), config)
```

## Error Handling & Retry

### Retry Strategy
- **Transient Errors**: Retry with exponential backoff
  - Network timeouts
  - Connection errors
  - Temporary unavailability
- **Permanent Errors**: Fail fast
  - Authentication errors
  - Schema errors
  - Constraint violations

### Circuit Breaker Pattern
```go
type CircuitBreaker struct {
    maxFailures  int
    resetTimeout time.Duration
    state        atomic.Value // "closed", "open", "half-open"
    failures     atomic.Int32
}
```

## Performance Considerations

### Redis Performance
- **Latency**: ~1ms for simple operations
- **Throughput**: 100K+ ops/sec (pipelined)
- **Vector Search**: ~10ms for 10K vectors (HNSW)
- **Memory**: ~1KB per session, ~12KB per 1536D vector

### PostgreSQL Performance
- **Latency**: ~5ms for simple queries
- **Throughput**: 10K+ TPS (with connection pooling)
- **Vector Search**: ~50ms for 100K vectors (IVFFlat)
- **Disk**: ~1KB per session, ~15KB per vector (with indices)

### Optimization Strategies
1. **Connection Pooling**: Reuse connections
2. **Prepared Statements**: Reduce parsing overhead
3. **Batch Operations**: Use `MGET`, `MSET`, `COPY` for bulk ops
4. **Read Replicas**: Scale read-heavy workloads
5. **Partitioning**: Shard data by user_id or time

## Testing Strategy

### Unit Tests
- Mock Redis/PostgreSQL clients
- Test serialization/deserialization
- Test error handling

### Integration Tests
- Use Testcontainers for real Redis/Postgres instances
- Test all CRUD operations
- Test connection pooling
- Test transaction handling

### Performance Tests
- Benchmark operations (Get, Put, Query)
- Load testing with 1M+ records
- Compare Redis vs PostgreSQL performance

## Migration Strategy

### From InMemory to Redis
1. Deploy Redis alongside existing services
2. Enable dual-write (InMemory + Redis)
3. Warm up Redis cache from InMemory
4. Switch reads to Redis (with fallback)
5. Remove InMemory backend

### From Redis to PostgreSQL
1. Deploy PostgreSQL with schema
2. Background migration: Copy Redis → PostgreSQL
3. Enable dual-write (Redis + PostgreSQL)
4. Switch reads to PostgreSQL (with Redis cache)
5. Deprecate Redis as primary storage

## Configuration Examples

### Development (InMemory)
```go
bundle := services.NewInMemoryServiceBundle(4)
```

### Staging (Redis)
```go
bundle := services.NewServiceBundle(services.ServiceBundleConfig{
    NumPartitions:        4,
    SessionBackend:       services.NewRedisSessionBackend(redisConfig),
    ArtifactBackend:      services.NewRedisArtifactBackend(redisConfig),
    MemoryBackend:        services.NewRedisMemoryBackend(redisConfig),
    SessionCacheEnabled:  true,  // L1 cache in front of Redis
    ArtifactCacheEnabled: true,
    MemoryCacheEnabled:   true,
})
```

### Production (PostgreSQL + Redis Cache)
```go
bundle := services.NewServiceBundle(services.ServiceBundleConfig{
    NumPartitions:        16,
    SessionBackend:       services.NewPostgresSessionBackend(pgConfig),
    ArtifactBackend:      services.NewPostgresArtifactBackend(pgConfig),
    MemoryBackend:        services.NewPostgresMemoryBackend(pgConfig),
    SessionCacheEnabled:  true,  // L1 (in-memory) + L2 (Redis)
    ArtifactCacheEnabled: true,
    MemoryCacheEnabled:   true,
})
```

## Security Considerations

### Credentials Management
- Use environment variables or secrets manager
- Never hardcode credentials
- Rotate credentials regularly

### Network Security
- Use TLS for Redis connections (`rediss://`)
- Use SSL for PostgreSQL connections (`sslmode=require`)
- Restrict network access (VPC, firewall rules)

### Data Encryption
- Redis: Enable `requirepass` and TLS
- PostgreSQL: Use `pg_crypto` for sensitive fields
- At-rest encryption: Encrypted disks/volumes

## Monitoring & Observability

### Metrics (Prometheus)
- Connection pool statistics (active, idle, wait time)
- Operation latency (p50, p95, p99)
- Error rates by operation type
- Cache hit/miss rates

### Logging
- Connection events (connect, disconnect, error)
- Slow queries (> 100ms)
- Transaction rollbacks
- Circuit breaker state changes

### Tracing (OpenTelemetry)
- Span per database operation
- Include query, latency, result size
- Propagate trace context across services

## File Structure

```
pkg/runtime/services/
├── backends.go              (existing interfaces)
├── backends_redis.go        (NEW: Redis implementations)
├── backends_postgres.go     (NEW: PostgreSQL implementations)
├── backends_redis_test.go   (NEW: Redis tests)
├── backends_postgres_test.go(NEW: PostgreSQL tests)
└── migrations/
    ├── 001_create_sessions.sql
    ├── 002_create_artifacts.sql
    └── 003_create_memories.sql
```

## Implementation Timeline

### Week 1: Redis Backend
- Day 1-2: RedisSessionBackend + tests
- Day 3-4: RedisArtifactBackend + tests
- Day 5: RedisMemoryBackend (basic, no vector search yet)

### Week 2: Redis Vector Search
- Day 1-2: RediSearch integration
- Day 3-4: Vector search implementation + tests
- Day 5: Benchmarks and optimization

### Week 3: PostgreSQL Backend
- Day 1-2: PostgresSessionBackend + PostgresArtifactBackend
- Day 3-4: PostgresMemoryBackend with pgvector
- Day 5: Integration tests with Testcontainers

### Week 4: Production Readiness
- Day 1-2: Performance benchmarks (Redis vs PostgreSQL)
- Day 3: Migration scripts and guides
- Day 4: Monitoring integration (Prometheus)
- Day 5: Documentation and examples

## Success Metrics

- ✅ All backends implement standard interfaces
- ✅ Integration tests passing (100% coverage)
- ✅ Performance benchmarks documented
- ✅ Production deployment guide available
- ✅ Redis latency < 5ms (p95)
- ✅ PostgreSQL latency < 20ms (p95)
- ✅ Vector search < 50ms for 10K vectors

## Next Steps After Phase 3.1

### Phase 3.2: Advanced Features
- S3 artifact backend for large files
- HNSW indices for faster vector search
- Multi-region replication
- Backup and restore strategies

### Phase 4: Spark Integration
- Native Spark DataFrame integration
- Distributed query pushdown
- Spark Streaming integration

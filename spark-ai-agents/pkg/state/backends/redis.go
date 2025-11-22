// Package backends provides state storage backend implementations.
package backends

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/state"
	"github.com/redis/go-redis/v9"
)

// RedisConfig configures the Redis backend
type RedisConfig struct {
	// Address is the Redis server address (host:port)
	Address string
	// Password for authentication
	Password string
	// DB number (0-15)
	DB int
	// KeyPrefix for all keys
	KeyPrefix string
	// PoolSize is the maximum number of connections
	PoolSize int
	// MaxRetries for failed operations
	MaxRetries int
	// DialTimeout for connections
	DialTimeout time.Duration
	// ReadTimeout for operations
	ReadTimeout time.Duration
	// WriteTimeout for operations
	WriteTimeout time.Duration
}

// DefaultRedisConfig returns sensible defaults
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Address:      "localhost:6379",
		DB:           0,
		KeyPrefix:    "spark-agents:",
		PoolSize:     10,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// RedisBackend implements StateBackend using Redis
type RedisBackend struct {
	client    *redis.Client
	prefix    string
	closed    bool
}

// NewRedisBackend creates a new Redis state backend
func NewRedisBackend(config RedisConfig) (*RedisBackend, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         config.Address,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisBackend{
		client: client,
		prefix: config.KeyPrefix,
	}, nil
}

// prefixKey adds the key prefix
func (r *RedisBackend) prefixKey(key string) string {
	return r.prefix + key
}

// Get retrieves a value by key
func (r *RedisBackend) Get(ctx context.Context, key string) ([]byte, error) {
	if r.closed {
		return nil, state.ErrBackendClosed
	}

	val, err := r.client.Get(ctx, r.prefixKey(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, state.ErrNotFound
		}
		return nil, err
	}

	return val, nil
}

// Set stores a value with optional TTL
func (r *RedisBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if r.closed {
		return state.ErrBackendClosed
	}

	return r.client.Set(ctx, r.prefixKey(key), value, ttl).Err()
}

// Delete removes a value
func (r *RedisBackend) Delete(ctx context.Context, key string) error {
	if r.closed {
		return state.ErrBackendClosed
	}

	return r.client.Del(ctx, r.prefixKey(key)).Err()
}

// Exists checks if a key exists
func (r *RedisBackend) Exists(ctx context.Context, key string) (bool, error) {
	if r.closed {
		return false, state.ErrBackendClosed
	}

	n, err := r.client.Exists(ctx, r.prefixKey(key)).Result()
	if err != nil {
		return false, err
	}

	return n > 0, nil
}

// List returns keys matching a pattern
func (r *RedisBackend) List(ctx context.Context, pattern string) ([]string, error) {
	if r.closed {
		return nil, state.ErrBackendClosed
	}

	keys, err := r.client.Keys(ctx, r.prefixKey(pattern)).Result()
	if err != nil {
		return nil, err
	}

	// Remove prefix from keys
	result := make([]string, len(keys))
	prefixLen := len(r.prefix)
	for i, key := range keys {
		if len(key) > prefixLen {
			result[i] = key[prefixLen:]
		} else {
			result[i] = key
		}
	}

	return result, nil
}

// Close closes the Redis connection
func (r *RedisBackend) Close() error {
	r.closed = true
	return r.client.Close()
}

// Client returns the underlying Redis client for advanced operations
func (r *RedisBackend) Client() *redis.Client {
	return r.client
}

// RedisCheckpointBackend implements CheckpointBackend using Redis
type RedisCheckpointBackend struct {
	backend *RedisBackend
}

// NewRedisCheckpointBackend creates a new Redis checkpoint backend
func NewRedisCheckpointBackend(backend *RedisBackend) *RedisCheckpointBackend {
	return &RedisCheckpointBackend{backend: backend}
}

func (r *RedisCheckpointBackend) checkpointKey(id string) string {
	return "checkpoint:" + id
}

func (r *RedisCheckpointBackend) agentCheckpointsKey(agentID string) string {
	return "agent-checkpoints:" + agentID
}

// SaveCheckpoint stores a checkpoint
func (r *RedisCheckpointBackend) SaveCheckpoint(ctx context.Context, checkpoint *state.Checkpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	// Use pipeline for atomic operations
	pipe := r.backend.client.Pipeline()

	// Store the checkpoint
	key := r.backend.prefixKey(r.checkpointKey(checkpoint.ID))
	pipe.Set(ctx, key, data, 0)

	// Add to agent's checkpoint sorted set (sorted by timestamp)
	agentKey := r.backend.prefixKey(r.agentCheckpointsKey(checkpoint.AgentID))
	pipe.ZAdd(ctx, agentKey, redis.Z{
		Score:  float64(checkpoint.Timestamp.UnixNano()),
		Member: checkpoint.ID,
	})

	_, err = pipe.Exec(ctx)
	return err
}

// LoadCheckpoint retrieves a checkpoint by ID
func (r *RedisCheckpointBackend) LoadCheckpoint(ctx context.Context, id string) (*state.Checkpoint, error) {
	data, err := r.backend.Get(ctx, r.checkpointKey(id))
	if err != nil {
		return nil, err
	}

	var checkpoint state.Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

// LoadLatestCheckpoint retrieves the most recent checkpoint for an agent
func (r *RedisCheckpointBackend) LoadLatestCheckpoint(ctx context.Context, agentID string) (*state.Checkpoint, error) {
	agentKey := r.backend.prefixKey(r.agentCheckpointsKey(agentID))

	// Get the highest scored (most recent) checkpoint ID
	ids, err := r.backend.client.ZRevRange(ctx, agentKey, 0, 0).Result()
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, state.ErrNotFound
	}

	return r.LoadCheckpoint(ctx, ids[0])
}

// ListCheckpoints returns checkpoint metadata for an agent
func (r *RedisCheckpointBackend) ListCheckpoints(ctx context.Context, agentID string, limit int) ([]*state.CheckpointMeta, error) {
	agentKey := r.backend.prefixKey(r.agentCheckpointsKey(agentID))

	// Get checkpoint IDs (most recent first)
	ids, err := r.backend.client.ZRevRange(ctx, agentKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	metas := make([]*state.CheckpointMeta, 0, len(ids))
	for _, id := range ids {
		checkpoint, err := r.LoadCheckpoint(ctx, id)
		if err != nil {
			continue // Skip if checkpoint not found
		}

		metas = append(metas, &state.CheckpointMeta{
			ID:        checkpoint.ID,
			AgentID:   checkpoint.AgentID,
			SessionID: checkpoint.SessionID,
			Timestamp: checkpoint.Timestamp,
			Version:   checkpoint.Version,
			Metadata:  checkpoint.Metadata,
		})
	}

	return metas, nil
}

// DeleteCheckpoint removes a checkpoint
func (r *RedisCheckpointBackend) DeleteCheckpoint(ctx context.Context, id string) error {
	// First load to get agent ID
	checkpoint, err := r.LoadCheckpoint(ctx, id)
	if err != nil {
		return err
	}

	pipe := r.backend.client.Pipeline()

	// Delete the checkpoint
	key := r.backend.prefixKey(r.checkpointKey(id))
	pipe.Del(ctx, key)

	// Remove from agent's sorted set
	agentKey := r.backend.prefixKey(r.agentCheckpointsKey(checkpoint.AgentID))
	pipe.ZRem(ctx, agentKey, id)

	_, err = pipe.Exec(ctx)
	return err
}

// DeleteOldCheckpoints removes checkpoints older than the specified time
func (r *RedisCheckpointBackend) DeleteOldCheckpoints(ctx context.Context, agentID string, before time.Time) (int, error) {
	agentKey := r.backend.prefixKey(r.agentCheckpointsKey(agentID))

	// Get checkpoint IDs older than 'before'
	ids, err := r.backend.client.ZRangeByScore(ctx, agentKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", before.UnixNano()),
	}).Result()
	if err != nil {
		return 0, err
	}

	if len(ids) == 0 {
		return 0, nil
	}

	pipe := r.backend.client.Pipeline()

	// Delete each checkpoint
	for _, id := range ids {
		key := r.backend.prefixKey(r.checkpointKey(id))
		pipe.Del(ctx, key)
	}

	// Remove from sorted set
	pipe.ZRemRangeByScore(ctx, agentKey, "-inf", fmt.Sprintf("%d", before.UnixNano()))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(ids), nil
}

// RedisSessionBackend implements SessionBackend using Redis
type RedisSessionBackend struct {
	backend     *RedisBackend
	sessionTTL  time.Duration
}

// NewRedisSessionBackend creates a new Redis session backend
func NewRedisSessionBackend(backend *RedisBackend, sessionTTL time.Duration) *RedisSessionBackend {
	return &RedisSessionBackend{
		backend:    backend,
		sessionTTL: sessionTTL,
	}
}

func (r *RedisSessionBackend) sessionKey(sessionID string) string {
	return "session:" + sessionID
}

func (r *RedisSessionBackend) sessionHistoryKey(sessionID string) string {
	return "session-history:" + sessionID
}

func (r *RedisSessionBackend) agentSessionsKey(agentID string) string {
	return "agent-sessions:" + agentID
}

// CreateSession creates a new session
func (r *RedisSessionBackend) CreateSession(ctx context.Context, session *state.SessionState) error {
	session.CreatedAt = time.Now()
	session.UpdatedAt = session.CreatedAt
	session.Version = 1

	if r.sessionTTL > 0 {
		session.ExpiresAt = session.CreatedAt.Add(r.sessionTTL)
	}

	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	pipe := r.backend.client.Pipeline()

	// Store session
	key := r.backend.prefixKey(r.sessionKey(session.SessionID))
	pipe.Set(ctx, key, data, r.sessionTTL)

	// Add to agent's session set
	agentKey := r.backend.prefixKey(r.agentSessionsKey(session.AgentID))
	pipe.SAdd(ctx, agentKey, session.SessionID)

	_, err = pipe.Exec(ctx)
	return err
}

// GetSession retrieves a session
func (r *RedisSessionBackend) GetSession(ctx context.Context, sessionID string) (*state.SessionState, error) {
	data, err := r.backend.Get(ctx, r.sessionKey(sessionID))
	if err != nil {
		return nil, err
	}

	var session state.SessionState
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, err
	}

	return &session, nil
}

// UpdateSession updates a session with optimistic locking
func (r *RedisSessionBackend) UpdateSession(ctx context.Context, session *state.SessionState) error {
	key := r.backend.prefixKey(r.sessionKey(session.SessionID))

	// Use WATCH for optimistic locking
	err := r.backend.client.Watch(ctx, func(tx *redis.Tx) error {
		// Get current session
		data, err := tx.Get(ctx, key).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return state.ErrNotFound
			}
			return err
		}

		var current state.SessionState
		if err := json.Unmarshal(data, &current); err != nil {
			return err
		}

		// Check version
		if current.Version != session.Version {
			return state.ErrVersionConflict
		}

		// Update
		session.Version++
		session.UpdatedAt = time.Now()

		newData, err := json.Marshal(session)
		if err != nil {
			return err
		}

		// Calculate remaining TTL
		var ttl time.Duration
		if !session.ExpiresAt.IsZero() {
			ttl = time.Until(session.ExpiresAt)
			if ttl < 0 {
				ttl = 0
			}
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, key, newData, ttl)
			return nil
		})

		return err
	}, key)

	return err
}

// DeleteSession removes a session
func (r *RedisSessionBackend) DeleteSession(ctx context.Context, sessionID string) error {
	// Get session to find agent ID
	session, err := r.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}

	pipe := r.backend.client.Pipeline()

	// Delete session
	key := r.backend.prefixKey(r.sessionKey(sessionID))
	pipe.Del(ctx, key)

	// Delete history
	historyKey := r.backend.prefixKey(r.sessionHistoryKey(sessionID))
	pipe.Del(ctx, historyKey)

	// Remove from agent's session set
	agentKey := r.backend.prefixKey(r.agentSessionsKey(session.AgentID))
	pipe.SRem(ctx, agentKey, sessionID)

	_, err = pipe.Exec(ctx)
	return err
}

// ListSessions lists sessions for an agent
func (r *RedisSessionBackend) ListSessions(ctx context.Context, agentID string, limit int) ([]*state.SessionState, error) {
	agentKey := r.backend.prefixKey(r.agentSessionsKey(agentID))

	sessionIDs, err := r.backend.client.SMembers(ctx, agentKey).Result()
	if err != nil {
		return nil, err
	}

	sessions := make([]*state.SessionState, 0, len(sessionIDs))
	for i, id := range sessionIDs {
		if limit > 0 && i >= limit {
			break
		}

		session, err := r.GetSession(ctx, id)
		if err != nil {
			continue // Skip if not found
		}
		sessions = append(sessions, session)
	}

	return sessions, nil
}

// AddHistoryEntry adds an entry to session history
func (r *RedisSessionBackend) AddHistoryEntry(ctx context.Context, sessionID string, entry state.HistoryEntry) error {
	entry.Timestamp = time.Now()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	key := r.backend.prefixKey(r.sessionHistoryKey(sessionID))
	return r.backend.client.RPush(ctx, key, data).Err()
}

// GetHistory retrieves session history
func (r *RedisSessionBackend) GetHistory(ctx context.Context, sessionID string, limit int) ([]state.HistoryEntry, error) {
	key := r.backend.prefixKey(r.sessionHistoryKey(sessionID))

	var start, stop int64 = 0, -1
	if limit > 0 {
		// Get last N entries
		start = int64(-limit)
	}

	data, err := r.backend.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]state.HistoryEntry, 0, len(data))
	for _, d := range data {
		var entry state.HistoryEntry
		if err := json.Unmarshal([]byte(d), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

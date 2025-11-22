package backends

import (
	"context"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/state"
)

// MemoryBackend implements StateBackend using in-memory storage
// Useful for testing and single-node deployments
type MemoryBackend struct {
	data   map[string]memoryEntry
	mu     sync.RWMutex
	closed bool
}

type memoryEntry struct {
	value     []byte
	expiresAt time.Time
}

// NewMemoryBackend creates a new in-memory state backend
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		data: make(map[string]memoryEntry),
	}
}

// Get retrieves a value by key
func (m *MemoryBackend) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, state.ErrBackendClosed
	}

	entry, ok := m.data[key]
	if !ok {
		return nil, state.ErrNotFound
	}

	// Check expiration
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return nil, state.ErrNotFound
	}

	// Return a copy
	result := make([]byte, len(entry.value))
	copy(result, entry.value)
	return result, nil
}

// Set stores a value with optional TTL
func (m *MemoryBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return state.ErrBackendClosed
	}

	entry := memoryEntry{
		value: make([]byte, len(value)),
	}
	copy(entry.value, value)

	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}

	m.data[key] = entry
	return nil
}

// Delete removes a value
func (m *MemoryBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return state.ErrBackendClosed
	}

	delete(m.data, key)
	return nil
}

// Exists checks if a key exists
func (m *MemoryBackend) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return false, state.ErrBackendClosed
	}

	entry, ok := m.data[key]
	if !ok {
		return false, nil
	}

	// Check expiration
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		return false, nil
	}

	return true, nil
}

// List returns keys matching a pattern (supports * and ?)
func (m *MemoryBackend) List(ctx context.Context, pattern string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, state.ErrBackendClosed
	}

	now := time.Now()
	var keys []string

	for key, entry := range m.data {
		// Skip expired entries
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			continue
		}

		// Match pattern
		matched, _ := filepath.Match(pattern, key)
		if matched {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)
	return keys, nil
}

// Close closes the backend
func (m *MemoryBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.data = nil
	return nil
}

// Cleanup removes expired entries
func (m *MemoryBackend) Cleanup() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	removed := 0

	for key, entry := range m.data {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(m.data, key)
			removed++
		}
	}

	return removed
}

// Size returns the number of entries
func (m *MemoryBackend) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// MemoryCheckpointBackend implements CheckpointBackend using in-memory storage
type MemoryCheckpointBackend struct {
	checkpoints map[string]*state.Checkpoint
	byAgent     map[string][]string // agentID -> []checkpointID (sorted by time)
	mu          sync.RWMutex
}

// NewMemoryCheckpointBackend creates a new in-memory checkpoint backend
func NewMemoryCheckpointBackend() *MemoryCheckpointBackend {
	return &MemoryCheckpointBackend{
		checkpoints: make(map[string]*state.Checkpoint),
		byAgent:     make(map[string][]string),
	}
}

// SaveCheckpoint stores a checkpoint
func (m *MemoryCheckpointBackend) SaveCheckpoint(ctx context.Context, checkpoint *state.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store checkpoint
	m.checkpoints[checkpoint.ID] = checkpoint

	// Update agent index
	ids := m.byAgent[checkpoint.AgentID]
	ids = append(ids, checkpoint.ID)

	// Sort by timestamp (newest first)
	sort.Slice(ids, func(i, j int) bool {
		ci := m.checkpoints[ids[i]]
		cj := m.checkpoints[ids[j]]
		return ci.Timestamp.After(cj.Timestamp)
	})

	m.byAgent[checkpoint.AgentID] = ids
	return nil
}

// LoadCheckpoint retrieves a checkpoint by ID
func (m *MemoryCheckpointBackend) LoadCheckpoint(ctx context.Context, id string) (*state.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	checkpoint, ok := m.checkpoints[id]
	if !ok {
		return nil, state.ErrNotFound
	}

	return checkpoint, nil
}

// LoadLatestCheckpoint retrieves the most recent checkpoint for an agent
func (m *MemoryCheckpointBackend) LoadLatestCheckpoint(ctx context.Context, agentID string) (*state.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byAgent[agentID]
	if !ok || len(ids) == 0 {
		return nil, state.ErrNotFound
	}

	return m.checkpoints[ids[0]], nil
}

// ListCheckpoints returns checkpoint metadata for an agent
func (m *MemoryCheckpointBackend) ListCheckpoints(ctx context.Context, agentID string, limit int) ([]*state.CheckpointMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byAgent[agentID]
	if !ok {
		return []*state.CheckpointMeta{}, nil
	}

	count := len(ids)
	if limit > 0 && limit < count {
		count = limit
	}

	metas := make([]*state.CheckpointMeta, count)
	for i := 0; i < count; i++ {
		cp := m.checkpoints[ids[i]]
		metas[i] = &state.CheckpointMeta{
			ID:        cp.ID,
			AgentID:   cp.AgentID,
			SessionID: cp.SessionID,
			Timestamp: cp.Timestamp,
			Version:   cp.Version,
			Metadata:  cp.Metadata,
		}
	}

	return metas, nil
}

// DeleteCheckpoint removes a checkpoint
func (m *MemoryCheckpointBackend) DeleteCheckpoint(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	checkpoint, ok := m.checkpoints[id]
	if !ok {
		return state.ErrNotFound
	}

	delete(m.checkpoints, id)

	// Update agent index
	ids := m.byAgent[checkpoint.AgentID]
	for i, cid := range ids {
		if cid == id {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	m.byAgent[checkpoint.AgentID] = ids

	return nil
}

// DeleteOldCheckpoints removes checkpoints older than the specified time
func (m *MemoryCheckpointBackend) DeleteOldCheckpoints(ctx context.Context, agentID string, before time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids, ok := m.byAgent[agentID]
	if !ok {
		return 0, nil
	}

	deleted := 0
	remaining := make([]string, 0, len(ids))

	for _, id := range ids {
		cp := m.checkpoints[id]
		if cp.Timestamp.Before(before) {
			delete(m.checkpoints, id)
			deleted++
		} else {
			remaining = append(remaining, id)
		}
	}

	m.byAgent[agentID] = remaining
	return deleted, nil
}

// MemorySessionBackend implements SessionBackend using in-memory storage
type MemorySessionBackend struct {
	sessions  map[string]*state.SessionState
	history   map[string][]state.HistoryEntry
	byAgent   map[string][]string
	mu        sync.RWMutex
}

// NewMemorySessionBackend creates a new in-memory session backend
func NewMemorySessionBackend() *MemorySessionBackend {
	return &MemorySessionBackend{
		sessions: make(map[string]*state.SessionState),
		history:  make(map[string][]state.HistoryEntry),
		byAgent:  make(map[string][]string),
	}
}

// CreateSession creates a new session
func (m *MemorySessionBackend) CreateSession(ctx context.Context, session *state.SessionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.sessions[session.SessionID]; exists {
		return state.ErrAlreadyExists
	}

	session.CreatedAt = time.Now()
	session.UpdatedAt = session.CreatedAt
	session.Version = 1

	m.sessions[session.SessionID] = session
	m.byAgent[session.AgentID] = append(m.byAgent[session.AgentID], session.SessionID)

	return nil
}

// GetSession retrieves a session
func (m *MemorySessionBackend) GetSession(ctx context.Context, sessionID string) (*state.SessionState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return nil, state.ErrNotFound
	}

	// Check expiration
	if !session.ExpiresAt.IsZero() && time.Now().After(session.ExpiresAt) {
		return nil, state.ErrNotFound
	}

	return session, nil
}

// UpdateSession updates a session
func (m *MemorySessionBackend) UpdateSession(ctx context.Context, session *state.SessionState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current, ok := m.sessions[session.SessionID]
	if !ok {
		return state.ErrNotFound
	}

	if current.Version != session.Version {
		return state.ErrVersionConflict
	}

	session.Version++
	session.UpdatedAt = time.Now()
	m.sessions[session.SessionID] = session

	return nil
}

// DeleteSession removes a session
func (m *MemorySessionBackend) DeleteSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[sessionID]
	if !ok {
		return state.ErrNotFound
	}

	delete(m.sessions, sessionID)
	delete(m.history, sessionID)

	// Remove from agent index
	ids := m.byAgent[session.AgentID]
	for i, id := range ids {
		if id == sessionID {
			ids = append(ids[:i], ids[i+1:]...)
			break
		}
	}
	m.byAgent[session.AgentID] = ids

	return nil
}

// ListSessions lists sessions for an agent
func (m *MemorySessionBackend) ListSessions(ctx context.Context, agentID string, limit int) ([]*state.SessionState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids, ok := m.byAgent[agentID]
	if !ok {
		return []*state.SessionState{}, nil
	}

	count := len(ids)
	if limit > 0 && limit < count {
		count = limit
	}

	sessions := make([]*state.SessionState, 0, count)
	for i := 0; i < count; i++ {
		session := m.sessions[ids[i]]
		if session != nil {
			sessions = append(sessions, session)
		}
	}

	return sessions, nil
}

// AddHistoryEntry adds an entry to session history
func (m *MemorySessionBackend) AddHistoryEntry(ctx context.Context, sessionID string, entry state.HistoryEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry.Timestamp = time.Now()
	m.history[sessionID] = append(m.history[sessionID], entry)
	return nil
}

// GetHistory retrieves session history
func (m *MemorySessionBackend) GetHistory(ctx context.Context, sessionID string, limit int) ([]state.HistoryEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	history, ok := m.history[sessionID]
	if !ok {
		return []state.HistoryEntry{}, nil
	}

	if limit > 0 && limit < len(history) {
		// Return last N entries
		start := len(history) - limit
		return history[start:], nil
	}

	return history, nil
}

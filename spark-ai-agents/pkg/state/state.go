// Package state provides distributed state management for AI agents.
// This includes checkpoint/restore capabilities, session state, and
// distributed coordination.
package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Common errors
var (
	ErrNotFound       = errors.New("state not found")
	ErrAlreadyExists  = errors.New("state already exists")
	ErrInvalidState   = errors.New("invalid state")
	ErrBackendClosed  = errors.New("backend is closed")
	ErrVersionConflict = errors.New("version conflict")
)

// StateBackend is the interface for state storage backends
type StateBackend interface {
	// Get retrieves a value by key
	Get(ctx context.Context, key string) ([]byte, error)
	// Set stores a value with optional TTL
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes a value
	Delete(ctx context.Context, key string) error
	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (bool, error)
	// List returns keys matching a pattern
	List(ctx context.Context, pattern string) ([]string, error)
	// Close closes the backend connection
	Close() error
}

// Checkpoint represents a point-in-time snapshot of agent state
type Checkpoint struct {
	ID        string                 `json:"id"`
	AgentID   string                 `json:"agent_id"`
	SessionID string                 `json:"session_id"`
	Timestamp time.Time              `json:"timestamp"`
	State     map[string]interface{} `json:"state"`
	Metadata  map[string]string      `json:"metadata"`
	Version   int64                  `json:"version"`
}

// CheckpointBackend is the interface for checkpoint storage
type CheckpointBackend interface {
	// SaveCheckpoint stores a checkpoint
	SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	// LoadCheckpoint retrieves a checkpoint by ID
	LoadCheckpoint(ctx context.Context, id string) (*Checkpoint, error)
	// LoadLatestCheckpoint retrieves the most recent checkpoint for an agent
	LoadLatestCheckpoint(ctx context.Context, agentID string) (*Checkpoint, error)
	// ListCheckpoints returns checkpoint metadata for an agent
	ListCheckpoints(ctx context.Context, agentID string, limit int) ([]*CheckpointMeta, error)
	// DeleteCheckpoint removes a checkpoint
	DeleteCheckpoint(ctx context.Context, id string) error
	// DeleteOldCheckpoints removes checkpoints older than the specified time
	DeleteOldCheckpoints(ctx context.Context, agentID string, before time.Time) (int, error)
}

// CheckpointMeta contains checkpoint metadata without the full state
type CheckpointMeta struct {
	ID        string            `json:"id"`
	AgentID   string            `json:"agent_id"`
	SessionID string            `json:"session_id"`
	Timestamp time.Time         `json:"timestamp"`
	Version   int64             `json:"version"`
	Metadata  map[string]string `json:"metadata"`
}

// SessionState represents the current state of an agent session
type SessionState struct {
	SessionID   string                 `json:"session_id"`
	AgentID     string                 `json:"agent_id"`
	State       map[string]interface{} `json:"state"`
	History     []HistoryEntry         `json:"history"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	ExpiresAt   time.Time              `json:"expires_at,omitempty"`
	Version     int64                  `json:"version"`
}

// HistoryEntry represents a single interaction in session history
type HistoryEntry struct {
	Role      string    `json:"role"`      // "user", "assistant", "system"
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// SessionBackend is the interface for session state storage
type SessionBackend interface {
	// CreateSession creates a new session
	CreateSession(ctx context.Context, session *SessionState) error
	// GetSession retrieves a session
	GetSession(ctx context.Context, sessionID string) (*SessionState, error)
	// UpdateSession updates a session (with optimistic locking)
	UpdateSession(ctx context.Context, session *SessionState) error
	// DeleteSession removes a session
	DeleteSession(ctx context.Context, sessionID string) error
	// ListSessions lists sessions for an agent
	ListSessions(ctx context.Context, agentID string, limit int) ([]*SessionState, error)
	// AddHistoryEntry adds an entry to session history
	AddHistoryEntry(ctx context.Context, sessionID string, entry HistoryEntry) error
	// GetHistory retrieves session history
	GetHistory(ctx context.Context, sessionID string, limit int) ([]HistoryEntry, error)
}

// StateManager provides a unified interface for state operations
type StateManager struct {
	backend    StateBackend
	checkpoint CheckpointBackend
	session    SessionBackend
}

// NewStateManager creates a new state manager
func NewStateManager(backend StateBackend, checkpoint CheckpointBackend, session SessionBackend) *StateManager {
	return &StateManager{
		backend:    backend,
		checkpoint: checkpoint,
		session:    session,
	}
}

// Get retrieves a typed value from state
func Get[T any](ctx context.Context, sm *StateManager, key string) (T, error) {
	var zero T
	data, err := sm.backend.Get(ctx, key)
	if err != nil {
		return zero, err
	}

	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		return zero, err
	}

	return result, nil
}

// Set stores a typed value in state
func Set[T any](ctx context.Context, sm *StateManager, key string, value T, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return sm.backend.Set(ctx, key, data, ttl)
}

// SaveCheckpoint saves a checkpoint
func (sm *StateManager) SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	if sm.checkpoint == nil {
		return errors.New("checkpoint backend not configured")
	}
	return sm.checkpoint.SaveCheckpoint(ctx, checkpoint)
}

// LoadCheckpoint loads a checkpoint
func (sm *StateManager) LoadCheckpoint(ctx context.Context, id string) (*Checkpoint, error) {
	if sm.checkpoint == nil {
		return nil, errors.New("checkpoint backend not configured")
	}
	return sm.checkpoint.LoadCheckpoint(ctx, id)
}

// LoadLatestCheckpoint loads the most recent checkpoint for an agent
func (sm *StateManager) LoadLatestCheckpoint(ctx context.Context, agentID string) (*Checkpoint, error) {
	if sm.checkpoint == nil {
		return nil, errors.New("checkpoint backend not configured")
	}
	return sm.checkpoint.LoadLatestCheckpoint(ctx, agentID)
}

// CreateSession creates a new session
func (sm *StateManager) CreateSession(ctx context.Context, session *SessionState) error {
	if sm.session == nil {
		return errors.New("session backend not configured")
	}
	return sm.session.CreateSession(ctx, session)
}

// GetSession retrieves a session
func (sm *StateManager) GetSession(ctx context.Context, sessionID string) (*SessionState, error) {
	if sm.session == nil {
		return nil, errors.New("session backend not configured")
	}
	return sm.session.GetSession(ctx, sessionID)
}

// UpdateSession updates a session
func (sm *StateManager) UpdateSession(ctx context.Context, session *SessionState) error {
	if sm.session == nil {
		return errors.New("session backend not configured")
	}
	return sm.session.UpdateSession(ctx, session)
}

// Close closes all backends
func (sm *StateManager) Close() error {
	var errs []error

	if sm.backend != nil {
		if err := sm.backend.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

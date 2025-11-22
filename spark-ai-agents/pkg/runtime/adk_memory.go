// Copyright 2025 Apache Spark AI Agents
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package runtime provides ADK-compatible Memory interface for agents.
//
// This file implements the Google ADK Go Memory interface pattern:
// - AddSession for ingesting entire sessions into memory
// - Search for semantic search across memories
// - User/App/Session scoped memory isolation
package runtime

import (
	"context"
	"strings"
	"sync"
	"time"
)

// ============================================================================
// ADK-Compatible Memory Interface
// ============================================================================

// ADKMemory provides ADK-compatible memory interface for agents.
// This matches Google ADK Go's Memory interface pattern.
type ADKMemory interface {
	// AddSession adds a complete session to memory for later recall
	AddSession(ctx context.Context, session *Session) error
	// Search performs semantic search across memories
	Search(ctx context.Context, query string) (*SearchResponse, error)
}

// SearchResponse contains memory search results (ADK pattern)
type SearchResponse struct {
	Memories []MemoryEntry `json:"memories"`
}

// MemoryEntry represents a single memory item in search results (ADK pattern)
type MemoryEntry struct {
	Content   interface{} `json:"content"`
	Author    string      `json:"author"`
	Timestamp time.Time   `json:"timestamp"`
	// Extended fields beyond ADK
	SessionID string                 `json:"session_id,omitempty"`
	EventID   string                 `json:"event_id,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// ============================================================================
// Memory Service Implementation
// ============================================================================

// ADKMemoryService implements ADKMemory interface with user/app scoping
type ADKMemoryService struct {
	// Backend storage
	backend MemoryService

	// Scoping
	userID    string
	appName   string
	sessionID string

	// In-memory session store (for AddSession)
	sessions map[string]*SessionMemory
	mu       sync.RWMutex

	// Optional: embedding provider for semantic search
	embeddingProvider EmbeddingProviderFunc
}

// EmbeddingProviderFunc generates embeddings from text
type EmbeddingProviderFunc func(ctx context.Context, text string) ([]float64, error)

// SessionMemory stores a session's content for memory search
type SessionMemory struct {
	Session   *Session
	Events    []MemoryEntry
	AddedAt   time.Time
	Embedding []float64
}

// ADKMemoryConfig configures the ADK memory service
type ADKMemoryConfig struct {
	Backend           MemoryService
	UserID            string
	AppName           string
	SessionID         string
	EmbeddingProvider EmbeddingProviderFunc
}

// NewADKMemoryService creates an ADK-compatible memory service
func NewADKMemoryService(config ADKMemoryConfig) *ADKMemoryService {
	return &ADKMemoryService{
		backend:           config.Backend,
		userID:            config.UserID,
		appName:           config.AppName,
		sessionID:         config.SessionID,
		sessions:          make(map[string]*SessionMemory),
		embeddingProvider: config.EmbeddingProvider,
	}
}

// AddSession adds a complete session to memory (ADK pattern)
func (m *ADKMemoryService) AddSession(ctx context.Context, session *Session) error {
	if session == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Extract memory entries from session events
	entries := make([]MemoryEntry, 0, len(session.EventHistory))
	for _, event := range session.EventHistory {
		entry := MemoryEntry{
			Content:   event.Content,
			Author:    event.Author,
			Timestamp: event.Timestamp,
			SessionID: session.ID,
			EventID:   event.ID,
			Metadata:  event.Metadata,
		}
		entries = append(entries, entry)
	}

	// Store session memory
	sessionMem := &SessionMemory{
		Session: session,
		Events:  entries,
		AddedAt: time.Now(),
	}

	// Generate session embedding if provider available
	if m.embeddingProvider != nil {
		// Concatenate all text content for embedding
		var textParts []string
		for _, entry := range entries {
			if text, ok := entry.Content.(string); ok {
				textParts = append(textParts, text)
			}
		}
		if len(textParts) > 0 {
			fullText := strings.Join(textParts, " ")
			if embedding, err := m.embeddingProvider(ctx, fullText); err == nil {
				sessionMem.Embedding = embedding
			}
		}
	}

	m.sessions[session.ID] = sessionMem

	// Also store individual memories to backend for persistence
	for i, entry := range entries {
		if text, ok := entry.Content.(string); ok {
			memory := &Memory{
				ID:      session.ID + "_" + string(rune(i)),
				Content: text,
				Metadata: map[string]interface{}{
					"user_id":    session.UserID,
					"session_id": session.ID,
					"author":     entry.Author,
					"event_id":   entry.EventID,
					"app_name":   m.appName,
				},
				CreatedAt: entry.Timestamp,
			}

			// Generate embedding if provider available
			if m.embeddingProvider != nil {
				if embedding, err := m.embeddingProvider(ctx, text); err == nil {
					memory.Embedding = embedding
				}
			}

			if m.backend != nil {
				if err := m.backend.Store(ctx, m.userID, memory); err != nil {
					// Log but don't fail
					continue
				}
			}
		}
	}

	return nil
}

// Search performs semantic search across memories (ADK pattern)
func (m *ADKMemoryService) Search(ctx context.Context, query string) (*SearchResponse, error) {
	if query == "" {
		return &SearchResponse{Memories: []MemoryEntry{}}, nil
	}

	// Search in backend
	var backendResults []*Memory
	if m.backend != nil {
		results, err := m.backend.Query(ctx, m.userID, query)
		if err == nil {
			backendResults = results
		}
	}

	// Search in in-memory sessions (simple text matching if no embeddings)
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Convert backend results to entries
	entries := make([]MemoryEntry, 0)
	for _, mem := range backendResults {
		entry := MemoryEntry{
			Content:   mem.Content,
			Timestamp: mem.CreatedAt,
			Metadata:  mem.Metadata,
		}
		if mem.Metadata != nil {
			if author, ok := mem.Metadata["author"].(string); ok {
				entry.Author = author
			}
			if sessionID, ok := mem.Metadata["session_id"].(string); ok {
				entry.SessionID = sessionID
			}
			if eventID, ok := mem.Metadata["event_id"].(string); ok {
				entry.EventID = eventID
			}
		}
		entries = append(entries, entry)
	}

	// Also search in-memory sessions (for recently added sessions not yet in backend)
	queryLower := strings.ToLower(query)
	for _, sessionMem := range m.sessions {
		for _, event := range sessionMem.Events {
			if text, ok := event.Content.(string); ok {
				if strings.Contains(strings.ToLower(text), queryLower) {
					// Check if already in results
					found := false
					for _, existing := range entries {
						if existing.EventID == event.EventID {
							found = true
							break
						}
					}
					if !found {
						entries = append(entries, event)
					}
				}
			}
		}
	}

	return &SearchResponse{Memories: entries}, nil
}

// ============================================================================
// Tool Context Memory Support
// ============================================================================

// ToolContextWithMemory extends ToolContext with memory search capability
type ToolContextWithMemory struct {
	*ToolContext
	memory ADKMemory
}

// NewToolContextWithMemory creates a tool context with memory support
func NewToolContextWithMemory(toolCtx *ToolContext, memory ADKMemory) *ToolContextWithMemory {
	return &ToolContextWithMemory{
		ToolContext: toolCtx,
		memory:      memory,
	}
}

// SearchMemory performs semantic search on agent memory (ADK pattern)
func (t *ToolContextWithMemory) SearchMemory(ctx context.Context, query string) (*SearchResponse, error) {
	if t.memory == nil {
		return &SearchResponse{Memories: []MemoryEntry{}}, nil
	}
	return t.memory.Search(ctx, query)
}

// ============================================================================
// Invocation Context Memory Support
// ============================================================================

// InvocationContextWithMemory extends InvocationContext with memory
type InvocationContextWithMemory struct {
	*InvocationContext
	memory ADKMemory
}

// NewInvocationContextWithMemory creates an invocation context with memory
func NewInvocationContextWithMemory(invCtx *InvocationContext, memory ADKMemory) *InvocationContextWithMemory {
	return &InvocationContextWithMemory{
		InvocationContext: invCtx,
		memory:            memory,
	}
}

// Memory returns the ADK memory interface
func (i *InvocationContextWithMemory) Memory() ADKMemory {
	return i.memory
}

// ============================================================================
// In-Memory Service (for testing)
// ============================================================================

// InMemoryADKMemoryService provides an in-memory ADK memory implementation
type InMemoryADKMemoryService struct {
	memories map[string][]MemoryEntry // userID -> entries
	mu       sync.RWMutex
}

// NewInMemoryADKMemoryService creates an in-memory ADK memory service
func NewInMemoryADKMemoryService() *InMemoryADKMemoryService {
	return &InMemoryADKMemoryService{
		memories: make(map[string][]MemoryEntry),
	}
}

// AddSession adds a session to memory
func (m *InMemoryADKMemoryService) AddSession(ctx context.Context, session *Session) error {
	if session == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	entries := make([]MemoryEntry, 0, len(session.EventHistory))
	for _, event := range session.EventHistory {
		entry := MemoryEntry{
			Content:   event.Content,
			Author:    event.Author,
			Timestamp: event.Timestamp,
			SessionID: session.ID,
			EventID:   event.ID,
		}
		entries = append(entries, entry)
	}

	if m.memories[session.UserID] == nil {
		m.memories[session.UserID] = make([]MemoryEntry, 0)
	}
	m.memories[session.UserID] = append(m.memories[session.UserID], entries...)

	return nil
}

// Search performs text search across memories
func (m *InMemoryADKMemoryService) Search(ctx context.Context, query string) (*SearchResponse, error) {
	if query == "" {
		return &SearchResponse{Memories: []MemoryEntry{}}, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	queryLower := strings.ToLower(query)
	results := make([]MemoryEntry, 0)

	for _, entries := range m.memories {
		for _, entry := range entries {
			if text, ok := entry.Content.(string); ok {
				if strings.Contains(strings.ToLower(text), queryLower) {
					results = append(results, entry)
				}
			}
		}
	}

	return &SearchResponse{Memories: results}, nil
}

// SearchForUser performs search scoped to a user
func (m *InMemoryADKMemoryService) SearchForUser(ctx context.Context, userID, query string) (*SearchResponse, error) {
	if query == "" {
		return &SearchResponse{Memories: []MemoryEntry{}}, nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	queryLower := strings.ToLower(query)
	results := make([]MemoryEntry, 0)

	entries := m.memories[userID]
	for _, entry := range entries {
		if text, ok := entry.Content.(string); ok {
			if strings.Contains(strings.ToLower(text), queryLower) {
				results = append(results, entry)
			}
		}
	}

	return &SearchResponse{Memories: results}, nil
}

// Clear removes all memories for a user
func (m *InMemoryADKMemoryService) Clear(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.memories, userID)
}

// ClearAll removes all memories
func (m *InMemoryADKMemoryService) ClearAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memories = make(map[string][]MemoryEntry)
}

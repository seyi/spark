// Package artifacts provides structured artifact management for agent outputs
// Inspired by ADK's artifact handling system
package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ArtifactStore manages agent artifacts
type ArtifactStore interface {
	// Save stores an artifact
	Save(ctx context.Context, artifact *Artifact) error

	// Load retrieves an artifact by ID
	Load(ctx context.Context, artifactID string) (*Artifact, error)

	// List retrieves artifacts matching filters
	List(ctx context.Context, filters ArtifactFilters) ([]*Artifact, error)

	// Delete removes an artifact
	Delete(ctx context.Context, artifactID string) error

	// GetVersion retrieves a specific version of an artifact
	GetVersion(ctx context.Context, artifactID string, version int) (*Artifact, error)
}

// Artifact represents an agent output artifact
type Artifact struct {
	ID          string
	Type        ArtifactType
	Content     []byte
	ContentType string // MIME type
	Metadata    map[string]interface{}
	AgentID     string
	SessionID   string
	TaskID      string
	Version     int
	Checksum    string
	Size        int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	ExpiresAt   *time.Time
	Tags        []string
	ParentID    string // For versioning
}

// ArtifactType defines the type of artifact
type ArtifactType string

const (
	ArtifactTypeText      ArtifactType = "text"
	ArtifactTypeCode      ArtifactType = "code"
	ArtifactTypeImage     ArtifactType = "image"
	ArtifactTypeAudio     ArtifactType = "audio"
	ArtifactTypeVideo     ArtifactType = "video"
	ArtifactTypeData      ArtifactType = "data"
	ArtifactTypeJSON      ArtifactType = "json"
	ArtifactTypeMarkdown  ArtifactType = "markdown"
	ArtifactTypePDF       ArtifactType = "pdf"
	ArtifactTypeArchive   ArtifactType = "archive"
	ArtifactTypeBinary    ArtifactType = "binary"
)

// ArtifactFilters defines search filters
type ArtifactFilters struct {
	AgentID   string
	SessionID string
	TaskID    string
	Type      ArtifactType
	Tags      []string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
	Offset    int
}

// InMemoryArtifactStore implements an in-memory artifact store
type InMemoryArtifactStore struct {
	artifacts map[string]*Artifact
	versions  map[string][]*Artifact // artifactID -> versions
	mu        sync.RWMutex
}

// NewInMemoryArtifactStore creates a new in-memory store
func NewInMemoryArtifactStore() *InMemoryArtifactStore {
	return &InMemoryArtifactStore{
		artifacts: make(map[string]*Artifact),
		versions:  make(map[string][]*Artifact),
	}
}

// Save stores an artifact
func (s *InMemoryArtifactStore) Save(ctx context.Context, artifact *Artifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if artifact.ID == "" {
		artifact.ID = uuid.New().String()
	}

	now := time.Now()
	artifact.CreatedAt = now
	artifact.UpdatedAt = now

	// Calculate checksum
	artifact.Checksum = calculateChecksum(artifact.Content)
	artifact.Size = int64(len(artifact.Content))

	// Check if this is an update to existing artifact
	if existing, exists := s.artifacts[artifact.ID]; exists {
		// Create new version
		artifact.Version = existing.Version + 1
		artifact.ParentID = existing.ID

		// Store version history (make a deep copy to preserve the original version)
		contentCopy := make([]byte, len(existing.Content))
		copy(contentCopy, existing.Content)

		var tagsCopy []string
		if existing.Tags != nil {
			tagsCopy = make([]string, len(existing.Tags))
			copy(tagsCopy, existing.Tags)
		}

		existingCopy := &Artifact{
			ID:          existing.ID,
			Type:        existing.Type,
			Content:     contentCopy,
			ContentType: existing.ContentType,
			Checksum:    existing.Checksum,
			Size:        existing.Size,
			Version:     existing.Version,
			ParentID:    existing.ParentID,
			AgentID:     existing.AgentID,
			SessionID:   existing.SessionID,
			TaskID:      existing.TaskID,
			Tags:        tagsCopy,
			Metadata:    existing.Metadata,
			CreatedAt:   existing.CreatedAt,
			UpdatedAt:   existing.UpdatedAt,
			ExpiresAt:   existing.ExpiresAt,
		}
		s.versions[artifact.ID] = append(s.versions[artifact.ID], existingCopy)
	} else {
		artifact.Version = 1
	}

	// Store a copy of the artifact to prevent external modifications
	contentCopy := make([]byte, len(artifact.Content))
	copy(contentCopy, artifact.Content)

	var tagsCopy []string
	if artifact.Tags != nil {
		tagsCopy = make([]string, len(artifact.Tags))
		copy(tagsCopy, artifact.Tags)
	}

	storedArtifact := &Artifact{
		ID:          artifact.ID,
		Type:        artifact.Type,
		Content:     contentCopy,
		ContentType: artifact.ContentType,
		Checksum:    artifact.Checksum,
		Size:        artifact.Size,
		Version:     artifact.Version,
		ParentID:    artifact.ParentID,
		AgentID:     artifact.AgentID,
		SessionID:   artifact.SessionID,
		TaskID:      artifact.TaskID,
		Tags:        tagsCopy,
		Metadata:    artifact.Metadata,
		CreatedAt:   artifact.CreatedAt,
		UpdatedAt:   artifact.UpdatedAt,
		ExpiresAt:   artifact.ExpiresAt,
	}
	s.artifacts[artifact.ID] = storedArtifact
	return nil
}

// Load retrieves an artifact
func (s *InMemoryArtifactStore) Load(ctx context.Context, artifactID string) (*Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	artifact, exists := s.artifacts[artifactID]
	if !exists {
		return nil, fmt.Errorf("artifact %s not found", artifactID)
	}

	// Check expiration
	if artifact.ExpiresAt != nil && time.Now().After(*artifact.ExpiresAt) {
		return nil, fmt.Errorf("artifact %s has expired", artifactID)
	}

	return artifact, nil
}

// List retrieves artifacts matching filters
func (s *InMemoryArtifactStore) List(ctx context.Context, filters ArtifactFilters) ([]*Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	results := make([]*Artifact, 0)

	for _, artifact := range s.artifacts {
		if s.matchesFilters(artifact, filters) {
			results = append(results, artifact)
		}
	}

	// Apply pagination
	start := filters.Offset
	if start > len(results) {
		start = len(results)
	}

	end := start + filters.Limit
	if filters.Limit == 0 || end > len(results) {
		end = len(results)
	}

	if start < end {
		return results[start:end], nil
	}

	return results, nil
}

// matchesFilters checks if an artifact matches the filters
func (s *InMemoryArtifactStore) matchesFilters(artifact *Artifact, filters ArtifactFilters) bool {
	if filters.AgentID != "" && artifact.AgentID != filters.AgentID {
		return false
	}

	if filters.SessionID != "" && artifact.SessionID != filters.SessionID {
		return false
	}

	if filters.TaskID != "" && artifact.TaskID != filters.TaskID {
		return false
	}

	if filters.Type != "" && artifact.Type != filters.Type {
		return false
	}

	if len(filters.Tags) > 0 {
		if !containsAllTags(artifact.Tags, filters.Tags) {
			return false
		}
	}

	if !filters.StartTime.IsZero() && artifact.CreatedAt.Before(filters.StartTime) {
		return false
	}

	if !filters.EndTime.IsZero() && artifact.CreatedAt.After(filters.EndTime) {
		return false
	}

	// Check expiration
	if artifact.ExpiresAt != nil && time.Now().After(*artifact.ExpiresAt) {
		return false
	}

	return true
}

// Delete removes an artifact
func (s *InMemoryArtifactStore) Delete(ctx context.Context, artifactID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.artifacts[artifactID]; !exists {
		return fmt.Errorf("artifact %s not found", artifactID)
	}

	delete(s.artifacts, artifactID)
	delete(s.versions, artifactID)

	return nil
}

// GetVersion retrieves a specific version
func (s *InMemoryArtifactStore) GetVersion(ctx context.Context, artifactID string, version int) (*Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if it's the current version
	if current, exists := s.artifacts[artifactID]; exists && current.Version == version {
		return current, nil
	}

	// Check version history
	if versions, exists := s.versions[artifactID]; exists {
		for _, v := range versions {
			if v.Version == version {
				return v, nil
			}
		}
	}

	return nil, fmt.Errorf("artifact %s version %d not found", artifactID, version)
}

// Helper functions

func calculateChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func containsAllTags(artifactTags, filterTags []string) bool {
	tagMap := make(map[string]bool)
	for _, tag := range artifactTags {
		tagMap[tag] = true
	}

	for _, filterTag := range filterTags {
		if !tagMap[filterTag] {
			return false
		}
	}

	return true
}

// ArtifactBuilder helps construct artifacts
type ArtifactBuilder struct {
	artifact *Artifact
}

// NewArtifactBuilder creates a new builder
func NewArtifactBuilder() *ArtifactBuilder {
	return &ArtifactBuilder{
		artifact: &Artifact{
			Metadata: make(map[string]interface{}),
			Tags:     make([]string, 0),
		},
	}
}

// WithType sets the artifact type
func (b *ArtifactBuilder) WithType(t ArtifactType) *ArtifactBuilder {
	b.artifact.Type = t
	return b
}

// WithContent sets the content
func (b *ArtifactBuilder) WithContent(content []byte) *ArtifactBuilder {
	b.artifact.Content = content
	return b
}

// WithContentType sets the MIME type
func (b *ArtifactBuilder) WithContentType(contentType string) *ArtifactBuilder {
	b.artifact.ContentType = contentType
	return b
}

// WithAgentID sets the agent ID
func (b *ArtifactBuilder) WithAgentID(agentID string) *ArtifactBuilder {
	b.artifact.AgentID = agentID
	return b
}

// WithSessionID sets the session ID
func (b *ArtifactBuilder) WithSessionID(sessionID string) *ArtifactBuilder {
	b.artifact.SessionID = sessionID
	return b
}

// WithTaskID sets the task ID
func (b *ArtifactBuilder) WithTaskID(taskID string) *ArtifactBuilder {
	b.artifact.TaskID = taskID
	return b
}

// WithTags adds tags
func (b *ArtifactBuilder) WithTags(tags ...string) *ArtifactBuilder {
	b.artifact.Tags = append(b.artifact.Tags, tags...)
	return b
}

// WithMetadata adds metadata
func (b *ArtifactBuilder) WithMetadata(key string, value interface{}) *ArtifactBuilder {
	b.artifact.Metadata[key] = value
	return b
}

// WithExpiration sets expiration time
func (b *ArtifactBuilder) WithExpiration(expiresAt time.Time) *ArtifactBuilder {
	b.artifact.ExpiresAt = &expiresAt
	return b
}

// Build creates the artifact
func (b *ArtifactBuilder) Build() *Artifact {
	return b.artifact
}

// ArtifactReader provides streaming access to artifact content
type ArtifactReader struct {
	artifact *Artifact
	offset   int64
}

// NewArtifactReader creates a reader for an artifact
func NewArtifactReader(artifact *Artifact) *ArtifactReader {
	return &ArtifactReader{
		artifact: artifact,
		offset:   0,
	}
}

// Read implements io.Reader
func (r *ArtifactReader) Read(p []byte) (n int, err error) {
	if r.offset >= int64(len(r.artifact.Content)) {
		return 0, io.EOF
	}

	n = copy(p, r.artifact.Content[r.offset:])
	r.offset += int64(n)

	return n, nil
}

// Seek implements io.Seeker
func (r *ArtifactReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = int64(len(r.artifact.Content)) + offset
	default:
		return 0, fmt.Errorf("invalid whence value")
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative position")
	}

	r.offset = newOffset
	return newOffset, nil
}

// Close implements io.Closer
func (r *ArtifactReader) Close() error {
	return nil
}

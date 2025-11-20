package artifacts

import (
	"context"
	"testing"
	"time"
)

func TestNewInMemoryArtifactStore(t *testing.T) {
	store := NewInMemoryArtifactStore()

	if store == nil {
		t.Fatal("Store should not be nil")
	}
}

func TestSaveAndLoadArtifact(t *testing.T) {
	store := NewInMemoryArtifactStore()
	ctx := context.Background()

	artifact := &Artifact{
		Type:      ArtifactTypeText,
		Content:   []byte("test content"),
		AgentID:   "agent-1",
		SessionID: "session-1",
		Tags:      []string{"test"},
	}

	err := store.Save(ctx, artifact)
	if err != nil {
		t.Fatalf("Failed to save artifact: %v", err)
	}

	if artifact.ID == "" {
		t.Error("Artifact ID should be generated")
	}

	if artifact.Checksum == "" {
		t.Error("Checksum should be calculated")
	}

	loaded, err := store.Load(ctx, artifact.ID)
	if err != nil {
		t.Fatalf("Failed to load artifact: %v", err)
	}

	if string(loaded.Content) != "test content" {
		t.Errorf("Expected content 'test content', got '%s'", string(loaded.Content))
	}
}

func TestArtifactVersioning(t *testing.T) {
	store := NewInMemoryArtifactStore()
	ctx := context.Background()

	artifact := &Artifact{
		ID:      "artifact-1",
		Type:    ArtifactTypeText,
		Content: []byte("version 1"),
	}

	// Save version 1
	store.Save(ctx, artifact)

	if artifact.Version != 1 {
		t.Errorf("Expected version 1, got %d", artifact.Version)
	}

	// Update to version 2
	artifact.Content = []byte("version 2")
	store.Save(ctx, artifact)

	if artifact.Version != 2 {
		t.Errorf("Expected version 2, got %d", artifact.Version)
	}

	// Retrieve version 1
	v1, err := store.GetVersion(ctx, artifact.ID, 1)
	if err != nil {
		t.Fatalf("Failed to get version 1: %v", err)
	}

	if string(v1.Content) != "version 1" {
		t.Error("Version 1 content not preserved")
	}
}

func TestListArtifacts(t *testing.T) {
	store := NewInMemoryArtifactStore()
	ctx := context.Background()

	// Create multiple artifacts
	for i := 0; i < 5; i++ {
		artifact := &Artifact{
			Type:      ArtifactTypeText,
			Content:   []byte("content"),
			AgentID:   "agent-1",
			SessionID: "session-1",
			Tags:      []string{"test"},
		}
		store.Save(ctx, artifact)
	}

	// List by agent
	results, err := store.List(ctx, ArtifactFilters{
		AgentID: "agent-1",
	})
	if err != nil {
		t.Fatalf("Failed to list artifacts: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 artifacts, got %d", len(results))
	}

	// List by tags
	results, err = store.List(ctx, ArtifactFilters{
		Tags: []string{"test"},
	})
	if err != nil {
		t.Fatalf("Failed to list by tags: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 artifacts with tag 'test', got %d", len(results))
	}
}

func TestArtifactBuilder(t *testing.T) {
	artifact := NewArtifactBuilder().
		WithType(ArtifactTypeCode).
		WithContent([]byte("print('hello')")).
		WithContentType("text/x-python").
		WithAgentID("agent-1").
		WithTags("python", "code").
		WithMetadata("language", "python").
		Build()

	if artifact.Type != ArtifactTypeCode {
		t.Errorf("Expected type Code, got %s", artifact.Type)
	}

	if len(artifact.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(artifact.Tags))
	}

	if artifact.Metadata["language"] != "python" {
		t.Error("Metadata not set correctly")
	}
}

func TestArtifactExpiration(t *testing.T) {
	store := NewInMemoryArtifactStore()
	ctx := context.Background()

	// Create expired artifact
	expiresAt := time.Now().Add(-1 * time.Hour)
	artifact := &Artifact{
		Type:      ArtifactTypeText,
		Content:   []byte("expired"),
		ExpiresAt: &expiresAt,
	}

	store.Save(ctx, artifact)

	// Try to load expired artifact
	_, err := store.Load(ctx, artifact.ID)
	if err == nil {
		t.Error("Loading expired artifact should fail")
	}
}

func TestDeleteArtifact(t *testing.T) {
	store := NewInMemoryArtifactStore()
	ctx := context.Background()

	artifact := &Artifact{
		Type:    ArtifactTypeText,
		Content: []byte("test"),
	}

	store.Save(ctx, artifact)

	err := store.Delete(ctx, artifact.ID)
	if err != nil {
		t.Fatalf("Failed to delete artifact: %v", err)
	}

	_, err = store.Load(ctx, artifact.ID)
	if err == nil {
		t.Error("Deleted artifact should not be loadable")
	}
}

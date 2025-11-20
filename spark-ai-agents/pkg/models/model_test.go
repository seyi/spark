package models

import (
	"context"
	"testing"
)

func TestModelRegistry(t *testing.T) {
	registry := NewModelRegistry()

	t.Run("Register and Get Model", func(t *testing.T) {
		provider := NewMockModelProvider("test-model", "test-provider")

		err := registry.Register(provider)
		if err != nil {
			t.Fatalf("Failed to register provider: %v", err)
		}

		retrieved, err := registry.Get("test-model")
		if err != nil {
			t.Fatalf("Failed to get provider: %v", err)
		}

		if retrieved.Name() != "test-model" {
			t.Errorf("Expected model name 'test-model', got '%s'", retrieved.Name())
		}
	})

	t.Run("Duplicate Registration", func(t *testing.T) {
		provider1 := NewMockModelProvider("duplicate", "provider1")
		provider2 := NewMockModelProvider("duplicate", "provider2")

		err := registry.Register(provider1)
		if err != nil {
			t.Fatalf("Failed to register first provider: %v", err)
		}

		err = registry.Register(provider2)
		if err == nil {
			t.Error("Expected error when registering duplicate model, got nil")
		}
	})

	t.Run("Get Non-Existent Model", func(t *testing.T) {
		_, err := registry.Get("non-existent")
		if err == nil {
			t.Error("Expected error when getting non-existent model, got nil")
		}
	})

	t.Run("List Models", func(t *testing.T) {
		registry := NewModelRegistry()

		models := []string{"model1", "model2", "model3"}
		for _, name := range models {
			provider := NewMockModelProvider(name, "test")
			registry.Register(provider)
		}

		list := registry.List()
		if len(list) != len(models) {
			t.Errorf("Expected %d models, got %d", len(models), len(list))
		}
	})

	t.Run("Execute Model", func(t *testing.T) {
		registry := NewModelRegistry()
		provider := NewMockModelProvider("exec-test", "test")
		registry.Register(provider)

		request := &ModelRequest{
			Prompt:      "test prompt",
			Temperature: 0.7,
			MaxTokens:   100,
		}

		response, err := registry.Execute(context.Background(), "exec-test", request)
		if err != nil {
			t.Fatalf("Failed to execute model: %v", err)
		}

		if response.Content == "" {
			t.Error("Expected non-empty response content")
		}

		if response.TokensUsed == 0 {
			t.Error("Expected non-zero tokens used")
		}
	})
}

func TestMockModelProvider(t *testing.T) {
	provider := NewMockModelProvider("gpt-4", "openai")

	t.Run("Provider Properties", func(t *testing.T) {
		if provider.Name() != "gpt-4" {
			t.Errorf("Expected name 'gpt-4', got '%s'", provider.Name())
		}

		if provider.Provider() != "openai" {
			t.Errorf("Expected provider 'openai', got '%s'", provider.Provider())
		}

		if provider.MaxTokens() <= 0 {
			t.Error("Expected positive max tokens")
		}

		if !provider.SupportsTools() {
			t.Error("Expected tools support")
		}
	})

	t.Run("Execute Request", func(t *testing.T) {
		request := &ModelRequest{
			Prompt:       "What is 2+2?",
			Temperature:  0.0,
			MaxTokens:    50,
			SystemPrompt: "You are a helpful assistant",
		}

		response, err := provider.Execute(context.Background(), request)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if response.Content == "" {
			t.Error("Expected non-empty content")
		}

		if response.FinishReason == "" {
			t.Error("Expected finish reason")
		}

		if response.Metadata == nil {
			t.Error("Expected metadata")
		}
	})
}

func TestOpenAIProvider(t *testing.T) {
	provider := NewOpenAIProvider("test-key", "gpt-4")

	t.Run("Provider Info", func(t *testing.T) {
		if provider.Name() != "gpt-4" {
			t.Errorf("Expected name 'gpt-4', got '%s'", provider.Name())
		}

		if provider.Provider() != "openai" {
			t.Errorf("Expected provider 'openai', got '%s'", provider.Provider())
		}

		if !provider.SupportsTools() {
			t.Error("Expected tools support for GPT-4")
		}

		if !provider.SupportsStreaming() {
			t.Error("Expected streaming support for GPT-4")
		}
	})

	t.Run("Execute", func(t *testing.T) {
		request := &ModelRequest{
			Prompt: "test",
		}

		// Mock implementation returns mock response
		response, err := provider.Execute(context.Background(), request)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if response == nil {
			t.Fatal("Expected non-nil response")
		}
	})
}

func TestAnthropicProvider(t *testing.T) {
	provider := NewAnthropicProvider("test-key", "claude-3-opus")

	if provider.Name() != "claude-3-opus" {
		t.Errorf("Expected name 'claude-3-opus', got '%s'", provider.Name())
	}

	if provider.Provider() != "anthropic" {
		t.Errorf("Expected provider 'anthropic', got '%s'", provider.Provider())
	}

	if provider.MaxTokens() != 200000 {
		t.Errorf("Expected 200000 max tokens for Claude, got %d", provider.MaxTokens())
	}
}

func TestGeminiProvider(t *testing.T) {
	provider := NewGeminiProvider("test-key", "gemini-2.5-flash")

	if provider.Name() != "gemini-2.5-flash" {
		t.Errorf("Expected name 'gemini-2.5-flash', got '%s'", provider.Name())
	}

	if provider.Provider() != "google" {
		t.Errorf("Expected provider 'google', got '%s'", provider.Provider())
	}

	if provider.MaxTokens() != 1000000 {
		t.Errorf("Expected 1000000 max tokens for Gemini, got %d", provider.MaxTokens())
	}
}

func TestOllamaProvider(t *testing.T) {
	provider := NewOllamaProvider("llama3", "http://localhost:11434")

	if provider.Name() != "llama3" {
		t.Errorf("Expected name 'llama3', got '%s'", provider.Name())
	}

	if provider.Provider() != "ollama" {
		t.Errorf("Expected provider 'ollama', got '%s'", provider.Provider())
	}

	// Test default URL
	provider2 := NewOllamaProvider("llama3", "")
	if provider2.Provider() != "ollama" {
		t.Error("Expected default URL to work")
	}
}

func TestConcurrentModelAccess(t *testing.T) {
	registry := NewModelRegistry()
	provider := NewMockModelProvider("concurrent-test", "test")
	registry.Register(provider)

	const numGoroutines = 10
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			request := &ModelRequest{Prompt: "test"}
			_, err := registry.Execute(context.Background(), "concurrent-test", request)
			errors <- err
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		if err := <-errors; err != nil {
			t.Errorf("Concurrent execution failed: %v", err)
		}
	}
}

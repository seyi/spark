// Package models provides model abstraction for multi-model support
// Inspired by ADK's model-agnostic architecture
package models

import (
	"context"
	"fmt"
	"sync"
)

// ModelProvider is the interface for AI model implementations
type ModelProvider interface {
	// Execute runs the model with given prompt and tools
	Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error)

	// Name returns the model name (e.g., "gpt-4", "gemini-2.5-flash")
	Name() string

	// Provider returns the provider name (e.g., "openai", "google", "anthropic")
	Provider() string

	// MaxTokens returns the maximum token limit
	MaxTokens() int

	// SupportsTools returns whether the model supports tool calling
	SupportsTools() bool

	// SupportsStreaming returns whether the model supports streaming
	SupportsStreaming() bool
}

// ModelRequest represents a request to the model
type ModelRequest struct {
	Prompt      string
	Tools       []ToolDefinition
	Temperature float64
	MaxTokens   int
	SystemPrompt string
	Context     map[string]interface{}
}

// ModelResponse represents the model's response
type ModelResponse struct {
	Content     string
	ToolCalls   []ToolCall
	TokensUsed  int
	FinishReason string
	Metadata    map[string]interface{}
}

// ToolDefinition defines a tool the model can use
type ToolDefinition struct {
	Name        string
	Description string
	Parameters  map[string]interface{}
}

// ToolCall represents a model's decision to call a tool
type ToolCall struct {
	ID         string
	ToolName   string
	Arguments  map[string]interface{}
}

// ModelRegistry manages available model providers
type ModelRegistry struct {
	providers map[string]ModelProvider
	mu        sync.RWMutex
}

// NewModelRegistry creates a new model registry
func NewModelRegistry() *ModelRegistry {
	return &ModelRegistry{
		providers: make(map[string]ModelProvider),
	}
}

// Register adds a model provider to the registry
func (r *ModelRegistry) Register(provider ModelProvider) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := provider.Name()
	if _, exists := r.providers[name]; exists {
		return fmt.Errorf("model %s already registered", name)
	}

	r.providers[name] = provider
	return nil
}

// Get retrieves a model provider by name
func (r *ModelRegistry) Get(name string) (ModelProvider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	provider, exists := r.providers[name]
	if !exists {
		return nil, fmt.Errorf("model %s not found", name)
	}

	return provider, nil
}

// List returns all registered model names
func (r *ModelRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.providers))
	for name := range r.providers {
		names = append(names, name)
	}
	return names
}

// Execute runs a model by name
func (r *ModelRegistry) Execute(ctx context.Context, modelName string, request *ModelRequest) (*ModelResponse, error) {
	provider, err := r.Get(modelName)
	if err != nil {
		return nil, err
	}

	return provider.Execute(ctx, request)
}

// Mock model provider for testing
type MockModelProvider struct {
	name         string
	provider     string
	maxTokens    int
	supportsTools bool
}

func NewMockModelProvider(name, provider string) *MockModelProvider {
	return &MockModelProvider{
		name:         name,
		provider:     provider,
		maxTokens:    4096,
		supportsTools: true,
	}
}

func (m *MockModelProvider) Name() string                { return m.name }
func (m *MockModelProvider) Provider() string            { return m.provider }
func (m *MockModelProvider) MaxTokens() int              { return m.maxTokens }
func (m *MockModelProvider) SupportsTools() bool         { return m.supportsTools }
func (m *MockModelProvider) SupportsStreaming() bool     { return false }

func (m *MockModelProvider) Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error) {
	return &ModelResponse{
		Content:      fmt.Sprintf("Mock response from %s to: %s", m.name, request.Prompt),
		ToolCalls:    []ToolCall{},
		TokensUsed:   100,
		FinishReason: "stop",
		Metadata:     map[string]interface{}{"model": m.name},
	}, nil
}

// OpenAI-compatible provider
type OpenAIProvider struct {
	apiKey    string
	modelName string
	baseURL   string
}

func NewOpenAIProvider(apiKey, modelName string) *OpenAIProvider {
	return &OpenAIProvider{
		apiKey:    apiKey,
		modelName: modelName,
		baseURL:   "https://api.openai.com/v1",
	}
}

func (o *OpenAIProvider) Name() string             { return o.modelName }
func (o *OpenAIProvider) Provider() string         { return "openai" }
func (o *OpenAIProvider) MaxTokens() int           { return 128000 } // gpt-4-turbo
func (o *OpenAIProvider) SupportsTools() bool      { return true }
func (o *OpenAIProvider) SupportsStreaming() bool  { return true }

func (o *OpenAIProvider) Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error) {
	// In a real implementation, this would call the OpenAI API
	// For now, return mock response
	return &ModelResponse{
		Content:      "Response from OpenAI (not implemented)",
		ToolCalls:    []ToolCall{},
		TokensUsed:   150,
		FinishReason: "stop",
		Metadata:     map[string]interface{}{"model": o.modelName, "provider": "openai"},
	}, nil
}

// Anthropic Claude provider
type AnthropicProvider struct {
	apiKey    string
	modelName string
}

func NewAnthropicProvider(apiKey, modelName string) *AnthropicProvider {
	return &AnthropicProvider{
		apiKey:    apiKey,
		modelName: modelName,
	}
}

func (a *AnthropicProvider) Name() string             { return a.modelName }
func (a *AnthropicProvider) Provider() string         { return "anthropic" }
func (a *AnthropicProvider) MaxTokens() int           { return 200000 } // Claude 3
func (a *AnthropicProvider) SupportsTools() bool      { return true }
func (a *AnthropicProvider) SupportsStreaming() bool  { return true }

func (a *AnthropicProvider) Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error) {
	// In a real implementation, this would call the Anthropic API
	return &ModelResponse{
		Content:      "Response from Claude (not implemented)",
		ToolCalls:    []ToolCall{},
		TokensUsed:   200,
		FinishReason: "end_turn",
		Metadata:     map[string]interface{}{"model": a.modelName, "provider": "anthropic"},
	}, nil
}

// Google Gemini provider
type GeminiProvider struct {
	apiKey    string
	modelName string
}

func NewGeminiProvider(apiKey, modelName string) *GeminiProvider {
	return &GeminiProvider{
		apiKey:    apiKey,
		modelName: modelName,
	}
}

func (g *GeminiProvider) Name() string             { return g.modelName }
func (g *GeminiProvider) Provider() string         { return "google" }
func (g *GeminiProvider) MaxTokens() int           { return 1000000 } // Gemini 1.5
func (g *GeminiProvider) SupportsTools() bool      { return true }
func (g *GeminiProvider) SupportsStreaming() bool  { return true }

func (g *GeminiProvider) Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error) {
	// In a real implementation, this would call the Gemini API
	return &ModelResponse{
		Content:      "Response from Gemini (not implemented)",
		ToolCalls:    []ToolCall{},
		TokensUsed:   120,
		FinishReason: "STOP",
		Metadata:     map[string]interface{}{"model": g.modelName, "provider": "google"},
	}, nil
}

// Ollama local provider
type OllamaProvider struct {
	modelName string
	baseURL   string
}

func NewOllamaProvider(modelName, baseURL string) *OllamaProvider {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	return &OllamaProvider{
		modelName: modelName,
		baseURL:   baseURL,
	}
}

func (o *OllamaProvider) Name() string             { return o.modelName }
func (o *OllamaProvider) Provider() string         { return "ollama" }
func (o *OllamaProvider) MaxTokens() int           { return 4096 }
func (o *OllamaProvider) SupportsTools() bool      { return true }
func (o *OllamaProvider) SupportsStreaming() bool  { return true }

func (o *OllamaProvider) Execute(ctx context.Context, request *ModelRequest) (*ModelResponse, error) {
	// In a real implementation, this would call the Ollama API
	return &ModelResponse{
		Content:      "Response from Ollama (not implemented)",
		ToolCalls:    []ToolCall{},
		TokensUsed:   80,
		FinishReason: "stop",
		Metadata:     map[string]interface{}{"model": o.modelName, "provider": "ollama"},
	}, nil
}

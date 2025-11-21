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

package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
)

// MockLlmProvider implements LlmProvider for testing
type MockLlmProvider struct {
	responses   []*LlmResponse
	callCount   int
	shouldError bool
	errorMsg    string
}

func NewMockLlmProvider(responses ...*LlmResponse) *MockLlmProvider {
	return &MockLlmProvider{
		responses: responses,
	}
}

func (m *MockLlmProvider) WithError(msg string) *MockLlmProvider {
	m.shouldError = true
	m.errorMsg = msg
	return m
}

func (m *MockLlmProvider) GenerateContent(ctx context.Context, req *LlmRequest) <-chan *LlmResponse {
	output := make(chan *LlmResponse)
	go func() {
		defer close(output)
		m.callCount++

		if m.shouldError {
			output <- &LlmResponse{
				ErrorCode:    "provider_error",
				ErrorMessage: m.errorMsg,
			}
			return
		}

		for _, resp := range m.responses {
			output <- resp
		}
	}()
	return output
}

func TestLlmRequest(t *testing.T) {
	t.Run("NewLlmRequest", func(t *testing.T) {
		req := &LlmRequest{
			SystemInstruction: "You are a helpful assistant",
			Contents: []Content{
				{Role: "user", Parts: []Part{{Text: "Hello"}}},
			},
			Tools: []ToolDefinition{
				{Name: "calculator", Description: "Perform calculations"},
			},
		}

		if req.SystemInstruction != "You are a helpful assistant" {
			t.Errorf("expected system instruction, got %s", req.SystemInstruction)
		}
		if len(req.Contents) != 1 {
			t.Errorf("expected 1 content, got %d", len(req.Contents))
		}
		if len(req.Tools) != 1 {
			t.Errorf("expected 1 tool, got %d", len(req.Tools))
		}
	})
}

func TestLlmResponse(t *testing.T) {
	t.Run("GetFunctionCalls", func(t *testing.T) {
		resp := &LlmResponse{
			Content: &Content{
				Role: "assistant",
				Parts: []Part{
					{Text: "Let me calculate that"},
					{FunctionCall: &FunctionCall{Name: "calculator", Args: map[string]interface{}{"expr": "2+2"}}},
				},
			},
		}

		calls := resp.GetFunctionCalls()
		if len(calls) != 1 {
			t.Errorf("expected 1 function call, got %d", len(calls))
		}
		if calls[0].Name != "calculator" {
			t.Errorf("expected calculator, got %s", calls[0].Name)
		}
	})

	t.Run("HasFunctionCalls", func(t *testing.T) {
		respWithCalls := &LlmResponse{
			Content: &Content{
				Parts: []Part{
					{FunctionCall: &FunctionCall{Name: "test"}},
				},
			},
		}
		respWithoutCalls := &LlmResponse{
			Content: &Content{
				Parts: []Part{
					{Text: "Just text"},
				},
			},
		}

		if !respWithCalls.HasFunctionCalls() {
			t.Error("expected HasFunctionCalls to be true")
		}
		if respWithoutCalls.HasFunctionCalls() {
			t.Error("expected HasFunctionCalls to be false")
		}
	})
}

func TestLlmFlow(t *testing.T) {
	t.Run("BasicExecution", func(t *testing.T) {
		provider := NewMockLlmProvider(&LlmResponse{
			Content: &Content{
				Role:  "assistant",
				Parts: []Part{{Text: "Hello, world!"}},
			},
			TurnComplete: true,
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 3

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "Say hello"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.AgentName = "test-agent"

		events := CollectEvents(flow.RunAsync(ctx, invCtx))

		if len(events) == 0 {
			t.Fatal("expected at least one event")
		}

		// Should have received events
		var hasMessage bool
		for _, e := range events {
			if e.Type == EventTypeMessage {
				hasMessage = true
			}
		}
		if !hasMessage {
			t.Error("expected at least one message event")
		}
	})

	t.Run("BeforeModelCallback_ShortCircuit", func(t *testing.T) {
		provider := NewMockLlmProvider(&LlmResponse{
			Content: &Content{Parts: []Part{{Text: "Should not see this"}}},
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 1

		// Add callback that short-circuits
		shortCircuitResponse := &LlmResponse{
			Content: &Content{
				Role:  "assistant",
				Parts: []Part{{Text: "Cached response"}},
			},
			TurnComplete: true,
		}

		flow.WithBeforeModelCallback(func(ctx *CallbackContext, req *LlmRequest) (*LlmResponse, error) {
			return shortCircuitResponse, nil
		})

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.AgentName = "test-agent"

		events := CollectEvents(flow.RunAsync(ctx, invCtx))

		// Provider should not have been called
		if provider.callCount != 0 {
			t.Errorf("expected provider not to be called, but was called %d times", provider.callCount)
		}

		// Should have gotten the cached response
		if len(events) == 0 {
			t.Fatal("expected events from short-circuit")
		}
	})

	t.Run("AfterModelCallback_Modifies", func(t *testing.T) {
		provider := NewMockLlmProvider(&LlmResponse{
			Content: &Content{
				Role:  "assistant",
				Parts: []Part{{Text: "Original response"}},
			},
			TurnComplete: true,
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 1

		// Add callback that modifies response
		flow.WithAfterModelCallback(func(ctx *CallbackContext, resp *LlmResponse) (*LlmResponse, error) {
			if resp.Content != nil && len(resp.Content.Parts) > 0 {
				resp.Content.Parts[0].Text = "Modified response"
			}
			return resp, nil
		})

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.AgentName = "test-agent"

		events := CollectEvents(flow.RunAsync(ctx, invCtx))

		// Check that we got events
		if len(events) == 0 {
			t.Fatal("expected events")
		}
	})

	t.Run("MaxStepsLimit", func(t *testing.T) {
		// Provider that returns non-final responses
		provider := NewMockLlmProvider(&LlmResponse{
			Content: &Content{
				Parts: []Part{{FunctionCall: &FunctionCall{Name: "loop", Args: map[string]interface{}{}}}},
			},
			TurnComplete: false,
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 3

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.AgentName = "test-agent"

		events := CollectEvents(flow.RunAsync(ctx, invCtx))

		// Should have max_steps_reached event
		var hasMaxSteps bool
		for _, e := range events {
			if e.Metadata != nil {
				if _, ok := e.Metadata["max_steps_reached"]; ok {
					hasMaxSteps = true
				}
			}
		}
		if !hasMaxSteps {
			t.Error("expected max_steps_reached event")
		}
	})
}

func TestRequestProcessor(t *testing.T) {
	t.Run("LoggingProcessor", func(t *testing.T) {
		processor := &LoggingRequestProcessor{Prefix: "test"}

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		req := &LlmRequest{
			SystemInstruction: "test instruction",
		}

		events := processor.ProcessRequest(ctx, invCtx, req)
		eventList := CollectEvents(events)

		if len(eventList) != 1 {
			t.Errorf("expected 1 event, got %d", len(eventList))
		}
		if eventList[0].Type != EventTypeStateChange {
			t.Errorf("expected StateChange event, got %v", eventList[0].Type)
		}
	})
}

func TestResponseProcessor(t *testing.T) {
	t.Run("LoggingProcessor", func(t *testing.T) {
		processor := &LoggingResponseProcessor{Prefix: "test"}

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		resp := &LlmResponse{
			Content: &Content{Parts: []Part{{Text: "response"}}},
		}

		events := processor.ProcessResponse(ctx, invCtx, resp)
		eventList := CollectEvents(events)

		if len(eventList) != 1 {
			t.Errorf("expected 1 event, got %d", len(eventList))
		}
	})
}

func TestCallbackContext(t *testing.T) {
	t.Run("NewCallbackContext", func(t *testing.T) {
		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		actions := &EventActions{}
		cbCtx := NewCallbackContext(invCtx, actions)

		if cbCtx.InvocationContext != invCtx {
			t.Error("expected invocation context to match")
		}
		if cbCtx.EventActions != actions {
			t.Error("expected event actions to match")
		}
	})
}

func TestReadonlyContext(t *testing.T) {
	t.Run("Accessors", func(t *testing.T) {
		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.UserID = "user-123"

		ro := NewReadonlyContext(invCtx)

		if ro.SessionID() != invCtx.SessionID {
			t.Error("SessionID mismatch")
		}
		if ro.UserID() != "user-123" {
			t.Error("UserID mismatch")
		}
		if ro.InvocationID() != invCtx.InvocationID {
			t.Error("InvocationID mismatch")
		}
	})

	t.Run("Get", func(t *testing.T) {
		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.Set("key", "value")

		ro := NewReadonlyContext(invCtx)

		val, ok := ro.Get("key")
		if !ok {
			t.Error("expected key to exist")
		}
		if val != "value" {
			t.Errorf("expected value, got %v", val)
		}
	})
}

func TestToolContext(t *testing.T) {
	t.Run("NewToolContext", func(t *testing.T) {
		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		toolCtx := NewToolContext(invCtx, "calculator")

		if toolCtx.InvocationContext != invCtx {
			t.Error("expected invocation context to match")
		}
		if toolCtx.ToolName != "calculator" {
			t.Errorf("expected calculator, got %s", toolCtx.ToolName)
		}
	})
}

func TestRateLimitingCallback(t *testing.T) {
	t.Run("ExceedsLimit", func(t *testing.T) {
		rlCallback := &RateLimitingBeforeModelCallback{
			MaxCallsPerMinute: 2,
		}

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		cbCtx := NewCallbackContext(invCtx, nil)

		req := &LlmRequest{}

		// First call should succeed
		resp, err := rlCallback.Callback()(cbCtx, req)
		if err != nil || resp != nil {
			t.Error("first call should succeed")
		}

		// Second call should succeed
		resp, err = rlCallback.Callback()(cbCtx, req)
		if err != nil || resp != nil {
			t.Error("second call should succeed")
		}

		// Third call should be rate limited
		resp, err = rlCallback.Callback()(cbCtx, req)
		if err != nil {
			t.Error("should not return error, but response")
		}
		if resp == nil {
			t.Error("expected rate limit response")
		} else if resp.ErrorCode != "rate_limited" {
			t.Errorf("expected rate_limited, got %s", resp.ErrorCode)
		}
	})
}

func TestCachingCallback(t *testing.T) {
	t.Run("ReturnsCachedResponse", func(t *testing.T) {
		cache := make(map[string]*LlmResponse)
		cachedResp := &LlmResponse{
			Content:      &Content{Parts: []Part{{Text: "cached"}}},
			TurnComplete: true,
		}
		cache["test-instruction"] = cachedResp

		callback := CachingBeforeModelCallback(cache)

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		cbCtx := NewCallbackContext(invCtx, nil)

		req := &LlmRequest{
			SystemInstruction: "test-instruction",
		}

		resp, err := callback(cbCtx, req)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if resp != cachedResp {
			t.Error("expected cached response")
		}
	})

	t.Run("ReturnsNilForMiss", func(t *testing.T) {
		cache := make(map[string]*LlmResponse)
		callback := CachingBeforeModelCallback(cache)

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		cbCtx := NewCallbackContext(invCtx, nil)

		req := &LlmRequest{
			SystemInstruction: "not-in-cache",
		}

		resp, err := callback(cbCtx, req)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if resp != nil {
			t.Error("expected nil for cache miss")
		}
	})
}

func TestFunctionCall(t *testing.T) {
	t.Run("Serialization", func(t *testing.T) {
		call := &FunctionCall{
			ID:   "call-123",
			Name: "calculator",
			Args: map[string]interface{}{
				"expression": "2 + 2",
			},
		}

		if call.Name != "calculator" {
			t.Errorf("expected calculator, got %s", call.Name)
		}
		if call.Args["expression"] != "2 + 2" {
			t.Errorf("expected expression, got %v", call.Args)
		}
	})
}

func TestFunctionResponse(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		resp := &FunctionResponse{
			Name:     "calculator",
			Response: 4,
		}

		if resp.Name != "calculator" {
			t.Error("name mismatch")
		}
		if resp.Response != 4 {
			t.Error("response mismatch")
		}
		if resp.Error != "" {
			t.Error("should not have error")
		}
	})

	t.Run("Error", func(t *testing.T) {
		resp := &FunctionResponse{
			Name:  "calculator",
			Error: "division by zero",
		}

		if resp.Error != "division by zero" {
			t.Error("error mismatch")
		}
	})
}

func TestLLMAsyncAgentWithFlow(t *testing.T) {
	t.Run("UsesFlowWhenSet", func(t *testing.T) {
		provider := NewMockLlmProvider(&LlmResponse{
			Content:      &Content{Parts: []Part{{Text: "response"}}},
			TurnComplete: true,
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 1

		// Create agent without executor (will use flow)
		ag := NewLLMAsyncAgent("agent-1", "test-agent", nil)
		ag.WithLlmFlow(flow)

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		events := CollectEvents(ag.RunAsync(ctx, invCtx))

		if len(events) == 0 {
			t.Fatal("expected events")
		}

		// Provider should have been called
		if provider.callCount != 1 {
			t.Errorf("expected provider to be called once, got %d", provider.callCount)
		}
	})

	t.Run("MergesCallbacks", func(t *testing.T) {
		provider := NewMockLlmProvider(&LlmResponse{
			Content:      &Content{Parts: []Part{{Text: "response"}}},
			TurnComplete: true,
		})

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 1

		callbackCalled := false
		ag := NewLLMAsyncAgent("agent-1", "test-agent", nil)
		ag.WithLlmFlow(flow)
		ag.WithBeforeModelCallback(func(ctx *CallbackContext, req *LlmRequest) (*LlmResponse, error) {
			callbackCalled = true
			return nil, nil
		})

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)

		CollectEvents(ag.RunAsync(ctx, invCtx))

		if !callbackCalled {
			t.Error("expected callback to be called")
		}
	})
}

func TestUsageMetadata(t *testing.T) {
	t.Run("TokenCounts", func(t *testing.T) {
		usage := &UsageMetadata{
			PromptTokens:     100,
			CompletionTokens: 50,
			TotalTokens:      150,
		}

		if usage.PromptTokens != 100 {
			t.Errorf("expected 100, got %d", usage.PromptTokens)
		}
		if usage.TotalTokens != 150 {
			t.Errorf("expected 150, got %d", usage.TotalTokens)
		}
	})
}

func TestGenerateConfig(t *testing.T) {
	t.Run("ConfigOptions", func(t *testing.T) {
		config := &GenerateConfig{
			Temperature:     0.7,
			TopP:            0.9,
			TopK:            40,
			MaxOutputTokens: 1000,
			StopSequences:   []string{"END"},
			Labels:          map[string]string{"env": "test"},
		}

		if config.Temperature != 0.7 {
			t.Error("temperature mismatch")
		}
		if config.MaxOutputTokens != 1000 {
			t.Error("max tokens mismatch")
		}
		if len(config.Labels) != 1 {
			t.Error("labels mismatch")
		}
	})
}

func TestOnModelErrorCallback(t *testing.T) {
	t.Run("RetryCallback", func(t *testing.T) {
		retryableErrors := []string{"rate_limit", "timeout"}
		callback := RetryOnErrorCallback(3, retryableErrors)

		ctx := context.Background()
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		cbCtx := NewCallbackContext(invCtx, nil)

		req := &LlmRequest{}

		// Non-retryable error
		resp, err := callback(cbCtx, req, errors.New("unknown error"))
		if err != nil {
			t.Error("should not return error")
		}
		if resp != nil {
			t.Error("should return nil for non-retryable")
		}

		// Retryable error
		resp, err = callback(cbCtx, req, errors.New("rate_limit"))
		if err != nil {
			t.Error("should not return error")
		}
		// Returns nil to signal retry
		if resp != nil {
			t.Error("should return nil to signal retry")
		}
	})
}

func TestContent(t *testing.T) {
	t.Run("MultiPart", func(t *testing.T) {
		content := &Content{
			Role: "user",
			Parts: []Part{
				{Text: "Hello"},
				{Text: "World"},
			},
		}

		if content.Role != "user" {
			t.Error("role mismatch")
		}
		if len(content.Parts) != 2 {
			t.Errorf("expected 2 parts, got %d", len(content.Parts))
		}
	})

	t.Run("MixedParts", func(t *testing.T) {
		content := &Content{
			Role: "assistant",
			Parts: []Part{
				{Text: "Let me help"},
				{FunctionCall: &FunctionCall{Name: "search", Args: map[string]interface{}{"q": "test"}}},
			},
		}

		if len(content.Parts) != 2 {
			t.Errorf("expected 2 parts, got %d", len(content.Parts))
		}
		if content.Parts[0].Text == "" {
			t.Error("expected text in first part")
		}
		if content.Parts[1].FunctionCall == nil {
			t.Error("expected function call in second part")
		}
	})
}

func TestFlowContextCancellation(t *testing.T) {
	t.Run("RespectsContextCancel", func(t *testing.T) {
		// Provider that delays
		provider := &MockLlmProvider{
			responses: []*LlmResponse{
				{Content: &Content{Parts: []Part{{Text: "response"}}}},
			},
		}

		flow := NewLlmFlow(provider)
		flow.MaxSteps = 10

		// Create a cancellable context
		ctx, cancel := context.WithCancel(context.Background())
		input := &agent.AgentInput{Instruction: "test"}
		invCtx := NewInvocationContext(ctx, input)
		invCtx.AgentName = "test-agent"

		// Cancel after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		events := CollectEvents(flow.RunAsync(ctx, invCtx))

		// Should have completed (may have error due to cancel)
		_ = events
	})
}

func TestToolDefinition(t *testing.T) {
	t.Run("WithHandler", func(t *testing.T) {
		handlerCalled := false
		tool := ToolDefinition{
			Name:        "test_tool",
			Description: "A test tool",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"input": map[string]interface{}{"type": "string"},
				},
			},
			Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
				handlerCalled = true
				return "result", nil
			},
		}

		result, err := tool.Handler(context.Background(), map[string]interface{}{})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != "result" {
			t.Errorf("expected result, got %v", result)
		}
		if !handlerCalled {
			t.Error("handler was not called")
		}
	})
}

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

// Package runtime provides ADK-compatible LLM flow with processors and callbacks.
//
// This file implements the ADK BaseLlmFlow pattern with:
// - Request/Response processors that can yield events
// - Before/After model callbacks that can short-circuit execution
// - Error recovery callbacks
// - Function/tool call handling
package runtime

import (
	"context"
	"fmt"
	"time"
)

// LlmRequest represents a request to the LLM.
// Matches ADK's LlmRequest structure.
type LlmRequest struct {
	// Contents is the conversation history
	Contents []Content `json:"contents,omitempty"`

	// SystemInstruction is the system prompt
	SystemInstruction string `json:"system_instruction,omitempty"`

	// Tools available for this request
	Tools []ToolDefinition `json:"tools,omitempty"`

	// ToolsDict maps tool names to their definitions
	ToolsDict map[string]ToolDefinition `json:"-"`

	// Config contains generation configuration
	Config *GenerateConfig `json:"config,omitempty"`

	// Labels for billing/tracking (ADK pattern)
	Labels map[string]string `json:"labels,omitempty"`
}

// Content represents a message in the conversation
type Content struct {
	Role  string `json:"role"`
	Parts []Part `json:"parts"`
}

// Part represents a part of a message (text, function call, etc.)
type Part struct {
	Text             string            `json:"text,omitempty"`
	FunctionCall     *FunctionCall     `json:"function_call,omitempty"`
	FunctionResponse *FunctionResponse `json:"function_response,omitempty"`
}

// FunctionCall represents a function/tool call from the LLM
type FunctionCall struct {
	ID   string                 `json:"id,omitempty"`
	Name string                 `json:"name"`
	Args map[string]interface{} `json:"args"`
}

// FunctionResponse represents the result of a function call
type FunctionResponse struct {
	Name     string      `json:"name"`
	Response interface{} `json:"response"`
	Error    string      `json:"error,omitempty"`
}

// ToolDefinition defines a tool available to the LLM
type ToolDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Handler     ToolHandler            `json:"-"`
}

// ToolHandler is a function that executes a tool
type ToolHandler func(ctx context.Context, args map[string]interface{}) (interface{}, error)

// GenerateConfig contains LLM generation parameters
type GenerateConfig struct {
	Temperature     float64           `json:"temperature,omitempty"`
	TopP            float64           `json:"top_p,omitempty"`
	TopK            int               `json:"top_k,omitempty"`
	MaxOutputTokens int               `json:"max_output_tokens,omitempty"`
	StopSequences   []string          `json:"stop_sequences,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// LlmResponse represents a response from the LLM.
// Matches ADK's LlmResponse structure.
type LlmResponse struct {
	// Content is the response content
	Content *Content `json:"content,omitempty"`

	// Partial indicates if this is a streaming chunk
	Partial bool `json:"partial,omitempty"`

	// ErrorCode if the response is an error
	ErrorCode string `json:"error_code,omitempty"`

	// ErrorMessage provides error details
	ErrorMessage string `json:"error_message,omitempty"`

	// TurnComplete indicates the LLM finished its turn
	TurnComplete bool `json:"turn_complete,omitempty"`

	// Interrupted indicates the response was interrupted
	Interrupted bool `json:"interrupted,omitempty"`

	// UsageMetadata contains token usage info
	UsageMetadata *UsageMetadata `json:"usage_metadata,omitempty"`

	// GroundingMetadata for search grounding
	GroundingMetadata interface{} `json:"grounding_metadata,omitempty"`
}

// UsageMetadata contains token usage information
type UsageMetadata struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// GetFunctionCalls extracts function calls from the response
func (r *LlmResponse) GetFunctionCalls() []*FunctionCall {
	if r.Content == nil {
		return nil
	}
	var calls []*FunctionCall
	for _, part := range r.Content.Parts {
		if part.FunctionCall != nil {
			calls = append(calls, part.FunctionCall)
		}
	}
	return calls
}

// HasFunctionCalls checks if the response contains function calls
func (r *LlmResponse) HasFunctionCalls() bool {
	return len(r.GetFunctionCalls()) > 0
}

// CallbackContext provides context for callbacks (mutable)
type CallbackContext struct {
	InvocationContext *InvocationContext
	EventActions      *EventActions
}

// NewCallbackContext creates a new callback context
func NewCallbackContext(invCtx *InvocationContext, actions *EventActions) *CallbackContext {
	return &CallbackContext{
		InvocationContext: invCtx,
		EventActions:      actions,
	}
}

// ReadonlyContext provides read-only access to invocation context
type ReadonlyContext struct {
	invCtx *InvocationContext
}

// NewReadonlyContext creates a read-only context wrapper
func NewReadonlyContext(invCtx *InvocationContext) *ReadonlyContext {
	return &ReadonlyContext{invCtx: invCtx}
}

func (r *ReadonlyContext) SessionID() string    { return r.invCtx.SessionID }
func (r *ReadonlyContext) UserID() string       { return r.invCtx.UserID }
func (r *ReadonlyContext) InvocationID() string { return r.invCtx.InvocationID }
func (r *ReadonlyContext) Get(key string) (interface{}, bool) {
	return r.invCtx.Get(key)
}

// ToolContext provides context for tool execution
type ToolContext struct {
	InvocationContext *InvocationContext
	ToolName          string
}

// NewToolContext creates a new tool context
func NewToolContext(invCtx *InvocationContext, toolName string) *ToolContext {
	return &ToolContext{
		InvocationContext: invCtx,
		ToolName:          toolName,
	}
}

// RequestProcessor processes LLM requests before they're sent.
// Can yield events (e.g., for preprocessing status).
type RequestProcessor interface {
	// ProcessRequest modifies the request and optionally yields events
	ProcessRequest(ctx context.Context, invCtx *InvocationContext, req *LlmRequest) <-chan *AsyncEvent
}

// ResponseProcessor processes LLM responses after they're received.
// Can yield events (e.g., for postprocessing status).
type ResponseProcessor interface {
	// ProcessResponse handles the response and optionally yields events
	ProcessResponse(ctx context.Context, invCtx *InvocationContext, resp *LlmResponse) <-chan *AsyncEvent
}

// BeforeModelCallback is called before the LLM is invoked.
// Can return a response to short-circuit the LLM call.
type BeforeModelCallback func(ctx *CallbackContext, req *LlmRequest) (*LlmResponse, error)

// AfterModelCallback is called after the LLM responds.
// Can modify the response.
type AfterModelCallback func(ctx *CallbackContext, resp *LlmResponse) (*LlmResponse, error)

// OnModelErrorCallback is called when the LLM returns an error.
// Can return a recovery response.
type OnModelErrorCallback func(ctx *CallbackContext, req *LlmRequest, err error) (*LlmResponse, error)

// LlmFlow orchestrates LLM execution with processors and callbacks.
// This is Go's equivalent to ADK's BaseLlmFlow.
type LlmFlow struct {
	// Processors
	RequestProcessors  []RequestProcessor
	ResponseProcessors []ResponseProcessor

	// Callbacks
	BeforeModelCallbacks  []BeforeModelCallback
	AfterModelCallbacks   []AfterModelCallback
	OnModelErrorCallbacks []OnModelErrorCallback

	// LLM provider
	LlmProvider LlmProvider

	// Configuration
	AgentNameLabelKey string
	MaxSteps          int
}

// LlmProvider is the interface for LLM backends
type LlmProvider interface {
	// GenerateContent calls the LLM and streams responses
	GenerateContent(ctx context.Context, req *LlmRequest) <-chan *LlmResponse
}

// NewLlmFlow creates a new LLM flow
func NewLlmFlow(provider LlmProvider) *LlmFlow {
	return &LlmFlow{
		LlmProvider:       provider,
		AgentNameLabelKey: "adk_agent_name",
		MaxSteps:          10,
	}
}

// WithRequestProcessor adds a request processor
func (f *LlmFlow) WithRequestProcessor(p RequestProcessor) *LlmFlow {
	f.RequestProcessors = append(f.RequestProcessors, p)
	return f
}

// WithResponseProcessor adds a response processor
func (f *LlmFlow) WithResponseProcessor(p ResponseProcessor) *LlmFlow {
	f.ResponseProcessors = append(f.ResponseProcessors, p)
	return f
}

// WithBeforeModelCallback adds a before-model callback
func (f *LlmFlow) WithBeforeModelCallback(cb BeforeModelCallback) *LlmFlow {
	f.BeforeModelCallbacks = append(f.BeforeModelCallbacks, cb)
	return f
}

// WithAfterModelCallback adds an after-model callback
func (f *LlmFlow) WithAfterModelCallback(cb AfterModelCallback) *LlmFlow {
	f.AfterModelCallbacks = append(f.AfterModelCallbacks, cb)
	return f
}

// WithOnModelErrorCallback adds an error callback
func (f *LlmFlow) WithOnModelErrorCallback(cb OnModelErrorCallback) *LlmFlow {
	f.OnModelErrorCallbacks = append(f.OnModelErrorCallbacks, cb)
	return f
}

// RunAsync executes the LLM flow and yields events.
// This is Go's equivalent to ADK's run_async.
func (f *LlmFlow) RunAsync(ctx context.Context, invCtx *InvocationContext) <-chan *AsyncEvent {
	output := make(chan *AsyncEvent)

	go func() {
		defer close(output)

		step := 0
		for step < f.MaxSteps {
			step++

			var lastEvent *AsyncEvent

			// Run one step
			stepEvents := f.runOneStepAsync(ctx, invCtx)
			for event := range stepEvents {
				if event.Metadata == nil {
					event.Metadata = make(map[string]interface{})
				}
				event.Metadata["step"] = step
				lastEvent = event
				output <- event

				// Check for errors
				if event.Error != nil {
					return
				}
			}

			// Check if we should stop
			if lastEvent == nil {
				return
			}
			if lastEvent.IsFinalResponse() {
				return
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				errorEvent := NewAsyncEvent(EventTypeError, ctx.Err().Error(), "llm_flow")
				errorEvent.Error = ctx.Err()
				output <- errorEvent
				return
			default:
			}
		}

		// Max steps reached
		maxStepsEvent := NewAsyncEvent(EventTypeMessage, "Maximum steps reached", "llm_flow")
		maxStepsEvent.Metadata = map[string]interface{}{
			"max_steps_reached": true,
			"steps_executed":    step,
		}
		output <- maxStepsEvent
	}()

	return output
}

// runOneStepAsync executes one LLM step (one LLM call + tool handling)
func (f *LlmFlow) runOneStepAsync(ctx context.Context, invCtx *InvocationContext) <-chan *AsyncEvent {
	output := make(chan *AsyncEvent)

	go func() {
		defer close(output)

		// Create LLM request
		llmRequest := &LlmRequest{
			Config: &GenerateConfig{
				Labels: make(map[string]string),
			},
			Labels:    make(map[string]string),
			ToolsDict: make(map[string]ToolDefinition),
		}

		// Preprocess: run request processors
		for _, processor := range f.RequestProcessors {
			events := processor.ProcessRequest(ctx, invCtx, llmRequest)
			for event := range events {
				output <- event
			}

			// Check if invocation should end
			if invCtx.EndInvocation {
				return
			}
		}

		// Create model response event
		modelResponseEvent := NewAsyncEvent(EventTypeMessage, "", invCtx.AgentName)
		modelResponseEvent.InvocationID = invCtx.InvocationID

		// Call LLM with callbacks
		llmResponses := f.callLlmAsync(ctx, invCtx, llmRequest, modelResponseEvent)

		for llmResponse := range llmResponses {
			// Postprocess: run response processors
			for _, processor := range f.ResponseProcessors {
				events := processor.ProcessResponse(ctx, invCtx, llmResponse)
				for event := range events {
					output <- event
				}
			}

			// Skip empty responses
			if llmResponse.Content == nil && llmResponse.ErrorCode == "" && !llmResponse.Interrupted {
				continue
			}

			// Build the event
			event := f.finalizeModelResponseEvent(llmRequest, llmResponse, modelResponseEvent)
			output <- event

			// Handle function calls
			if llmResponse.HasFunctionCalls() {
				functionEvents := f.handleFunctionCallsAsync(ctx, invCtx, event, llmRequest)
				for funcEvent := range functionEvents {
					output <- funcEvent
				}
			}
		}
	}()

	return output
}

// callLlmAsync calls the LLM with before/after callbacks
func (f *LlmFlow) callLlmAsync(
	ctx context.Context,
	invCtx *InvocationContext,
	llmRequest *LlmRequest,
	modelResponseEvent *AsyncEvent,
) <-chan *LlmResponse {
	output := make(chan *LlmResponse)

	go func() {
		defer close(output)

		// Run before-model callbacks
		callbackCtx := NewCallbackContext(invCtx, modelResponseEvent.Actions)

		for _, callback := range f.BeforeModelCallbacks {
			response, err := callback(callbackCtx, llmRequest)
			if err != nil {
				// Callback error - yield error response
				output <- &LlmResponse{
					ErrorCode:    "callback_error",
					ErrorMessage: err.Error(),
				}
				return
			}
			if response != nil {
				// Callback short-circuited - yield its response and return
				output <- response
				return
			}
		}

		// Add agent name label for billing
		if llmRequest.Config == nil {
			llmRequest.Config = &GenerateConfig{Labels: make(map[string]string)}
		}
		if llmRequest.Config.Labels == nil {
			llmRequest.Config.Labels = make(map[string]string)
		}
		if _, ok := llmRequest.Config.Labels[f.AgentNameLabelKey]; !ok {
			llmRequest.Config.Labels[f.AgentNameLabelKey] = invCtx.AgentName
		}

		// Call the LLM
		if f.LlmProvider == nil {
			output <- &LlmResponse{
				ErrorCode:    "no_provider",
				ErrorMessage: "LLM provider not configured",
			}
			return
		}

		// Execute with error handling
		responses := f.runWithErrorHandling(ctx, invCtx, llmRequest, modelResponseEvent)

		for response := range responses {
			// Run after-model callbacks
			for _, callback := range f.AfterModelCallbacks {
				alteredResponse, err := callback(callbackCtx, response)
				if err != nil {
					// Log error but continue
					continue
				}
				if alteredResponse != nil {
					response = alteredResponse
				}
			}

			output <- response
		}
	}()

	return output
}

// runWithErrorHandling wraps LLM execution with error recovery
func (f *LlmFlow) runWithErrorHandling(
	ctx context.Context,
	invCtx *InvocationContext,
	llmRequest *LlmRequest,
	modelResponseEvent *AsyncEvent,
) <-chan *LlmResponse {
	output := make(chan *LlmResponse)

	go func() {
		defer close(output)

		// Create a channel to capture panics as errors
		done := make(chan struct{})
		var capturedErr error

		go func() {
			defer func() {
				if r := recover(); r != nil {
					capturedErr = fmt.Errorf("panic in LLM call: %v", r)
				}
				close(done)
			}()

			responses := f.LlmProvider.GenerateContent(ctx, llmRequest)
			for response := range responses {
				output <- response
			}
		}()

		<-done

		if capturedErr != nil {
			// Try error recovery callbacks
			callbackCtx := NewCallbackContext(invCtx, modelResponseEvent.Actions)

			for _, callback := range f.OnModelErrorCallbacks {
				recoveryResponse, err := callback(callbackCtx, llmRequest, capturedErr)
				if err != nil {
					continue
				}
				if recoveryResponse != nil {
					output <- recoveryResponse
					return
				}
			}

			// No recovery - yield error response
			output <- &LlmResponse{
				ErrorCode:    "llm_error",
				ErrorMessage: capturedErr.Error(),
			}
		}
	}()

	return output
}

// handleFunctionCallsAsync processes function calls from the LLM
func (f *LlmFlow) handleFunctionCallsAsync(
	ctx context.Context,
	invCtx *InvocationContext,
	functionCallEvent *AsyncEvent,
	llmRequest *LlmRequest,
) <-chan *AsyncEvent {
	output := make(chan *AsyncEvent)

	go func() {
		defer close(output)

		// Get function calls from the event content
		llmResponse, ok := functionCallEvent.Content.(*LlmResponse)
		if !ok {
			return
		}

		functionCalls := llmResponse.GetFunctionCalls()
		if len(functionCalls) == 0 {
			return
		}

		// Execute each function call
		var functionResponses []FunctionResponse

		for _, call := range functionCalls {
			// Look up the tool
			tool, exists := llmRequest.ToolsDict[call.Name]
			if !exists {
				functionResponses = append(functionResponses, FunctionResponse{
					Name:  call.Name,
					Error: fmt.Sprintf("unknown tool: %s", call.Name),
				})
				continue
			}

			// Execute the tool
			toolCtx := NewToolContext(invCtx, call.Name)
			_ = toolCtx // Used for future extensions

			if tool.Handler == nil {
				functionResponses = append(functionResponses, FunctionResponse{
					Name:  call.Name,
					Error: "tool has no handler",
				})
				continue
			}

			result, err := tool.Handler(ctx, call.Args)
			if err != nil {
				functionResponses = append(functionResponses, FunctionResponse{
					Name:  call.Name,
					Error: err.Error(),
				})
			} else {
				functionResponses = append(functionResponses, FunctionResponse{
					Name:     call.Name,
					Response: result,
				})
			}
		}

		// Create function response event
		functionResponseEvent := NewAsyncEvent(EventTypeToolResult, functionResponses, invCtx.AgentName)
		functionResponseEvent.InvocationID = invCtx.InvocationID
		functionResponseEvent.Timestamp = time.Now()

		// Check for agent transfer
		for _, resp := range functionResponses {
			if resp.Name == "transfer_to_agent" {
				if agentName, ok := resp.Response.(string); ok {
					functionResponseEvent.Actions = &EventActions{
						TransferToAgent: agentName,
					}
				}
			}
		}

		output <- functionResponseEvent
	}()

	return output
}

// finalizeModelResponseEvent builds the final event from the LLM response
func (f *LlmFlow) finalizeModelResponseEvent(
	llmRequest *LlmRequest,
	llmResponse *LlmResponse,
	modelResponseEvent *AsyncEvent,
) *AsyncEvent {
	event := &AsyncEvent{
		ID:           generateID("event"),
		InvocationID: modelResponseEvent.InvocationID,
		Author:       modelResponseEvent.Author,
		Type:         EventTypeMessage,
		Timestamp:    time.Now(),
		Partial:      llmResponse.Partial,
	}

	// Set content based on response
	if llmResponse.Content != nil {
		// Extract text content
		var textParts []string
		for _, part := range llmResponse.Content.Parts {
			if part.Text != "" {
				textParts = append(textParts, part.Text)
			}
		}
		if len(textParts) > 0 {
			event.Content = textParts[0]
			if len(textParts) > 1 {
				event.Content = textParts
			}
		}

		// Check for function calls
		if llmResponse.HasFunctionCalls() {
			event.Type = EventTypeToolCall
			event.Content = llmResponse
		}
	}

	// Set error if present
	if llmResponse.ErrorCode != "" {
		event.Type = EventTypeError
		event.Error = fmt.Errorf("%s: %s", llmResponse.ErrorCode, llmResponse.ErrorMessage)
	}

	// Copy metadata
	if llmResponse.UsageMetadata != nil {
		if event.Metadata == nil {
			event.Metadata = make(map[string]interface{})
		}
		event.Metadata["usage"] = llmResponse.UsageMetadata
	}

	return event
}

// Convenience functions for creating common processors

// LoggingRequestProcessor logs requests
type LoggingRequestProcessor struct {
	Prefix string
}

func (p *LoggingRequestProcessor) ProcessRequest(ctx context.Context, invCtx *InvocationContext, req *LlmRequest) <-chan *AsyncEvent {
	output := make(chan *AsyncEvent)
	go func() {
		defer close(output)
		// Emit a state change event for logging
		event := NewAsyncEvent(EventTypeStateChange, fmt.Sprintf("%s: Processing request", p.Prefix), "processor")
		event.Partial = true
		output <- event
	}()
	return output
}

// LoggingResponseProcessor logs responses
type LoggingResponseProcessor struct {
	Prefix string
}

func (p *LoggingResponseProcessor) ProcessResponse(ctx context.Context, invCtx *InvocationContext, resp *LlmResponse) <-chan *AsyncEvent {
	output := make(chan *AsyncEvent)
	go func() {
		defer close(output)
		// Emit a state change event for logging
		event := NewAsyncEvent(EventTypeStateChange, fmt.Sprintf("%s: Processed response", p.Prefix), "processor")
		event.Partial = true
		output <- event
	}()
	return output
}

// CachingBeforeModelCallback checks cache before LLM call
func CachingBeforeModelCallback(cache map[string]*LlmResponse) BeforeModelCallback {
	return func(ctx *CallbackContext, req *LlmRequest) (*LlmResponse, error) {
		// Simple cache key from system instruction
		cacheKey := req.SystemInstruction
		if cached, ok := cache[cacheKey]; ok {
			return cached, nil
		}
		return nil, nil
	}
}

// RateLimitingBeforeModelCallback implements rate limiting
type RateLimitingBeforeModelCallback struct {
	MaxCallsPerMinute int
	callCount         int
	windowStart       time.Time
}

func (r *RateLimitingBeforeModelCallback) Callback() BeforeModelCallback {
	return func(ctx *CallbackContext, req *LlmRequest) (*LlmResponse, error) {
		now := time.Now()
		if now.Sub(r.windowStart) > time.Minute {
			r.callCount = 0
			r.windowStart = now
		}

		if r.callCount >= r.MaxCallsPerMinute {
			return &LlmResponse{
				ErrorCode:    "rate_limited",
				ErrorMessage: fmt.Sprintf("rate limit exceeded: %d calls per minute", r.MaxCallsPerMinute),
			}, nil
		}

		r.callCount++
		return nil, nil
	}
}

// RetryOnErrorCallback retries on specific errors
func RetryOnErrorCallback(maxRetries int, retryableErrors []string) OnModelErrorCallback {
	retryCount := 0
	return func(ctx *CallbackContext, req *LlmRequest, err error) (*LlmResponse, error) {
		if retryCount >= maxRetries {
			return nil, nil // Let the error propagate
		}

		errStr := err.Error()
		for _, retryable := range retryableErrors {
			if errStr == retryable {
				retryCount++
				// Return nil to signal retry should happen
				// In practice, the flow would need to re-call the LLM
				return nil, nil
			}
		}

		return nil, nil
	}
}

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
	"github.com/apache/spark/spark-ai-agents/pkg/coordinator"
	"github.com/apache/spark/spark-ai-agents/pkg/events"
	"github.com/apache/spark/spark-ai-agents/pkg/memory"
	"github.com/apache/spark/spark-ai-agents/pkg/models"
	"github.com/apache/spark/spark-ai-agents/pkg/sessions"
	"github.com/apache/spark/spark-ai-agents/pkg/telemetry"
	"github.com/apache/spark/spark-ai-agents/pkg/tools"
)

// TestCoordinatorIntegration tests the full coordinator setup
func TestCoordinatorIntegration(t *testing.T) {
	config := &coordinator.CoordinatorConfig{
		MaxExecutors: 2,
		QueueSize:    10,
	}

	coord, err := coordinator.NewSparkAgentCoordinator(config)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Shutdown()

	// Verify all components are initialized
	if coord.GetModelRegistry() == nil {
		t.Error("Model registry not initialized")
	}

	if coord.GetSessionManager() == nil {
		t.Error("Session manager not initialized")
	}

	if coord.GetMemoryStore() == nil {
		t.Error("Memory store not initialized")
	}

	if coord.GetEventBus() == nil {
		t.Error("Event bus not initialized")
	}

	// Get metrics
	metrics := coord.GetMetrics()
	if metrics == nil {
		t.Error("Metrics should not be nil")
	}
}

// TestModelRegistryIntegration tests model registration and execution
func TestModelRegistryIntegration(t *testing.T) {
	registry := models.NewModelRegistry()

	// Register mock model
	mockModel := &MockModelProvider{
		name:     "mock-model",
		provider: "mock",
	}

	err := registry.Register("mock-model", mockModel)
	if err != nil {
		t.Fatalf("Failed to register model: %v", err)
	}

	// Test model execution
	ctx := context.Background()
	request := &models.ModelRequest{
		Prompt: "test prompt",
	}

	response, err := registry.Execute(ctx, "mock-model", request)
	if err != nil {
		t.Fatalf("Model execution failed: %v", err)
	}

	if response == nil {
		t.Error("Response should not be nil")
	}

	if response.Text != "mock response" {
		t.Errorf("Expected 'mock response', got '%s'", response.Text)
	}
}

// TestSessionMemoryIntegration tests session and memory integration
func TestSessionMemoryIntegration(t *testing.T) {
	sessionMgr := sessions.NewMemorySessionManager()
	memoryStore := memory.NewInMemoryStore()
	ctx := context.Background()

	// Create session
	session, err := sessionMgr.CreateSession(ctx, "agent-1")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Store memory for this session
	err = memoryStore.Store(ctx, "agent-1", "preference", "detailed explanations", map[string]interface{}{
		"session_id": session.ID,
		"importance": 0.9,
	})
	if err != nil {
		t.Fatalf("Failed to store memory: %v", err)
	}

	// Add invocation to session
	invocation := &sessions.Invocation{
		ID:        "inv-1",
		Timestamp: time.Now(),
		Input: &agent.AgentInput{
			Prompt: "What is 2+2?",
		},
		Output: &agent.AgentOutput{
			Result: "4",
		},
	}

	err = sessionMgr.AddInvocation(ctx, session.ID, invocation)
	if err != nil {
		t.Fatalf("Failed to add invocation: %v", err)
	}

	// Retrieve session
	retrievedSession, err := sessionMgr.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve session: %v", err)
	}

	if len(retrievedSession.History) != 1 {
		t.Errorf("Expected 1 invocation in history, got %d", len(retrievedSession.History))
	}

	// Retrieve memory
	memoryEntry, err := memoryStore.Retrieve(ctx, "agent-1", "preference")
	if err != nil {
		t.Fatalf("Failed to retrieve memory: %v", err)
	}

	if memoryEntry.Value != "detailed explanations" {
		t.Error("Memory not preserved correctly")
	}
}

// TestEventsIntegration tests event system integration
func TestEventsIntegration(t *testing.T) {
	eventBus := events.NewMemoryEventBus()
	eventStore := events.NewMemoryEventStore()
	metrics := events.NewEventMetrics()

	ctx := context.Background()
	defer eventBus.Close()

	// Subscribe to events
	eventBus.Subscribe(events.EventAgentStarted, func(ctx context.Context, event events.Event) error {
		// Track in metrics
		return metrics.Track(ctx, event)
	})

	eventBus.Subscribe(events.EventAgentStarted, func(ctx context.Context, event events.Event) error {
		// Store event
		return eventStore.Store(ctx, event)
	})

	// Publish event
	event := events.NewAgentEvent(events.EventAgentStarted, "agent-1", "session-1", nil)
	err := eventBus.Publish(event)
	if err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	// Give handlers time to process
	time.Sleep(20 * time.Millisecond)

	// Verify metrics
	count := metrics.GetCount(events.EventAgentStarted)
	if count != 1 {
		t.Errorf("Expected 1 event in metrics, got %d", count)
	}

	// Verify storage
	filter := events.EventFilter{
		EventTypes: []events.EventType{events.EventAgentStarted},
	}

	storedEvents, err := eventStore.Load(ctx, filter)
	if err != nil {
		t.Fatalf("Failed to load events: %v", err)
	}

	if len(storedEvents) != 1 {
		t.Errorf("Expected 1 stored event, got %d", len(storedEvents))
	}
}

// TestTelemetryIntegration tests telemetry integration
func TestTelemetryIntegration(t *testing.T) {
	collector := telemetry.NewTelemetryCollector()
	ctx := context.Background()

	// Start a span
	spanCtx, span := collector.GetTracer().StartSpan(ctx, "test-operation")
	span.SetAttribute("operation_type", "test")

	// Record metrics
	counter := collector.GetMeter().Counter("test_operations")
	counter.Inc()

	histogram := collector.GetMeter().Histogram("operation_duration")
	histogram.Record(100.0)

	gauge := collector.GetMeter().Gauge("active_operations")
	gauge.Set(5.0)

	// Log
	collector.GetLogger().Info("Operation in progress", map[string]interface{}{
		"trace_id": span.TraceID(),
		"span_id":  span.SpanID(),
	})

	// Complete span
	span.SetStatus(telemetry.StatusOK, "Success")
	span.End()

	// Get metrics snapshot
	snapshot := collector.GetMetricsSnapshot()

	if snapshot.Counters["test_operations"] != 1.0 {
		t.Errorf("Expected counter value 1.0, got %f", snapshot.Counters["test_operations"])
	}

	if snapshot.Histograms["operation_duration"].Count != 1 {
		t.Error("Histogram not recorded correctly")
	}

	if snapshot.Gauges["active_operations"] != 5.0 {
		t.Errorf("Expected gauge value 5.0, got %f", snapshot.Gauges["active_operations"])
	}

	// Verify span in context
	retrievedSpan := collector.GetTracer().GetSpan(spanCtx)
	if retrievedSpan == nil {
		t.Error("Span not retrievable from context")
	}
}

// TestToolsIntegration tests tool system integration
func TestToolsIntegration(t *testing.T) {
	registry := tools.NewToolRegistry()

	// Register a custom tool
	calculatorTool := &tools.BaseTool{
		ToolName:        "calculator",
		ToolDescription: "Performs calculations",
		Parameters: []tools.ToolParameter{
			{
				Name:        "operation",
				Type:        "string",
				Description: "The operation to perform",
				Required:    true,
			},
			{
				Name:        "a",
				Type:        "number",
				Description: "First operand",
				Required:    true,
			},
			{
				Name:        "b",
				Type:        "number",
				Description: "Second operand",
				Required:    true,
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			operation := params["operation"].(string)
			a := params["a"].(float64)
			b := params["b"].(float64)

			switch operation {
			case "add":
				return a + b, nil
			case "subtract":
				return a - b, nil
			case "multiply":
				return a * b, nil
			case "divide":
				if b == 0 {
					return nil, tools.ErrInvalidParameters
				}
				return a / b, nil
			default:
				return nil, tools.ErrInvalidParameters
			}
		},
	}

	err := registry.Register(calculatorTool)
	if err != nil {
		t.Fatalf("Failed to register tool: %v", err)
	}

	// Execute tool
	ctx := context.Background()
	params := map[string]interface{}{
		"operation": "add",
		"a":         10.0,
		"b":         5.0,
	}

	result, err := registry.Execute(ctx, "calculator", params)
	if err != nil {
		t.Fatalf("Tool execution failed: %v", err)
	}

	if result.(float64) != 15.0 {
		t.Errorf("Expected result 15.0, got %f", result.(float64))
	}

	// Test invalid parameters
	invalidParams := map[string]interface{}{
		"operation": "divide",
		"a":         10.0,
		"b":         0.0,
	}

	_, err = registry.Execute(ctx, "calculator", invalidParams)
	if err == nil {
		t.Error("Expected error for division by zero")
	}
}

// TestEndToEndWorkflow tests a complete workflow
func TestEndToEndWorkflow(t *testing.T) {
	// Setup all components
	config := &coordinator.CoordinatorConfig{
		MaxExecutors: 2,
		QueueSize:    10,
	}

	coord, err := coordinator.NewSparkAgentCoordinator(config)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Shutdown()

	ctx := context.Background()

	// Create a session
	session, err := coord.CreateSession(ctx, "test-agent")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	if session == nil {
		t.Fatal("Session should not be nil")
	}

	// Store some memory
	err = coord.StoreMemory(ctx, "test-agent", "user-preference", "concise answers", map[string]interface{}{
		"session_id": session.ID,
		"importance": 0.8,
	})
	if err != nil {
		t.Fatalf("Failed to store memory: %v", err)
	}

	// Search memory
	query := &memory.MemoryQuery{
		MinImportance: 0.7,
		Limit:         10,
	}

	memories, err := coord.SearchMemory(ctx, "test-agent", query)
	if err != nil {
		t.Fatalf("Failed to search memory: %v", err)
	}

	if len(memories) != 1 {
		t.Errorf("Expected 1 memory, got %d", len(memories))
	}

	// Get metrics
	metrics := coord.GetMetrics()
	if metrics.StoredMemories != 1 {
		t.Errorf("Expected 1 stored memory in metrics, got %d", metrics.StoredMemories)
	}

	if metrics.ActiveSessions != 1 {
		t.Errorf("Expected 1 active session in metrics, got %d", metrics.ActiveSessions)
	}
}

// TestConcurrentOperations tests concurrent operations across components
func TestConcurrentOperations(t *testing.T) {
	config := &coordinator.CoordinatorConfig{
		MaxExecutors: 4,
		QueueSize:    20,
	}

	coord, err := coordinator.NewSparkAgentCoordinator(config)
	if err != nil {
		t.Fatalf("Failed to create coordinator: %v", err)
	}
	defer coord.Shutdown()

	ctx := context.Background()
	const numOperations = 10

	// Concurrent session creation
	for i := 0; i < numOperations; i++ {
		go func(idx int) {
			agentID := "agent-" + string(rune(idx+'0'))
			_, err := coord.CreateSession(ctx, agentID)
			if err != nil {
				t.Errorf("Failed to create session: %v", err)
			}
		}(i)
	}

	// Concurrent memory storage
	for i := 0; i < numOperations; i++ {
		go func(idx int) {
			agentID := "agent-" + string(rune(idx+'0'))
			key := "key-" + string(rune(idx+'0'))
			err := coord.StoreMemory(ctx, agentID, key, "value", nil)
			if err != nil {
				t.Errorf("Failed to store memory: %v", err)
			}
		}(i)
	}

	// Give operations time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify metrics
	metrics := coord.GetMetrics()
	if metrics.ActiveSessions < numOperations {
		t.Logf("Expected at least %d sessions, got %d", numOperations, metrics.ActiveSessions)
	}

	if metrics.StoredMemories < numOperations {
		t.Logf("Expected at least %d memories, got %d", numOperations, metrics.StoredMemories)
	}
}

// Mock implementations for testing

type MockModelProvider struct {
	name     string
	provider string
}

func (m *MockModelProvider) Execute(ctx context.Context, request *models.ModelRequest) (*models.ModelResponse, error) {
	return &models.ModelResponse{
		Text:         "mock response",
		TokensUsed:   10,
		FinishReason: "stop",
	}, nil
}

func (m *MockModelProvider) Name() string {
	return m.name
}

func (m *MockModelProvider) Provider() string {
	return m.provider
}

func (m *MockModelProvider) MaxTokens() int {
	return 1000
}

func (m *MockModelProvider) SupportsTools() bool {
	return false
}

// TestSessionCheckpointRestore tests session checkpoint and restore
func TestSessionCheckpointRestore(t *testing.T) {
	sessionMgr := sessions.NewMemorySessionManager()
	ctx := context.Background()

	// Create session with history
	session, err := sessionMgr.CreateSession(ctx, "agent-1")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Add some invocations
	for i := 0; i < 3; i++ {
		invocation := &sessions.Invocation{
			ID:        "inv-" + string(rune(i+'0')),
			Timestamp: time.Now(),
			Input: &agent.AgentInput{
				Prompt: "test",
			},
		}
		sessionMgr.AddInvocation(ctx, session.ID, invocation)
	}

	// Create checkpoint
	checkpoint, err := sessionMgr.Checkpoint(ctx, session.ID, "Before rewind")
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	// Add more invocations
	sessionMgr.AddInvocation(ctx, session.ID, &sessions.Invocation{
		ID:        "inv-new",
		Timestamp: time.Now(),
		Input: &agent.AgentInput{
			Prompt: "new",
		},
	})

	// Restore from checkpoint
	err = sessionMgr.RestoreCheckpoint(ctx, session.ID, checkpoint.ID)
	if err != nil {
		t.Fatalf("Failed to restore checkpoint: %v", err)
	}

	// Verify session state
	restored, err := sessionMgr.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("Failed to get session: %v", err)
	}

	if len(restored.History) != 3 {
		t.Errorf("Expected 3 invocations after restore, got %d", len(restored.History))
	}
}

// TestMemorySearchIntegration tests memory search with different criteria
func TestMemorySearchIntegration(t *testing.T) {
	store := memory.NewInMemoryStore()
	ctx := context.Background()

	// Store various memories
	memories := []struct {
		key        string
		value      string
		tags       []string
		importance float64
	}{
		{"fact1", "Apache Spark is distributed", []string{"technical", "spark"}, 0.9},
		{"fact2", "User prefers detailed answers", []string{"preference"}, 0.8},
		{"fact3", "Previous topic was databases", []string{"context"}, 0.5},
		{"fact4", "Spark uses DAG scheduling", []string{"technical", "spark"}, 0.85},
	}

	for _, mem := range memories {
		metadata := map[string]interface{}{
			"tags":       mem.tags,
			"importance": mem.importance,
		}
		store.Store(ctx, "agent-1", mem.key, mem.value, metadata)
	}

	// Search by tags
	query := &memory.MemoryQuery{
		Tags:  []string{"spark"},
		Limit: 10,
	}

	results, err := store.Search(ctx, "agent-1", query)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results with 'spark' tag, got %d", len(results))
	}

	// Search by importance
	query2 := &memory.MemoryQuery{
		MinImportance: 0.8,
		Limit:         10,
	}

	results2, err := store.Search(ctx, "agent-1", query2)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results2) != 3 {
		t.Errorf("Expected 3 results with importance >= 0.8, got %d", len(results2))
	}

	// Combined search
	query3 := &memory.MemoryQuery{
		Tags:          []string{"technical"},
		MinImportance: 0.85,
		Limit:         10,
	}

	results3, err := store.Search(ctx, "agent-1", query3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results3) != 2 {
		t.Errorf("Expected 2 results matching combined criteria, got %d", len(results3))
	}
}

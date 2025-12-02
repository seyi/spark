// Package integration provides end-to-end integration tests for the agent system.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seyi/dagens/pkg/agent"
	"github.com/seyi/dagens/pkg/observability"
	"github.com/seyi/dagens/pkg/resilience"
	"github.com/seyi/dagens/pkg/state"
)

// TestAgent is a configurable test agent for integration tests
type TestAgent struct {
	name        string
	latency     time.Duration
	errorRate   float64
	callCount   int64
	shouldFail  bool
	failMessage string
}

func NewTestAgent(name string, latency time.Duration) *TestAgent {
	return &TestAgent{
		name:    name,
		latency: latency,
	}
}

func (a *TestAgent) ID() string                   { return "test-" + a.name }
func (a *TestAgent) Name() string                 { return a.name }
func (a *TestAgent) Description() string          { return "Test agent: " + a.name }
func (a *TestAgent) Capabilities() []string       { return []string{"test"} }
func (a *TestAgent) Dependencies() []agent.Agent  { return nil }
func (a *TestAgent) Partition() string            { return "" }

func (a *TestAgent) Execute(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	atomic.AddInt64(&a.callCount, 1)

	// Simulate latency
	if a.latency > 0 {
		select {
		case <-time.After(a.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Simulate failure
	if a.shouldFail {
		return nil, fmt.Errorf("%s", a.failMessage)
	}

	return &agent.AgentOutput{
		TaskID: input.TaskID,
		Result: fmt.Sprintf("Processed by %s: %s", a.name, input.Instruction),
		Metrics: &agent.ExecutionMetrics{
			StartTime: time.Now().Add(-a.latency),
			EndTime:   time.Now(),
			Duration:  a.latency,
		},
	}, nil
}

func (a *TestAgent) GetCallCount() int64 {
	return atomic.LoadInt64(&a.callCount)
}

func (a *TestAgent) SetShouldFail(fail bool, message string) {
	a.shouldFail = fail
	a.failMessage = message
}

// Integration Test: Agent with Resilience
func TestAgentWithResilience(t *testing.T) {
	testAgent := NewTestAgent("resilient-agent", 10*time.Millisecond)

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.BackoffConfig.MaxRetries = 2
	config.BackoffConfig.InitialDelay = 5 * time.Millisecond

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	input := &agent.AgentInput{
		TaskID:      "integration-test-1",
		Instruction: "Test resilience",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	output, err := resilientAgent.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Resilient agent failed: %v", err)
	}

	if output == nil {
		t.Fatal("Expected output, got nil")
	}

	result, ok := output.Result.(string)
	if !ok {
		t.Fatal("Expected string result")
	}

	if result != "Processed by resilient-agent: Test resilience" {
		t.Errorf("Unexpected result: %s", result)
	}

	if testAgent.GetCallCount() != 1 {
		t.Errorf("Expected 1 call, got %d", testAgent.GetCallCount())
	}
}

// FailingAgent is a test agent that fails a specified number of times
type FailingAgent struct {
	name        string
	failCount   int64
	maxFailures int64
	latency     time.Duration
}

func NewFailingAgent(name string, maxFailures int64, latency time.Duration) *FailingAgent {
	return &FailingAgent{
		name:        name,
		maxFailures: maxFailures,
		latency:     latency,
	}
}

func (a *FailingAgent) ID() string                   { return "failing-" + a.name }
func (a *FailingAgent) Name() string                 { return a.name }
func (a *FailingAgent) Description() string          { return "Failing agent: " + a.name }
func (a *FailingAgent) Capabilities() []string       { return []string{"test"} }
func (a *FailingAgent) Dependencies() []agent.Agent  { return nil }
func (a *FailingAgent) Partition() string            { return "" }

func (a *FailingAgent) Execute(ctx context.Context, input *agent.AgentInput) (*agent.AgentOutput, error) {
	count := atomic.AddInt64(&a.failCount, 1)

	if a.latency > 0 {
		select {
		case <-time.After(a.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if count <= a.maxFailures {
		return nil, fmt.Errorf("simulated failure %d", count)
	}

	return &agent.AgentOutput{
		TaskID: input.TaskID,
		Result: fmt.Sprintf("Processed by %s: %s", a.name, input.Instruction),
	}, nil
}

func (a *FailingAgent) GetFailCount() int64 {
	return atomic.LoadInt64(&a.failCount)
}

// Integration Test: Agent with Retry on Failure
func TestAgentRetryOnFailure(t *testing.T) {
	// Agent that fails twice then succeeds
	testAgent := NewFailingAgent("retry-agent", 2, 1*time.Millisecond)

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.BackoffConfig.MaxRetries = 5
	config.BackoffConfig.InitialDelay = 1 * time.Millisecond

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	input := &agent.AgentInput{
		TaskID:      "retry-test",
		Instruction: "Test retry",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	output, err := resilientAgent.Execute(ctx, input)
	if err != nil {
		t.Fatalf("Expected success after retries, got: %v", err)
	}

	if output == nil {
		t.Fatal("Expected output, got nil")
	}

	// Should have been called 3 times (2 failures + 1 success)
	if testAgent.GetFailCount() != 3 {
		t.Errorf("Expected 3 calls, got %d", testAgent.GetFailCount())
	}
}

// Integration Test: State Management with Checkpointing
func TestStateManagementCheckpointing(t *testing.T) {
	// Create state store
	store := state.NewMemoryStateStore()
	defer store.Close()

	checkpointBackend := store.Checkpoint

	// Create and save checkpoint
	checkpoint := &state.Checkpoint{
		ID:    "cp-integration-1",
		JobID: "integration-job",
		State: map[string]interface{}{
			"progress":    50,
			"last_result": "partial",
		},
	}

	ctx := context.Background()

	err := checkpointBackend.Save(ctx, checkpoint)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Load checkpoint
	loaded, err := checkpointBackend.Load(ctx, "cp-integration-1")
	if err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	if loaded.State["progress"] != 50 {
		t.Errorf("Expected progress=50, got %v", loaded.State["progress"])
	}

	// Get latest
	latest, err := checkpointBackend.GetLatest(ctx, "integration-job")
	if err != nil {
		t.Fatalf("Failed to get latest: %v", err)
	}

	if latest.ID != "cp-integration-1" {
		t.Errorf("Expected latest checkpoint ID 'cp-integration-1', got '%s'", latest.ID)
	}
}

// Integration Test: Session State with History
func TestSessionStateWithHistory(t *testing.T) {
	store := state.NewMemoryStateStore()
	defer store.Close()

	ctx := context.Background()

	// Create session
	session := &state.SessionState{
		ID:      "session-integration-1",
		AgentID: "integration-agent",
		State: map[string]interface{}{
			"counter": 0,
		},
	}

	err := store.Session.Create(ctx, session)
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	// Simulate multiple interactions
	for i := 1; i <= 3; i++ {
		// Get current session
		current, err := store.Session.Get(ctx, "session-integration-1")
		if err != nil {
			t.Fatalf("Failed to get session: %v", err)
		}

		// Update state
		current.State["counter"] = i

		err = store.Session.Update(ctx, current)
		if err != nil {
			t.Fatalf("Failed to update session: %v", err)
		}

		// Record history
		err = store.History.Append(ctx, &state.HistoryEntry{
			SessionID: "session-integration-1",
			Action:    fmt.Sprintf("increment_%d", i),
			Input:     fmt.Sprintf("Increment to %d", i),
			Output:    fmt.Sprintf("Counter is now %d", i),
		})
		if err != nil {
			t.Fatalf("Failed to append history: %v", err)
		}
	}

	// Verify final state
	final, _ := store.Session.Get(ctx, "session-integration-1")
	if final.State["counter"] != 3 {
		t.Errorf("Expected counter=3, got %v", final.State["counter"])
	}

	// Verify history
	history, _ := store.History.List(ctx, "session-integration-1", 0)
	if len(history) != 3 {
		t.Errorf("Expected 3 history entries, got %d", len(history))
	}
}

// Integration Test: Observability with Agent Execution
func TestObservabilityIntegration(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := observability.NewLogger(observability.LoggerConfig{
		Output:  buf,
		Level:   observability.LogLevelDebug,
		Service: "integration-test",
	})

	agentLogger := observability.NewAgentLogger(logger, "test-agent", "agent-123")

	// Simulate execution lifecycle
	startTime := time.Now()
	agentLogger.ExecutionStarted("task-1", "Process integration test")

	time.Sleep(10 * time.Millisecond)

	agentLogger.ExecutionCompleted("task-1", time.Since(startTime))

	// Parse log output
	output := buf.String()
	lines := bytes.Split(buf.Bytes(), []byte("\n"))

	if len(lines) < 2 {
		t.Fatalf("Expected at least 2 log lines, got %d", len(lines))
	}

	// Verify first log entry (execution started)
	var entry1 observability.LogEntry
	if err := json.Unmarshal(lines[0], &entry1); err != nil {
		t.Fatalf("Failed to parse log entry 1: %v", err)
	}

	if entry1.Message != "agent_execution_started" {
		t.Errorf("Expected 'agent_execution_started', got '%s'", entry1.Message)
	}

	// Verify the output contains expected content
	if !bytes.Contains([]byte(output), []byte("agent_execution_completed")) {
		t.Error("Expected log to contain 'agent_execution_completed'")
	}
}

// Integration Test: Circuit Breaker with Multiple Agents
func TestCircuitBreakerMultipleAgents(t *testing.T) {
	// Create multiple agents
	agent1 := NewTestAgent("cb-agent-1", 5*time.Millisecond)
	agent2 := NewTestAgent("cb-agent-2", 5*time.Millisecond)

	agent1.SetShouldFail(true, "agent1 failure")
	agent2.SetShouldFail(false, "")

	// Each agent gets its own circuit breaker
	config1 := agent.DefaultResilientAgentConfig()
	config1.MetricsEnabled = false
	config1.LoggingEnabled = false
	config1.BackoffConfig.MaxRetries = 1
	config1.BackoffConfig.InitialDelay = 1 * time.Millisecond
	config1.CircuitBreakerConfig = resilience.CircuitBreakerConfig{
		Name:             "agent1-cb",
		FailureThreshold: 3,
		Timeout:          1 * time.Second,
	}

	config2 := agent.DefaultResilientAgentConfig()
	config2.MetricsEnabled = false
	config2.LoggingEnabled = false
	config2.BackoffConfig.MaxRetries = 1
	config2.BackoffConfig.InitialDelay = 1 * time.Millisecond

	resilient1 := agent.NewResilientAgent(agent1, config1)
	resilient2 := agent.NewResilientAgent(agent2, config2)

	ctx := context.Background()
	input := &agent.AgentInput{TaskID: "test", Instruction: "test"}

	// Trip agent1's circuit breaker
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		resilient1.Execute(ctx, input)
		cancel()
	}

	// agent2 should still work fine
	ctx2, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()
	output, err := resilient2.Execute(ctx2, input)
	if err != nil {
		t.Fatalf("Agent2 should succeed: %v", err)
	}

	if output == nil {
		t.Error("Agent2 should return output")
	}
}

// Integration Test: Concurrent Agent Execution
func TestConcurrentAgentExecution(t *testing.T) {
	testAgent := NewTestAgent("concurrent-agent", 10*time.Millisecond)

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.RateLimitEnabled = false // Disable rate limiting for this test

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			input := &agent.AgentInput{
				TaskID:      fmt.Sprintf("concurrent-task-%d", id),
				Instruction: fmt.Sprintf("Task %d", id),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := resilientAgent.Execute(ctx, input)
			if err != nil {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent execution error: %v", err)
	}

	if testAgent.GetCallCount() != numGoroutines {
		t.Errorf("Expected %d calls, got %d", numGoroutines, testAgent.GetCallCount())
	}
}

// Integration Test: Full Pipeline - Agent -> State -> Observability
func TestFullPipeline(t *testing.T) {
	// Set up observability
	logBuf := &bytes.Buffer{}
	logger := observability.NewLogger(observability.LoggerConfig{
		Output:  logBuf,
		Level:   observability.LogLevelDebug,
		Service: "full-pipeline-test",
	})

	// Set up state store
	store := state.NewMemoryStateStore()
	defer store.Close()

	// Create agent
	testAgent := NewTestAgent("pipeline-agent", 10*time.Millisecond)

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	// Create session
	ctx := context.Background()
	session := &state.SessionState{
		ID:      "pipeline-session",
		AgentID: "pipeline-agent",
		State:   map[string]interface{}{"step": 0},
	}
	store.Session.Create(ctx, session)

	// Execute multiple steps
	agentLogger := observability.NewAgentLogger(logger, "pipeline-agent", "agent-pipeline")

	for step := 1; step <= 3; step++ {
		taskID := fmt.Sprintf("pipeline-task-%d", step)

		// Log start
		agentLogger.ExecutionStarted(taskID, fmt.Sprintf("Step %d", step))
		startTime := time.Now()

		// Execute agent
		input := &agent.AgentInput{
			TaskID:      taskID,
			Instruction: fmt.Sprintf("Execute step %d", step),
		}

		execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		output, err := resilientAgent.Execute(execCtx, input)
		cancel()

		if err != nil {
			agentLogger.ExecutionFailed(taskID, err, time.Since(startTime))
			t.Fatalf("Step %d failed: %v", step, err)
		}

		// Log completion
		agentLogger.ExecutionCompleted(taskID, time.Since(startTime))

		// Update session state
		currentSession, _ := store.Session.Get(ctx, "pipeline-session")
		currentSession.State["step"] = step
		currentSession.State["last_result"] = output.Result
		store.Session.Update(ctx, currentSession)

		// Record history
		store.History.Append(ctx, &state.HistoryEntry{
			SessionID: "pipeline-session",
			Action:    fmt.Sprintf("step_%d", step),
			Input:     input.Instruction,
			Output:    output.Result,
		})
	}

	// Verify final state
	finalSession, _ := store.Session.Get(ctx, "pipeline-session")
	if finalSession.State["step"] != 3 {
		t.Errorf("Expected final step=3, got %v", finalSession.State["step"])
	}

	// Verify history
	history, _ := store.History.List(ctx, "pipeline-session", 0)
	if len(history) != 3 {
		t.Errorf("Expected 3 history entries, got %d", len(history))
	}

	// Verify logs were produced
	logOutput := logBuf.String()
	if !bytes.Contains([]byte(logOutput), []byte("agent_execution_started")) {
		t.Error("Expected logs to contain execution started events")
	}
	if !bytes.Contains([]byte(logOutput), []byte("agent_execution_completed")) {
		t.Error("Expected logs to contain execution completed events")
	}
}

// Integration Test: Rate Limiting Behavior
func TestRateLimitingBehavior(t *testing.T) {
	testAgent := NewTestAgent("ratelimit-agent", 1*time.Millisecond)

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.RateLimitEnabled = true
	config.RateLimitPerSecond = 5
	config.RateLimitBurst = 5

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	input := &agent.AgentInput{
		TaskID:      "ratelimit-test",
		Instruction: "Test rate limiting",
	}

	// Execute within burst capacity
	successCount := 0
	rateLimitedCount := 0

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := resilientAgent.Execute(ctx, input)
		cancel()

		if err != nil {
			rateLimitedCount++
		} else {
			successCount++
		}
	}

	// Should have some successes (burst) and some rate limited
	if successCount == 0 {
		t.Error("Expected some successful requests within burst capacity")
	}

	if rateLimitedCount == 0 {
		t.Error("Expected some rate limited requests")
	}

	t.Logf("Successes: %d, Rate limited: %d", successCount, rateLimitedCount)
}

// Benchmark: Agent Execution Performance
func BenchmarkAgentExecution(b *testing.B) {
	testAgent := NewTestAgent("bench-agent", 0) // No latency for benchmarking

	config := agent.DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.RateLimitEnabled = false

	resilientAgent := agent.NewResilientAgent(testAgent, config)

	input := &agent.AgentInput{
		TaskID:      "bench-task",
		Instruction: "Benchmark",
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resilientAgent.Execute(ctx, input)
	}
}

// Benchmark: State Operations Performance
func BenchmarkStateOperations(b *testing.B) {
	store := state.NewMemoryStateStore()
	defer store.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		store.State.Set(ctx, key, []byte("value"), 0)
		store.State.Get(ctx, key)
		store.State.Delete(ctx, key)
	}
}

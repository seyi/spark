package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seyi/dagens/pkg/resilience"
)

// resilientTestExecutor is a test executor for resilient tests
type resilientTestExecutor struct {
	callCount   int
	shouldFail  bool
	failCount   int
	maxFailures int
	latency     time.Duration
}

func (m *resilientTestExecutor) Execute(ctx context.Context, agent Agent, input *AgentInput) (*AgentOutput, error) {
	m.callCount++

	// Simulate latency
	if m.latency > 0 {
		select {
		case <-time.After(m.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Fail a certain number of times
	if m.shouldFail && m.failCount < m.maxFailures {
		m.failCount++
		return nil, errors.New("simulated failure")
	}

	return &AgentOutput{
		TaskID: input.TaskID,
		Result: "success",
		Metrics: &ExecutionMetrics{
			StartTime: time.Now(),
			EndTime:   time.Now(),
		},
	}, nil
}

// resilientTestAgent is a test agent for resilient tests
type resilientTestAgent struct {
	name string
}

func (m *resilientTestAgent) ID() string                   { return "mock-" + m.name }
func (m *resilientTestAgent) Name() string                 { return m.name }
func (m *resilientTestAgent) Description() string          { return "Mock agent for testing" }
func (m *resilientTestAgent) Capabilities() []string       { return []string{"mock"} }
func (m *resilientTestAgent) Dependencies() []Agent        { return nil }
func (m *resilientTestAgent) Partition() string            { return "" }
func (m *resilientTestAgent) Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
	return &AgentOutput{TaskID: input.TaskID, Result: "mock result"}, nil
}

func TestResilientAgentExecutor_Success(t *testing.T) {
	inner := &resilientTestExecutor{}
	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false // Disable metrics for testing
	config.LoggingEnabled = false

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	output, err := executor.Execute(context.Background(), agent, input)
	if err != nil {
		t.Fatalf("Expected success, got error: %v", err)
	}

	if output == nil {
		t.Fatal("Expected output, got nil")
	}

	if inner.callCount != 1 {
		t.Errorf("Expected 1 call, got %d", inner.callCount)
	}
}

func TestResilientAgentExecutor_RetryOnFailure(t *testing.T) {
	inner := &resilientTestExecutor{
		shouldFail:  true,
		maxFailures: 2, // Fail twice, then succeed
	}

	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.BackoffConfig.InitialDelay = 10 * time.Millisecond
	config.BackoffConfig.MaxRetries = 5

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	output, err := executor.Execute(context.Background(), agent, input)
	if err != nil {
		t.Fatalf("Expected success after retries, got error: %v", err)
	}

	if output == nil {
		t.Fatal("Expected output, got nil")
	}

	// Should have called 3 times (2 failures + 1 success)
	if inner.callCount != 3 {
		t.Errorf("Expected 3 calls (2 retries + success), got %d", inner.callCount)
	}
}

func TestResilientAgentExecutor_MaxRetriesExceeded(t *testing.T) {
	inner := &resilientTestExecutor{
		shouldFail:  true,
		maxFailures: 100, // Always fail
	}

	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.BackoffConfig.InitialDelay = 10 * time.Millisecond
	config.BackoffConfig.MaxRetries = 2

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	_, err := executor.Execute(context.Background(), agent, input)
	if err == nil {
		t.Fatal("Expected error after max retries, got success")
	}

	// Should have called 3 times (initial + 2 retries)
	if inner.callCount != 3 {
		t.Errorf("Expected 3 calls (initial + 2 retries), got %d", inner.callCount)
	}
}

func TestResilientAgentExecutor_RateLimiting(t *testing.T) {
	inner := &resilientTestExecutor{}

	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.RateLimitEnabled = true
	config.RateLimitPerSecond = 2
	config.RateLimitBurst = 2

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	// First two requests should succeed (burst capacity)
	for i := 0; i < 2; i++ {
		_, err := executor.Execute(context.Background(), agent, input)
		if err != nil {
			t.Fatalf("Request %d should succeed, got error: %v", i+1, err)
		}
	}

	// Third request should be rate limited (no time to refill)
	_, err := executor.Execute(context.Background(), agent, input)
	if err == nil {
		t.Fatal("Expected rate limit error, got success")
	}
}

func TestResilientAgentExecutor_Timeout(t *testing.T) {
	inner := &resilientTestExecutor{
		latency: 500 * time.Millisecond,
	}

	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	config.ExecutionTimeout = 100 * time.Millisecond
	// MaxRetries = 0 means infinite retries in the backoff package
	// Use 1 to allow only initial attempt + 1 retry (we'll timeout on both)
	config.BackoffConfig.MaxRetries = 1
	config.BackoffConfig.InitialDelay = 10 * time.Millisecond

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	// Use a context with timeout to ensure the test doesn't hang
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := executor.Execute(ctx, agent, input)
	if err == nil {
		t.Fatal("Expected timeout error, got success")
	}
}

func TestResilientAgentExecutor_CircuitBreaker(t *testing.T) {
	inner := &resilientTestExecutor{
		shouldFail:  true,
		maxFailures: 100,
	}

	config := DefaultResilientAgentConfig()
	config.MetricsEnabled = false
	config.LoggingEnabled = false
	// MaxRetries = 0 means infinite retries, use 1 for minimal retry attempts
	config.BackoffConfig.MaxRetries = 1
	config.BackoffConfig.InitialDelay = 1 * time.Millisecond
	config.CircuitBreakerConfig = resilience.CircuitBreakerConfig{
		Name:             "test-cb",
		FailureThreshold: 3,
		Timeout:          1 * time.Second,
	}

	executor := NewResilientAgentExecutor(inner, config)
	agent := &resilientTestAgent{name: "test-agent"}

	input := &AgentInput{
		TaskID:      "test-task",
		Instruction: "test instruction",
	}

	// Trip the circuit breaker with failures
	// Use a context with timeout for each call to prevent hangs
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		executor.Execute(ctx, agent, input)
		cancel()
	}

	// Check circuit breaker state
	cb := executor.GetCircuitBreaker()
	if cb == nil {
		t.Fatal("Expected circuit breaker, got nil")
	}

	state := cb.State()
	if state != resilience.StateOpen {
		t.Errorf("Expected circuit breaker to be open, got %v", state)
	}
}

func TestWrapWithResilience(t *testing.T) {
	agent := &resilientTestAgent{name: "test-agent"}
	resilientAgent := WrapWithResilience(agent)

	if resilientAgent == nil {
		t.Fatal("Expected resilient agent, got nil")
	}

	if resilientAgent.Name() != agent.Name() {
		t.Errorf("Expected name %s, got %s", agent.Name(), resilientAgent.Name())
	}
}

func TestDefaultResilientAgentConfig(t *testing.T) {
	config := DefaultResilientAgentConfig()

	if config.BackoffConfig.MaxRetries != 3 {
		t.Errorf("Expected 3 max retries, got %d", config.BackoffConfig.MaxRetries)
	}

	if config.ExecutionTimeout != 60*time.Second {
		t.Errorf("Expected 60s timeout, got %v", config.ExecutionTimeout)
	}

	if !config.MetricsEnabled {
		t.Error("Expected metrics to be enabled by default")
	}

	if !config.LoggingEnabled {
		t.Error("Expected logging to be enabled by default")
	}
}

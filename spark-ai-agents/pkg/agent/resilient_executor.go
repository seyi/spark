// Package agent provides resilient execution wrappers for AI agents.
// This file integrates resilience patterns from pkg/resilience into agent execution.
package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/spark/spark-ai-agents/pkg/observability"
	"github.com/apache/spark/spark-ai-agents/pkg/resilience"
)

// ResilientAgentConfig configures resilient agent execution
type ResilientAgentConfig struct {
	// Backoff configuration for retries
	BackoffConfig resilience.BackoffConfig

	// Circuit breaker configuration (per agent)
	CircuitBreakerConfig resilience.CircuitBreakerConfig

	// Rate limiting
	RateLimitEnabled   bool
	RateLimitPerSecond float64
	RateLimitBurst     int

	// Timeout per execution attempt
	ExecutionTimeout time.Duration

	// Enable metrics recording
	MetricsEnabled bool

	// Enable structured logging
	LoggingEnabled bool
}

// DefaultResilientAgentConfig returns production-ready defaults
func DefaultResilientAgentConfig() ResilientAgentConfig {
	return ResilientAgentConfig{
		BackoffConfig: resilience.BackoffConfig{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			JitterFactor: 0.1,
			MaxRetries:   3,
		},
		CircuitBreakerConfig: resilience.CircuitBreakerConfig{
			Name:               "agent",
			FailureThreshold:   5,
			SuccessThreshold:   2,
			Timeout:            30 * time.Second,
			HalfOpenMaxRequests: 3,
		},
		RateLimitEnabled:   true,
		RateLimitPerSecond: 100,
		RateLimitBurst:     200,
		ExecutionTimeout:   60 * time.Second,
		MetricsEnabled:     true,
		LoggingEnabled:     true,
	}
}

// ResilientAgentExecutor wraps an AgentExecutor with resilience patterns
type ResilientAgentExecutor struct {
	inner          AgentExecutor
	circuitBreaker *resilience.CircuitBreaker
	rateLimiter    resilience.RateLimiter
	retrier        *resilience.Retrier
	config         ResilientAgentConfig
	metrics        *observability.Metrics
	logger         *observability.Logger
}

// NewResilientAgentExecutor creates a resilient wrapper around an existing executor
func NewResilientAgentExecutor(inner AgentExecutor, config ResilientAgentConfig) *ResilientAgentExecutor {
	rae := &ResilientAgentExecutor{
		inner:   inner,
		config:  config,
		retrier: resilience.NewRetrier(config.BackoffConfig),
	}

	// Set up circuit breaker
	if config.CircuitBreakerConfig.Name != "" {
		rae.circuitBreaker = resilience.GetCircuitBreakerWithConfig(config.CircuitBreakerConfig)
	}

	// Set up rate limiter
	if config.RateLimitEnabled {
		rae.rateLimiter = resilience.NewTokenBucket(config.RateLimitPerSecond, config.RateLimitBurst)
	}

	// Set up metrics
	if config.MetricsEnabled {
		rae.metrics = observability.GetMetrics()
	}

	// Set up logging
	if config.LoggingEnabled {
		rae.logger = observability.GetLogger()
	}

	return rae
}

// Execute runs the inner executor with all resilience patterns applied
func (rae *ResilientAgentExecutor) Execute(ctx context.Context, agent Agent, input *AgentInput) (*AgentOutput, error) {
	start := time.Now()
	agentName := agent.Name()

	// Log execution start
	if rae.logger != nil {
		rae.logger.Info("agent_execution_started",
			observability.Field("agent_name", agentName),
			observability.Field("agent_id", agent.ID()),
			observability.Field("task_id", input.TaskID),
			observability.Field("instruction_length", len(input.Instruction)),
		)
	}

	// 1. Check rate limiter
	if rae.rateLimiter != nil {
		if !rae.rateLimiter.Allow() {
			if rae.metrics != nil {
				rae.metrics.RecordRateLimiterRejected(agentName)
			}
			if rae.logger != nil {
				rae.logger.Warn("agent_rate_limited",
					observability.Field("agent_name", agentName),
				)
			}
			return nil, fmt.Errorf("agent %s rate limited", agentName)
		}
		if rae.metrics != nil {
			rae.metrics.RecordRateLimiterAllowed(agentName)
		}
	}

	// 2. Execute with retry and circuit breaker
	var output *AgentOutput
	var lastErr error

	retryErr := rae.retrier.Do(ctx, func(ctx context.Context) error {
		// Apply timeout if configured
		execCtx := ctx
		if rae.config.ExecutionTimeout > 0 {
			var cancel context.CancelFunc
			execCtx, cancel = context.WithTimeout(ctx, rae.config.ExecutionTimeout)
			defer cancel()
		}

		// Execute through circuit breaker if configured
		if rae.circuitBreaker != nil {
			err := rae.circuitBreaker.Execute(execCtx, func(ctx context.Context) error {
				var opErr error
				output, opErr = rae.inner.Execute(ctx, agent, input)
				return opErr
			})

			if err != nil {
				lastErr = err

				// Record circuit breaker state
				if rae.metrics != nil {
					rae.metrics.RecordCircuitBreakerState(agentName, int(rae.circuitBreaker.State()))
					if err == resilience.ErrCircuitOpen {
						rae.metrics.RecordCircuitBreakerFailure(agentName)
					}
				}

				if rae.logger != nil {
					rae.logger.Warn("agent_execution_circuit_breaker",
						observability.Field("agent_name", agentName),
						observability.Field("state", rae.circuitBreaker.State().String()),
						observability.Field("error", err.Error()),
					)
				}

				return err
			}
		} else {
			// No circuit breaker, execute directly
			var opErr error
			output, opErr = rae.inner.Execute(execCtx, agent, input)
			if opErr != nil {
				lastErr = opErr
				return opErr
			}
		}

		return nil
	}, resilience.WithOnRetry(func(attempt int, err error, delay time.Duration) {
		if rae.logger != nil {
			rae.logger.Warn("agent_execution_retry",
				observability.Field("agent_name", agentName),
				observability.Field("attempt", attempt),
				observability.Field("error", err.Error()),
				observability.Field("next_delay_ms", delay.Milliseconds()),
			)
		}
		if rae.metrics != nil {
			rae.metrics.RecordAgentRetry(agentName)
		}
	}))

	duration := time.Since(start)

	// Record final metrics and log
	if retryErr != nil {
		if rae.metrics != nil {
			rae.metrics.RecordAgentExecution(agentName, "resilient", "error", duration)
			rae.metrics.RecordAgentError(agentName, categorizeError(lastErr))
		}
		if rae.logger != nil {
			rae.logger.Error("agent_execution_failed",
				observability.Field("agent_name", agentName),
				observability.Field("agent_id", agent.ID()),
				observability.Field("task_id", input.TaskID),
				observability.Field("duration_ms", duration.Milliseconds()),
				observability.Field("error", retryErr.Error()),
			)
		}
		return nil, retryErr
	}

	// Success
	if rae.metrics != nil {
		rae.metrics.RecordAgentExecution(agentName, "resilient", "success", duration)
	}
	if rae.logger != nil {
		rae.logger.Info("agent_execution_completed",
			observability.Field("agent_name", agentName),
			observability.Field("agent_id", agent.ID()),
			observability.Field("task_id", input.TaskID),
			observability.Field("duration_ms", duration.Milliseconds()),
		)
	}

	// Update output metrics
	if output != nil && output.Metrics != nil {
		output.Metrics.Duration = duration
	}

	return output, nil
}

// GetCircuitBreaker returns the circuit breaker for inspection
func (rae *ResilientAgentExecutor) GetCircuitBreaker() *resilience.CircuitBreaker {
	return rae.circuitBreaker
}

// GetRateLimiter returns the rate limiter for inspection
func (rae *ResilientAgentExecutor) GetRateLimiter() resilience.RateLimiter {
	return rae.rateLimiter
}

// categorizeError categorizes errors for metrics
func categorizeError(err error) string {
	if err == nil {
		return "none"
	}

	switch err {
	case resilience.ErrCircuitOpen:
		return "circuit_open"
	case resilience.ErrRateLimited:
		return "rate_limited"
	case context.DeadlineExceeded:
		return "timeout"
	case context.Canceled:
		return "canceled"
	default:
		return "execution_error"
	}
}

// ResilientAgent wraps an Agent with resilience patterns
// Use this when you want to add resilience to an existing agent
type ResilientAgent struct {
	Agent
	executor *ResilientAgentExecutor
}

// NewResilientAgent creates a resilient wrapper around an agent
func NewResilientAgent(agent Agent, config ResilientAgentConfig) *ResilientAgent {
	// Get the inner executor if it's a BaseAgent
	var innerExecutor AgentExecutor
	if baseAgent, ok := agent.(*BaseAgent); ok && baseAgent.executor != nil {
		innerExecutor = baseAgent.executor
	} else {
		// Use a pass-through executor
		innerExecutor = &passThroughExecutor{agent: agent}
	}

	// Customize circuit breaker name for this agent
	config.CircuitBreakerConfig.Name = agent.Name()

	return &ResilientAgent{
		Agent:    agent,
		executor: NewResilientAgentExecutor(innerExecutor, config),
	}
}

// Execute runs the agent with resilience patterns
func (ra *ResilientAgent) Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
	return ra.executor.Execute(ctx, ra.Agent, input)
}

// passThroughExecutor wraps an agent for direct execution
type passThroughExecutor struct {
	agent Agent
}

func (p *passThroughExecutor) Execute(ctx context.Context, agent Agent, input *AgentInput) (*AgentOutput, error) {
	return p.agent.Execute(ctx, input)
}

// WrapWithResilience is a helper to wrap any agent with default resilience config
func WrapWithResilience(agent Agent) *ResilientAgent {
	return NewResilientAgent(agent, DefaultResilientAgentConfig())
}

// WrapWithResilienceConfig is a helper to wrap any agent with custom resilience config
func WrapWithResilienceConfig(agent Agent, config ResilientAgentConfig) *ResilientAgent {
	return NewResilientAgent(agent, config)
}

// ExecuteWithResilience executes an agent operation with resilience patterns
// This is a convenience function for one-off executions
func ExecuteAgentWithResilience(ctx context.Context, agent Agent, input *AgentInput) (*AgentOutput, error) {
	resilientAgent := WrapWithResilience(agent)
	return resilientAgent.Execute(ctx, input)
}

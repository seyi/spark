// Package resilience provides production-grade resilience patterns.
// This file integrates resilience patterns with agent execution.
package resilience

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seyi/dagens/pkg/observability"
)

// ResilientExecutorConfig configures resilient execution behavior
type ResilientExecutorConfig struct {
	// Backoff configuration
	Backoff BackoffConfig

	// Circuit breaker name (uses global registry)
	CircuitBreakerName string
	CircuitBreaker     CircuitBreakerConfig

	// Rate limiter configuration
	RateLimitEnabled    bool
	RateLimitPerSecond  float64
	RateLimitBurst      int

	// Timeout for each attempt
	Timeout time.Duration

	// Enable metrics recording
	MetricsEnabled bool
}

// DefaultResilientExecutorConfig returns production-ready defaults
func DefaultResilientExecutorConfig(name string) ResilientExecutorConfig {
	return ResilientExecutorConfig{
		Backoff: BackoffConfig{
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			JitterFactor: 0.1,
			MaxRetries:   3,
		},
		CircuitBreakerName: name,
		CircuitBreaker:     DefaultCircuitBreakerConfig(name),
		RateLimitEnabled:   true,
		RateLimitPerSecond: 100,
		RateLimitBurst:     200,
		Timeout:            60 * time.Second,
		MetricsEnabled:     true,
	}
}

// ResilientExecutor wraps operations with resilience patterns
type ResilientExecutor struct {
	config         ResilientExecutorConfig
	circuitBreaker *CircuitBreaker
	rateLimiter    RateLimiter
	retrier        *Retrier
	metrics        *observability.Metrics
	mu             sync.RWMutex
}

// NewResilientExecutor creates a new resilient executor
func NewResilientExecutor(config ResilientExecutorConfig) *ResilientExecutor {
	re := &ResilientExecutor{
		config:  config,
		retrier: NewRetrier(config.Backoff),
	}

	// Set up circuit breaker
	if config.CircuitBreakerName != "" {
		re.circuitBreaker = GetCircuitBreakerWithConfig(config.CircuitBreaker)
	}

	// Set up rate limiter
	if config.RateLimitEnabled {
		re.rateLimiter = NewTokenBucket(config.RateLimitPerSecond, config.RateLimitBurst)
	}

	// Set up metrics
	if config.MetricsEnabled {
		re.metrics = observability.GetMetrics()
	}

	return re
}

// Execute runs an operation with all resilience patterns applied
func (re *ResilientExecutor) Execute(ctx context.Context, name string, operation func(context.Context) error) error {
	return re.ExecuteWithResult(ctx, name, func(ctx context.Context) (interface{}, error) {
		return nil, operation(ctx)
	})
}

// ExecuteWithResult runs an operation that returns a result with all resilience patterns
func (re *ResilientExecutor) ExecuteWithResult(ctx context.Context, name string, operation func(context.Context) (interface{}, error)) error {
	_, err := re.ExecuteWithResultTyped(ctx, name, operation)
	return err
}

// ExecuteWithResultTyped runs an operation with typed result
func (re *ResilientExecutor) ExecuteWithResultTyped(ctx context.Context, name string, operation func(context.Context) (interface{}, error)) (interface{}, error) {
	start := time.Now()

	// 1. Check rate limiter
	if re.rateLimiter != nil {
		if !re.rateLimiter.Allow() {
			if re.metrics != nil {
				re.metrics.RecordRateLimiterRejected(name)
			}
			return nil, ErrRateLimited
		}
		if re.metrics != nil {
			re.metrics.RecordRateLimiterAllowed(name)
		}
	}

	// 2. Execute through circuit breaker with retry
	var result interface{}

	retryErr := re.retrier.Do(ctx, func(ctx context.Context) error {
		// Apply timeout if configured
		execCtx := ctx
		if re.config.Timeout > 0 {
			var cancel context.CancelFunc
			execCtx, cancel = context.WithTimeout(ctx, re.config.Timeout)
			defer cancel()
		}

		// Execute through circuit breaker if configured
		if re.circuitBreaker != nil {
			err := re.circuitBreaker.Execute(execCtx, func(ctx context.Context) error {
				var opErr error
				result, opErr = operation(ctx)
				return opErr
			})

			if err != nil {
				// Record circuit breaker state
				if re.metrics != nil {
					re.metrics.RecordCircuitBreakerState(name, int(re.circuitBreaker.State()))
					if err == ErrCircuitOpen {
						re.metrics.RecordCircuitBreakerFailure(name)
					}
				}
				return err
			}
		} else {
			// No circuit breaker, execute directly
			var opErr error
			result, opErr = operation(execCtx)
			if opErr != nil {
				return opErr
			}
		}

		return nil
	}, WithOnRetry(func(attempt int, err error, delay time.Duration) {
		// Log retry attempt
		if re.metrics != nil {
			// Could add retry metrics here
		}
	}))

	duration := time.Since(start)

	// Record metrics
	if re.metrics != nil {
		if retryErr != nil {
			re.metrics.RecordAgentExecution(name, "resilient", "error", duration)
		} else {
			re.metrics.RecordAgentExecution(name, "resilient", "success", duration)
		}
	}

	if retryErr != nil {
		return nil, retryErr
	}

	return result, nil
}

// GetCircuitBreaker returns the circuit breaker for inspection
func (re *ResilientExecutor) GetCircuitBreaker() *CircuitBreaker {
	return re.circuitBreaker
}

// GetRateLimiter returns the rate limiter for inspection
func (re *ResilientExecutor) GetRateLimiter() RateLimiter {
	return re.rateLimiter
}

// ResilientAgentExecutor wraps agent execution with resilience patterns
type ResilientAgentExecutor struct {
	executors map[string]*ResilientExecutor
	mu        sync.RWMutex

	// Default config for new executors
	defaultConfig ResilientExecutorConfig
}

// NewResilientAgentExecutor creates a new resilient agent executor
func NewResilientAgentExecutor(defaultConfig ResilientExecutorConfig) *ResilientAgentExecutor {
	return &ResilientAgentExecutor{
		executors:     make(map[string]*ResilientExecutor),
		defaultConfig: defaultConfig,
	}
}

// GetExecutor returns or creates a resilient executor for an agent
func (rae *ResilientAgentExecutor) GetExecutor(agentName string) *ResilientExecutor {
	rae.mu.RLock()
	executor, exists := rae.executors[agentName]
	rae.mu.RUnlock()

	if exists {
		return executor
	}

	rae.mu.Lock()
	defer rae.mu.Unlock()

	// Double-check
	if executor, exists = rae.executors[agentName]; exists {
		return executor
	}

	// Create new executor with agent-specific config
	config := rae.defaultConfig
	config.CircuitBreakerName = agentName
	config.CircuitBreaker.Name = agentName

	executor = NewResilientExecutor(config)
	rae.executors[agentName] = executor

	return executor
}

// SetConfig sets custom config for a specific agent
func (rae *ResilientAgentExecutor) SetConfig(agentName string, config ResilientExecutorConfig) {
	rae.mu.Lock()
	defer rae.mu.Unlock()

	config.CircuitBreakerName = agentName
	config.CircuitBreaker.Name = agentName
	rae.executors[agentName] = NewResilientExecutor(config)
}

// Stats returns statistics for all executors
func (rae *ResilientAgentExecutor) Stats() map[string]ExecutorStats {
	rae.mu.RLock()
	defer rae.mu.RUnlock()

	stats := make(map[string]ExecutorStats)
	for name, executor := range rae.executors {
		var cbState string
		var cbFailures, cbSuccesses int64

		if executor.circuitBreaker != nil {
			cbState = executor.circuitBreaker.State().String()
			cbFailures, cbSuccesses = executor.circuitBreaker.Counts()
		}

		stats[name] = ExecutorStats{
			CircuitBreakerState:    cbState,
			CircuitBreakerFailures: cbFailures,
			CircuitBreakerSuccesses: cbSuccesses,
		}
	}

	return stats
}

// ExecutorStats holds statistics for a resilient executor
type ExecutorStats struct {
	CircuitBreakerState     string `json:"circuit_breaker_state"`
	CircuitBreakerFailures  int64  `json:"circuit_breaker_failures"`
	CircuitBreakerSuccesses int64  `json:"circuit_breaker_successes"`
}

// Global resilient agent executor
var (
	globalAgentExecutor *ResilientAgentExecutor
	agentExecutorOnce   sync.Once
)

// GetResilientAgentExecutor returns the global resilient agent executor
func GetResilientAgentExecutor() *ResilientAgentExecutor {
	agentExecutorOnce.Do(func() {
		globalAgentExecutor = NewResilientAgentExecutor(DefaultResilientExecutorConfig("default"))
	})
	return globalAgentExecutor
}

// ExecuteWithResilience executes an operation with default resilience patterns
func ExecuteWithResilience(ctx context.Context, name string, operation func(context.Context) error) error {
	executor := GetResilientAgentExecutor().GetExecutor(name)
	return executor.Execute(ctx, name, operation)
}

// ExecuteWithResilienceResult executes an operation that returns a result
func ExecuteWithResilienceResult[T any](ctx context.Context, name string, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	executor := GetResilientAgentExecutor().GetExecutor(name)

	result, err := executor.ExecuteWithResultTyped(ctx, name, func(ctx context.Context) (interface{}, error) {
		return operation(ctx)
	})

	if err != nil {
		return zero, err
	}

	typed, ok := result.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected result type")
	}

	return typed, nil
}

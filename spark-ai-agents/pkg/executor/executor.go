// Package executor provides agent worker implementations
// inspired by Apache Spark's Executor abstraction.
package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seyi/dagens/pkg/agent"
	"github.com/google/uuid"
)

// Executor represents a worker that executes agent tasks.
// Inspired by Spark's Executor, it runs on worker nodes and executes tasks.
type Executor interface {
	// ID returns the unique identifier for this executor
	ID() string

	// IsAvailable returns whether this executor can accept new tasks
	IsAvailable() bool

	// ExecuteTask executes an agent task
	ExecuteTask(ctx context.Context, task *agent.AgentTask) (*agent.AgentOutput, error)

	// Partition returns the partition/locality identifier
	Partition() string

	// Node returns the node identifier for locality scheduling
	Node() string

	// Rack returns the rack/region identifier for locality scheduling
	Rack() string

	// GetMetrics returns executor metrics
	GetMetrics() *ExecutorMetrics

	// Shutdown gracefully shuts down the executor
	Shutdown() error
}

// ExecutorMetrics holds executor statistics
type ExecutorMetrics struct {
	TasksExecuted   int
	TasksFailed     int
	TotalDuration   time.Duration
	AvgDuration     time.Duration
	MemoryUsed      int64
	CPUUsage        float64
	LastHeartbeat   time.Time
}

// ExecutorImpl is a concrete implementation of Executor
type ExecutorImpl struct {
	id          string
	partition   string
	node        string
	rack        string
	maxTasks    int
	activeTasks int
	agentCache  map[string]agent.Agent
	mu          sync.RWMutex
	metrics     *ExecutorMetrics
	heartbeat   *Heartbeat
	ctx         context.Context
	cancel      context.CancelFunc
}

// ExecutorConfig holds configuration for creating an executor
type ExecutorConfig struct {
	Partition string
	Node      string
	Rack      string
	MaxTasks  int
}

// NewExecutor creates a new executor
func NewExecutor(config ExecutorConfig) *ExecutorImpl {
	ctx, cancel := context.WithCancel(context.Background())

	exec := &ExecutorImpl{
		id:         uuid.New().String(),
		partition:  config.Partition,
		node:       config.Node,
		rack:       config.Rack,
		maxTasks:   config.MaxTasks,
		agentCache: make(map[string]agent.Agent),
		metrics: &ExecutorMetrics{
			LastHeartbeat: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Start heartbeat
	exec.heartbeat = NewHeartbeat(exec)

	return exec
}

func (e *ExecutorImpl) ID() string         { return e.id }
func (e *ExecutorImpl) Partition() string  { return e.partition }
func (e *ExecutorImpl) Node() string       { return e.node }
func (e *ExecutorImpl) Rack() string       { return e.rack }

func (e *ExecutorImpl) IsAvailable() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.activeTasks < e.maxTasks
}

// ExecuteTask executes an agent task
func (e *ExecutorImpl) ExecuteTask(ctx context.Context, task *agent.AgentTask) (*agent.AgentOutput, error) {
	e.mu.Lock()
	e.activeTasks++
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.activeTasks--
		e.mu.Unlock()
	}()

	startTime := time.Now()

	// Get or create agent
	agent, err := e.getAgent(task.AgentID)
	if err != nil {
		e.mu.Lock()
		e.metrics.TasksFailed++
		e.mu.Unlock()
		return nil, fmt.Errorf("failed to get agent: %w", err)
	}

	// Execute with timeout
	execCtx := ctx
	if task.Input.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, task.Input.Timeout)
		defer cancel()
	}

	// Execute the agent task
	output, err := agent.Execute(execCtx, task.Input)

	duration := time.Since(startTime)

	// Update metrics
	e.mu.Lock()
	e.metrics.TotalDuration += duration
	if err != nil {
		e.metrics.TasksFailed++
	} else {
		e.metrics.TasksExecuted++
	}
	if e.metrics.TasksExecuted > 0 {
		e.metrics.AvgDuration = e.metrics.TotalDuration / time.Duration(e.metrics.TasksExecuted)
	}
	e.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("task execution failed: %w", err)
	}

	return output, nil
}

// getAgent retrieves an agent from cache or creates it
func (e *ExecutorImpl) getAgent(agentID string) (agent.Agent, error) {
	e.mu.RLock()
	ag, exists := e.agentCache[agentID]
	e.mu.RUnlock()

	if exists {
		return ag, nil
	}

	// In a real implementation, this would load agent definition from registry
	// For now, return error
	return nil, fmt.Errorf("agent %s not found in cache", agentID)
}

// RegisterAgent adds an agent to the executor's cache
func (e *ExecutorImpl) RegisterAgent(ag agent.Agent) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.agentCache[ag.ID()] = ag
}

func (e *ExecutorImpl) GetMetrics() *ExecutorMetrics {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Return a copy
	metrics := *e.metrics
	return &metrics
}

func (e *ExecutorImpl) Shutdown() error {
	e.cancel()
	e.heartbeat.Stop()
	return nil
}

// Heartbeat manages executor health reporting
type Heartbeat struct {
	executor *ExecutorImpl
	interval time.Duration
	ticker   *time.Ticker
	stopCh   chan struct{}
}

func NewHeartbeat(executor *ExecutorImpl) *Heartbeat {
	hb := &Heartbeat{
		executor: executor,
		interval: 5 * time.Second,
		stopCh:   make(chan struct{}),
	}

	go hb.run()
	return hb
}

func (hb *Heartbeat) run() {
	hb.ticker = time.NewTicker(hb.interval)
	defer hb.ticker.Stop()

	for {
		select {
		case <-hb.ticker.C:
			hb.executor.mu.Lock()
			hb.executor.metrics.LastHeartbeat = time.Now()
			hb.executor.mu.Unlock()
		case <-hb.stopCh:
			return
		}
	}
}

func (hb *Heartbeat) Stop() {
	close(hb.stopCh)
}

// ExecutorManager manages a pool of executors
type ExecutorManager interface {
	RegisterExecutor(executor Executor) error
	GetExecutor(id string) (Executor, error)
	GetAvailableExecutors() []Executor
	GetExecutorCount() int
	RemoveExecutor(id string) error
}

// ExecutorManagerImpl implements ExecutorManager
type ExecutorManagerImpl struct {
	executors map[string]Executor
	mu        sync.RWMutex
}

func NewExecutorManager() *ExecutorManagerImpl {
	return &ExecutorManagerImpl{
		executors: make(map[string]Executor),
	}
}

func (em *ExecutorManagerImpl) RegisterExecutor(executor Executor) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.executors[executor.ID()] = executor
	return nil
}

func (em *ExecutorManagerImpl) GetExecutor(id string) (Executor, error) {
	em.mu.RLock()
	defer em.mu.RUnlock()

	executor, exists := em.executors[id]
	if !exists {
		return nil, fmt.Errorf("executor %s not found", id)
	}

	return executor, nil
}

func (em *ExecutorManagerImpl) GetAvailableExecutors() []Executor {
	em.mu.RLock()
	defer em.mu.RUnlock()

	available := make([]Executor, 0)
	for _, executor := range em.executors {
		if executor.IsAvailable() {
			available = append(available, executor)
		}
	}

	return available
}

func (em *ExecutorManagerImpl) GetExecutorCount() int {
	em.mu.RLock()
	defer em.mu.RUnlock()
	return len(em.executors)
}

func (em *ExecutorManagerImpl) RemoveExecutor(id string) error {
	em.mu.Lock()
	defer em.mu.Unlock()

	executor, exists := em.executors[id]
	if !exists {
		return fmt.Errorf("executor %s not found", id)
	}

	executor.Shutdown()
	delete(em.executors, id)
	return nil
}

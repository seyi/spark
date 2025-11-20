// Package coordinator provides the main orchestration layer
// Ties together all components of the distributed AI agent system
package coordinator

import (
	"context"
	"fmt"

	"github.com/apache/spark/spark-ai-agents/pkg/agent"
	"github.com/apache/spark/spark-ai-agents/pkg/executor"
	"github.com/apache/spark/spark-ai-agents/pkg/scheduler"
	"github.com/apache/spark/spark-ai-agents/pkg/state"
	"github.com/apache/spark/spark-ai-agents/pkg/tools"
)

// SparkAgentCoordinator is the main entry point for distributed AI agent execution
// It orchestrates all components: scheduling, execution, state management
type SparkAgentCoordinator struct {
	dagScheduler    *scheduler.DAGScheduler
	taskScheduler   *scheduler.TaskSchedulerImpl
	executorManager executor.ExecutorManager
	stateManager    *state.StateManager
	toolRegistry    *tools.ToolRegistry
	agentRegistry   map[string]agent.Agent
}

// Config holds configuration for the coordinator
type Config struct {
	NumExecutors     int
	TasksPerExecutor int
	SchedulingMode   scheduler.SchedulingMode
	CheckpointDir    string
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		NumExecutors:     4,
		TasksPerExecutor: 2,
		SchedulingMode:   scheduler.FIFO,
		CheckpointDir:    "/tmp/spark-agents/checkpoints",
	}
}

// NewSparkAgentCoordinator creates a new coordinator
func NewSparkAgentCoordinator(config *Config) (*SparkAgentCoordinator, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Create executor manager
	executorMgr := executor.NewExecutorManager()

	// Create executors
	for i := 0; i < config.NumExecutors; i++ {
		exec := executor.NewExecutor(executor.ExecutorConfig{
			Partition: fmt.Sprintf("partition-%d", i),
			Node:      fmt.Sprintf("node-%d", i%2), // Simulate 2 nodes
			Rack:      fmt.Sprintf("rack-%d", i%4), // Simulate 4 racks
			MaxTasks:  config.TasksPerExecutor,
		})
		executorMgr.RegisterExecutor(exec)
	}

	// Create task scheduler
	taskScheduler := scheduler.NewTaskScheduler(executorMgr, config.SchedulingMode)

	// Create metadata store
	metadataStore := scheduler.NewMemoryMetadataStore()

	// Create DAG scheduler
	dagScheduler := scheduler.NewDAGScheduler(taskScheduler, metadataStore)
	taskScheduler.SetDAGScheduler(dagScheduler)

	// Create state manager
	checkpointMgr := state.NewMemoryCheckpointManager()
	stateMgr := state.NewStateManager(checkpointMgr)

	// Create tool registry
	toolReg := tools.NewToolRegistry()
	if err := tools.RegisterBuiltinTools(toolReg); err != nil {
		return nil, fmt.Errorf("failed to register builtin tools: %w", err)
	}

	coordinator := &SparkAgentCoordinator{
		dagScheduler:    dagScheduler,
		taskScheduler:   taskScheduler,
		executorManager: executorMgr,
		stateManager:    stateMgr,
		toolRegistry:    toolReg,
		agentRegistry:   make(map[string]agent.Agent),
	}

	return coordinator, nil
}

// RegisterAgent registers an agent for distributed execution
func (c *SparkAgentCoordinator) RegisterAgent(ag agent.Agent) error {
	c.agentRegistry[ag.ID()] = ag

	// Register agent with all executors
	executors := c.executorManager.GetAvailableExecutors()
	for _, exec := range executors {
		if execImpl, ok := exec.(*executor.ExecutorImpl); ok {
			execImpl.RegisterAgent(ag)
		}
	}

	return nil
}

// SubmitJob submits an agent workflow for distributed execution
func (c *SparkAgentCoordinator) SubmitJob(ctx context.Context, agentID string, input *agent.AgentInput) (string, error) {
	ag, exists := c.agentRegistry[agentID]
	if !exists {
		return "", fmt.Errorf("agent %s not registered", agentID)
	}

	// Submit to DAG scheduler
	jobID, err := c.dagScheduler.SubmitJob(ag, input)
	if err != nil {
		return "", fmt.Errorf("failed to submit job: %w", err)
	}

	return jobID, nil
}

// GetJobStatus retrieves the status of a job
func (c *SparkAgentCoordinator) GetJobStatus(jobID string) (*scheduler.Job, error) {
	return c.dagScheduler.GetJobStatus(jobID)
}

// WaitForJob waits for a job to complete
func (c *SparkAgentCoordinator) WaitForJob(ctx context.Context, jobID string) (*scheduler.Job, error) {
	// Simple polling implementation
	// In production, this would use event notifications
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			job, err := c.GetJobStatus(jobID)
			if err != nil {
				return nil, err
			}

			if job.State == scheduler.JobCompleted || job.State == scheduler.JobFailed {
				return job, nil
			}

			// Poll interval
			// time.Sleep(100 * time.Millisecond)
		}
	}
}

// GetToolRegistry returns the tool registry
func (c *SparkAgentCoordinator) GetToolRegistry() *tools.ToolRegistry {
	return c.toolRegistry
}

// GetStateManager returns the state manager
func (c *SparkAgentCoordinator) GetStateManager() *state.StateManager {
	return c.stateManager
}

// GetMetrics returns coordinator metrics
func (c *SparkAgentCoordinator) GetMetrics() *CoordinatorMetrics {
	schedulerMetrics := c.taskScheduler.GetMetrics()

	return &CoordinatorMetrics{
		RegisteredAgents:   len(c.agentRegistry),
		TotalExecutors:     schedulerMetrics.TotalExecutors,
		ActiveTasks:        schedulerMetrics.ActiveTasks,
		QueuedTasks:        schedulerMetrics.QueuedTasks,
		RegisteredTools:    len(c.toolRegistry.List()),
	}
}

// CoordinatorMetrics holds coordinator statistics
type CoordinatorMetrics struct {
	RegisteredAgents int
	TotalExecutors   int
	ActiveTasks      int
	QueuedTasks      int
	RegisteredTools  int
}

// Shutdown gracefully shuts down the coordinator
func (c *SparkAgentCoordinator) Shutdown() error {
	c.dagScheduler.Stop()
	c.taskScheduler.Stop()
	return nil
}

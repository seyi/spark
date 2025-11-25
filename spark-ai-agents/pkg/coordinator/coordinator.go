// Package coordinator provides the main orchestration layer
// Ties together all components of the distributed AI agent system
package coordinator

import (
	"context"
	"fmt"

	"github.com/seyi/dagens/pkg/agent"
	"github.com/seyi/dagens/pkg/events"
	"github.com/seyi/dagens/pkg/executor"
	"github.com/seyi/dagens/pkg/memory"
	"github.com/seyi/dagens/pkg/models"
	"github.com/seyi/dagens/pkg/scheduler"
	"github.com/seyi/dagens/pkg/sessions"
	"github.com/seyi/dagens/pkg/state"
	"github.com/seyi/dagens/pkg/tools"
)

// SparkAgentCoordinator is the main entry point for distributed AI agent execution
// It orchestrates all components: scheduling, execution, state management, models, sessions, memory, and events
type SparkAgentCoordinator struct {
	dagScheduler    *scheduler.DAGScheduler
	taskScheduler   *scheduler.TaskSchedulerImpl
	executorManager executor.ExecutorManager
	stateManager    *state.StateManager
	toolRegistry    *tools.ToolRegistry
	agentRegistry   map[string]agent.Agent

	// New ADK-inspired components
	modelRegistry   *models.ModelRegistry
	sessionManager  sessions.SessionManager
	memoryStore     memory.MemoryStore
	eventBus        events.EventBus
	eventStore      events.EventStore
}

// Config holds configuration for the coordinator
type Config struct {
	NumExecutors     int
	TasksPerExecutor int
	SchedulingMode   scheduler.SchedulingMode
	CheckpointDir    string

	// New feature configurations
	EnableSessions   bool
	EnableMemory     bool
	EnableEvents     bool
	MemoryBackend    string // "memory", "vector"
	SessionBackend   string // "memory", "distributed"
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		NumExecutors:     4,
		TasksPerExecutor: 2,
		SchedulingMode:   scheduler.FIFO,
		CheckpointDir:    "/tmp/spark-agents/checkpoints",

		// Enable all new features by default
		EnableSessions:  true,
		EnableMemory:    true,
		EnableEvents:    true,
		MemoryBackend:   "memory",
		SessionBackend:  "memory",
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

	// Create model registry with built-in models
	modelReg := models.NewModelRegistry()
	// Register mock models for development
	modelReg.Register(models.NewMockModelProvider("gpt-4", "openai"))
	modelReg.Register(models.NewMockModelProvider("claude-3-opus", "anthropic"))
	modelReg.Register(models.NewMockModelProvider("gemini-2.5-flash", "google"))
	modelReg.Register(models.NewMockModelProvider("llama-3.1", "ollama"))

	// Create session manager
	var sessionMgr sessions.SessionManager
	if config.EnableSessions {
		if config.SessionBackend == "memory" {
			sessionMgr = sessions.NewMemorySessionManager()
		} else {
			sessionMgr = sessions.NewMemorySessionManager() // Default to memory
		}
	}

	// Create memory store
	var memStore memory.MemoryStore
	if config.EnableMemory {
		if config.MemoryBackend == "vector" {
			// TODO: Implement vector memory with embeddings
			memStore = memory.NewInMemoryStore()
		} else {
			memStore = memory.NewInMemoryStore()
		}
	}

	// Create event system
	var eventBus events.EventBus
	var eventStore events.EventStore
	if config.EnableEvents {
		eventBus = events.NewMemoryEventBus()
		eventStore = events.NewMemoryEventStore()

		// Register event logger
		logger := events.NewEventLogger()
		eventBus.Subscribe(events.EventAgentStarted, logger.Log)
		eventBus.Subscribe(events.EventAgentCompleted, logger.Log)
		eventBus.Subscribe(events.EventJobSubmitted, logger.Log)
		eventBus.Subscribe(events.EventJobCompleted, logger.Log)
	}

	coordinator := &SparkAgentCoordinator{
		dagScheduler:    dagScheduler,
		taskScheduler:   taskScheduler,
		executorManager: executorMgr,
		stateManager:    stateMgr,
		toolRegistry:    toolReg,
		agentRegistry:   make(map[string]agent.Agent),

		// New components
		modelRegistry:   modelReg,
		sessionManager:  sessionMgr,
		memoryStore:     memStore,
		eventBus:        eventBus,
		eventStore:      eventStore,
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

	// Publish event
	if c.eventBus != nil {
		event := events.NewAgentEvent(events.EventAgentRegistered, ag.ID(), "", map[string]interface{}{
			"name": ag.Name(),
			"description": ag.Description(),
		})
		c.eventBus.Publish(event)
	}

	return nil
}

// SubmitJob submits an agent workflow for distributed execution
func (c *SparkAgentCoordinator) SubmitJob(ctx context.Context, agentID string, input *agent.AgentInput) (string, error) {
	ag, exists := c.agentRegistry[agentID]
	if !exists {
		return "", fmt.Errorf("agent %s not registered", agentID)
	}

	// Publish job submission event
	if c.eventBus != nil {
		event := events.NewAgentEvent(events.EventJobSubmitted, agentID, "", map[string]interface{}{
			"instruction": input.Instruction,
			"model": input.Model,
		})
		c.eventBus.Publish(event)
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

// GetModelRegistry returns the model registry
func (c *SparkAgentCoordinator) GetModelRegistry() *models.ModelRegistry {
	return c.modelRegistry
}

// GetSessionManager returns the session manager
func (c *SparkAgentCoordinator) GetSessionManager() sessions.SessionManager {
	return c.sessionManager
}

// GetMemoryStore returns the memory store
func (c *SparkAgentCoordinator) GetMemoryStore() memory.MemoryStore {
	return c.memoryStore
}

// GetEventBus returns the event bus
func (c *SparkAgentCoordinator) GetEventBus() events.EventBus {
	return c.eventBus
}

// GetEventStore returns the event store
func (c *SparkAgentCoordinator) GetEventStore() events.EventStore {
	return c.eventStore
}

// CreateSession creates a new session for an agent
func (c *SparkAgentCoordinator) CreateSession(ctx context.Context, agentID string) (*sessions.Session, error) {
	if c.sessionManager == nil {
		return nil, fmt.Errorf("session manager not enabled")
	}

	session, err := c.sessionManager.CreateSession(ctx, agentID)
	if err != nil {
		return nil, err
	}

	// Publish event
	if c.eventBus != nil {
		event := events.NewAgentEvent(events.EventSessionCreated, agentID, session.ID, nil)
		c.eventBus.Publish(event)
	}

	return session, nil
}

// StoreMemory stores a memory for an agent
func (c *SparkAgentCoordinator) StoreMemory(ctx context.Context, agentID, key string, value interface{}, metadata map[string]interface{}) error {
	if c.memoryStore == nil {
		return fmt.Errorf("memory store not enabled")
	}

	err := c.memoryStore.Store(ctx, agentID, key, value, metadata)
	if err != nil {
		return err
	}

	// Publish event
	if c.eventBus != nil {
		event := events.NewAgentEvent(events.EventMemoryStored, agentID, "", map[string]interface{}{
			"key": key,
		})
		c.eventBus.Publish(event)
	}

	return nil
}

// RetrieveMemory retrieves a memory for an agent
func (c *SparkAgentCoordinator) RetrieveMemory(ctx context.Context, agentID, key string) (*memory.MemoryEntry, error) {
	if c.memoryStore == nil {
		return nil, fmt.Errorf("memory store not enabled")
	}

	return c.memoryStore.Retrieve(ctx, agentID, key)
}

// GetMetrics returns coordinator metrics
func (c *SparkAgentCoordinator) GetMetrics() *CoordinatorMetrics {
	schedulerMetrics := c.taskScheduler.GetMetrics()

	modelCount := 0
	if c.modelRegistry != nil {
		modelCount = len(c.modelRegistry.List())
	}

	return &CoordinatorMetrics{
		RegisteredAgents:   len(c.agentRegistry),
		TotalExecutors:     schedulerMetrics.TotalExecutors,
		ActiveTasks:        schedulerMetrics.ActiveTasks,
		QueuedTasks:        schedulerMetrics.QueuedTasks,
		RegisteredTools:    len(c.toolRegistry.List()),
		RegisteredModels:   modelCount,
		ActiveSessions:     0, // TODO: Implement session counting
		StoredMemories:     0, // TODO: Implement memory counting
	}
}

// CoordinatorMetrics holds coordinator statistics
type CoordinatorMetrics struct {
	RegisteredAgents int
	TotalExecutors   int
	ActiveTasks      int
	QueuedTasks      int
	RegisteredTools  int
	RegisteredModels int
	ActiveSessions   int
	StoredMemories   int
}

// Shutdown gracefully shuts down the coordinator
func (c *SparkAgentCoordinator) Shutdown() error {
	c.dagScheduler.Stop()
	c.taskScheduler.Stop()

	// Shutdown event bus
	if c.eventBus != nil {
		c.eventBus.Close()
	}

	return nil
}

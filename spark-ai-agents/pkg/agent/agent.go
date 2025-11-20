// Package agent provides core abstractions for distributed AI agents
// inspired by Apache Spark's distributed computing model.
package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Agent represents a distributed AI agent with specific capabilities.
// Inspired by Spark's RDD abstraction, agents are composable units of computation.
type Agent interface {
	// ID returns the unique identifier for this agent
	ID() string

	// Name returns the human-readable name
	Name() string

	// Description returns what this agent does
	Description() string

	// Capabilities returns the list of tools/capabilities this agent has
	Capabilities() []string

	// Execute runs the agent with given input and context
	Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error)

	// Dependencies returns agents that must complete before this one
	Dependencies() []Agent

	// Partition returns the partition key for locality-aware scheduling
	Partition() string
}

// BaseAgent provides a concrete implementation of the Agent interface
type BaseAgent struct {
	id           string
	name         string
	description  string
	capabilities []string
	dependencies []Agent
	partition    string
	executor     AgentExecutor
}

// AgentInput represents input to an agent task
type AgentInput struct {
	TaskID      string
	Instruction string
	Context     map[string]interface{}
	Tools       []Tool
	Model       string
	MaxRetries  int
	Timeout     time.Duration
}

// AgentOutput represents the result of agent execution
type AgentOutput struct {
	TaskID       string
	Result       interface{}
	Metadata     map[string]interface{}
	Error        error
	ExecutionLog []ExecutionStep
	Metrics      *ExecutionMetrics
}

// ExecutionStep tracks a single step in agent execution (lineage tracking)
type ExecutionStep struct {
	StepID    string
	Timestamp time.Time
	Action    string
	Input     interface{}
	Output    interface{}
	Duration  time.Duration
}

// ExecutionMetrics tracks performance metrics
type ExecutionMetrics struct {
	StartTime      time.Time
	EndTime        time.Time
	Duration       time.Duration
	ToolCallCount  int
	TokensUsed     int
	RetryCount     int
	PartitionHits  int
}

// AgentExecutor is responsible for actually executing agent logic
type AgentExecutor interface {
	Execute(ctx context.Context, agent Agent, input *AgentInput) (*AgentOutput, error)
}

// Tool represents a capability that an agent can use
type Tool struct {
	Name        string
	Description string
	Schema      interface{}
	Handler     ToolHandler
}

// ToolHandler executes a tool with given parameters
type ToolHandler func(ctx context.Context, params map[string]interface{}) (interface{}, error)

// NewAgent creates a new agent with specified configuration
func NewAgent(config AgentConfig) *BaseAgent {
	return &BaseAgent{
		id:           uuid.New().String(),
		name:         config.Name,
		description:  config.Description,
		capabilities: config.Capabilities,
		dependencies: config.Dependencies,
		partition:    config.Partition,
		executor:     config.Executor,
	}
}

// AgentConfig holds configuration for creating an agent
type AgentConfig struct {
	Name         string
	Description  string
	Capabilities []string
	Dependencies []Agent
	Partition    string
	Executor     AgentExecutor
}

// Interface implementations for BaseAgent
func (a *BaseAgent) ID() string           { return a.id }
func (a *BaseAgent) Name() string         { return a.name }
func (a *BaseAgent) Description() string  { return a.description }
func (a *BaseAgent) Capabilities() []string { return a.capabilities }
func (a *BaseAgent) Dependencies() []Agent { return a.dependencies }
func (a *BaseAgent) Partition() string    { return a.partition }

func (a *BaseAgent) Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
	if a.executor == nil {
		return nil, fmt.Errorf("no executor configured for agent %s", a.name)
	}
	return a.executor.Execute(ctx, a, input)
}

// AgentTask represents a unit of work to be executed by an agent
// Similar to Spark's Task abstraction
type AgentTask struct {
	ID           string
	AgentID      string
	Input        *AgentInput
	Stage        int
	Attempt      int
	PreferredLoc string // Locality preference
	State        TaskState
	CreatedAt    time.Time
	StartedAt    time.Time
	CompletedAt  time.Time
	Output       *AgentOutput
}

// TaskState represents the execution state of a task
type TaskState int

const (
	TaskPending TaskState = iota
	TaskScheduled
	TaskRunning
	TaskCompleted
	TaskFailed
	TaskRetrying
)

func (s TaskState) String() string {
	return [...]string{
		"PENDING",
		"SCHEDULED",
		"RUNNING",
		"COMPLETED",
		"FAILED",
		"RETRYING",
	}[s]
}

// AgentDAG represents a directed acyclic graph of agent tasks
// Inspired by Spark's DAG scheduling
type AgentDAG struct {
	ID     string
	Name   string
	Stages []*AgentStage
	Root   Agent
}

// AgentStage groups tasks that can be executed in parallel
// Similar to Spark's Stage abstraction
type AgentStage struct {
	ID           int
	Tasks        []*AgentTask
	Dependencies []*AgentStage
	IsShuffleMap bool // Indicates if stage produces intermediate results
}

// NewAgentDAG creates a DAG from a root agent
func NewAgentDAG(root Agent, input *AgentInput) (*AgentDAG, error) {
	dag := &AgentDAG{
		ID:     uuid.New().String(),
		Name:   fmt.Sprintf("DAG-%s", root.Name()),
		Root:   root,
		Stages: make([]*AgentStage, 0),
	}

	// Build stages from agent dependencies
	if err := dag.buildStages(root, input); err != nil {
		return nil, err
	}

	return dag, nil
}

// buildStages recursively builds stages from agent dependencies
func (d *AgentDAG) buildStages(agent Agent, input *AgentInput) error {
	visited := make(map[string]bool)
	stageMap := make(map[int][]*AgentTask)

	var buildRecursive func(Agent, int) error
	buildRecursive = func(a Agent, depth int) error {
		if visited[a.ID()] {
			return nil
		}
		visited[a.ID()] = true

		// Process dependencies first (deeper stages)
		for _, dep := range a.Dependencies() {
			if err := buildRecursive(dep, depth+1); err != nil {
				return err
			}
		}

		// Create task for this agent
		task := &AgentTask{
			ID:           uuid.New().String(),
			AgentID:      a.ID(),
			Input:        input,
			Stage:        depth,
			Attempt:      0,
			PreferredLoc: a.Partition(),
			State:        TaskPending,
			CreatedAt:    time.Now(),
		}

		stageMap[depth] = append(stageMap[depth], task)
		return nil
	}

	if err := buildRecursive(agent, 0); err != nil {
		return err
	}

	// Convert stageMap to ordered stages
	maxDepth := 0
	for depth := range stageMap {
		if depth > maxDepth {
			maxDepth = depth
		}
	}

	for i := maxDepth; i >= 0; i-- {
		if tasks, ok := stageMap[i]; ok {
			stage := &AgentStage{
				ID:           i,
				Tasks:        tasks,
				Dependencies: make([]*AgentStage, 0),
				IsShuffleMap: i > 0, // Non-final stages are shuffle map stages
			}
			d.Stages = append(d.Stages, stage)
		}
	}

	return nil
}

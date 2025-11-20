// Package agent provides core abstractions for distributed AI agents
// inspired by Apache Spark's distributed computing model.
package agent

import (
	"context"
	"fmt"
	"sync"
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
	outputKey    string // ADK-compatible: auto-store result to this key

	// Agent hierarchy support (ADK-compatible)
	parent    Agent        // Parent agent reference
	subAgents []Agent      // Child agents
	mu        sync.RWMutex // Thread-safe access to hierarchy
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
	agent := &BaseAgent{
		id:           uuid.New().String(),
		name:         config.Name,
		description:  config.Description,
		capabilities: config.Capabilities,
		dependencies: config.Dependencies,
		partition:    config.Partition,
		executor:     config.Executor,
		outputKey:    config.OutputKey,
		subAgents:    make([]Agent, 0),
	}

	// Set up sub-agent parent references (ADK-compatible)
	if len(config.SubAgents) > 0 {
		for _, subAgent := range config.SubAgents {
			if err := agent.AddSubAgent(subAgent); err != nil {
				// Log error but don't fail agent creation
				fmt.Printf("Warning: failed to add sub-agent %s: %v\n", subAgent.Name(), err)
			}
		}
	}

	return agent
}

// AgentConfig holds configuration for creating an agent
type AgentConfig struct {
	Name         string
	Description  string
	Capabilities []string
	Dependencies []Agent
	Partition    string
	Executor     AgentExecutor
	SubAgents    []Agent // Child agents (ADK-compatible)
	OutputKey    string  // ADK-compatible: auto-store result to this key in context
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

	// ADK-compatible: Interpolate templates in instruction before execution
	if input.Instruction != "" && HasTemplates(input.Instruction) {
		interpolated, err := InterpolateInstruction(input.Instruction, input.Context)
		if err != nil {
			// Log warning but continue with interpolated result
			// This matches ADK's lenient behavior
			fmt.Printf("Warning: template interpolation for agent %s: %v\n", a.name, err)
		}
		input.Instruction = interpolated
	}

	// Execute the agent
	output, err := a.executor.Execute(ctx, a, input)
	if err != nil {
		return nil, err
	}

	// ADK-compatible: Auto-store result to OutputKey if specified
	if a.outputKey != "" && output != nil {
		// Ensure Context map exists
		if input.Context == nil {
			input.Context = make(map[string]interface{})
		}

		// Store result to the specified key
		// This enables subsequent agents to access the result by key
		input.Context[a.outputKey] = output.Result

		// Also store in output metadata for visibility
		if output.Metadata == nil {
			output.Metadata = make(map[string]interface{})
		}
		output.Metadata["output_key"] = a.outputKey
		output.Metadata["stored_to_context"] = true
	}

	return output, nil
}

// Agent Hierarchy Methods (ADK-compatible)

// Parent returns the parent agent, or nil if this is a root agent
func (a *BaseAgent) Parent() Agent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.parent
}

// SubAgents returns the list of child agents
func (a *BaseAgent) SubAgents() []Agent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	// Return a copy to prevent external modification
	agents := make([]Agent, len(a.subAgents))
	copy(agents, a.subAgents)
	return agents
}

// AddSubAgent adds a child agent and sets this agent as its parent
// Returns error if the child already has a parent (single parent rule)
func (a *BaseAgent) AddSubAgent(child Agent) error {
	if child == nil {
		return fmt.Errorf("cannot add nil sub-agent")
	}

	// Check if child is a BaseAgent (so we can set parent)
	baseChild, ok := child.(*BaseAgent)
	if !ok {
		return fmt.Errorf("sub-agent must be *BaseAgent to support hierarchy")
	}

	// Check single parent rule
	baseChild.mu.Lock()
	defer baseChild.mu.Unlock()

	if baseChild.parent != nil {
		return fmt.Errorf("agent %s already has a parent %s (single parent rule)",
			child.Name(), baseChild.parent.Name())
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Set parent reference
	baseChild.parent = a

	// Add to sub-agents list
	a.subAgents = append(a.subAgents, child)

	return nil
}

// RemoveSubAgent removes a child agent
func (a *BaseAgent) RemoveSubAgent(child Agent) error {
	if child == nil {
		return fmt.Errorf("cannot remove nil sub-agent")
	}

	baseChild, ok := child.(*BaseAgent)
	if !ok {
		return fmt.Errorf("sub-agent must be *BaseAgent")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Find and remove from sub-agents list
	found := false
	for i, sub := range a.subAgents {
		if sub.ID() == child.ID() {
			a.subAgents = append(a.subAgents[:i], a.subAgents[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("agent %s is not a sub-agent of %s", child.Name(), a.name)
	}

	// Clear parent reference
	baseChild.mu.Lock()
	baseChild.parent = nil
	baseChild.mu.Unlock()

	return nil
}

// FindAgent searches for a descendant agent by name
// Returns the agent if found, or error if not found
func (a *BaseAgent) FindAgent(name string) (Agent, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Check direct children
	for _, child := range a.subAgents {
		if child.Name() == name {
			return child, nil
		}
	}

	// Recursively search descendants
	for _, child := range a.subAgents {
		if baseChild, ok := child.(*BaseAgent); ok {
			if found, err := baseChild.FindAgent(name); err == nil {
				return found, nil
			}
		}
	}

	return nil, fmt.Errorf("agent %s not found in hierarchy", name)
}

// FindAgentByID searches for a descendant agent by ID
func (a *BaseAgent) FindAgentByID(id string) (Agent, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Check direct children
	for _, child := range a.subAgents {
		if child.ID() == id {
			return child, nil
		}
	}

	// Recursively search descendants
	for _, child := range a.subAgents {
		if baseChild, ok := child.(*BaseAgent); ok {
			if found, err := baseChild.FindAgentByID(id); err == nil {
				return found, nil
			}
		}
	}

	return nil, fmt.Errorf("agent with ID %s not found in hierarchy", id)
}

// GetRoot returns the root agent in the hierarchy
func (a *BaseAgent) GetRoot() Agent {
	a.mu.RLock()
	current := Agent(a)
	a.mu.RUnlock()

	for {
		if baseAgent, ok := current.(*BaseAgent); ok {
			parent := baseAgent.Parent()
			if parent == nil {
				return current
			}
			current = parent
		} else {
			return current
		}
	}
}

// GetPath returns the path from root to this agent
func (a *BaseAgent) GetPath() []Agent {
	path := []Agent{}
	current := Agent(a)

	for current != nil {
		path = append([]Agent{current}, path...)
		if baseAgent, ok := current.(*BaseAgent); ok {
			current = baseAgent.Parent()
		} else {
			break
		}
	}

	return path
}

// GetDepth returns the depth of this agent in the hierarchy (0 for root)
func (a *BaseAgent) GetDepth() int {
	depth := 0
	current := Agent(a)

	for current != nil {
		if baseAgent, ok := current.(*BaseAgent); ok {
			parent := baseAgent.Parent()
			if parent == nil {
				break
			}
			depth++
			current = parent
		} else {
			break
		}
	}

	return depth
}

// AddDependency adds a dependency agent (helper method)
func (a *BaseAgent) AddDependency(dep Agent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dependencies = append(a.dependencies, dep)
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

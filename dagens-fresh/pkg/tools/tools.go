// Package tools provides composable tool implementations for agents
// Inspired by the tool-based architecture in Zen MCP and ADK
package tools

import (
	"context"
	"fmt"
)

// ToolRegistry manages available tools for agents
type ToolRegistry struct {
	tools map[string]*ToolDefinition
}

// ToolDefinition defines a tool that agents can use
type ToolDefinition struct {
	Name        string
	Description string
	Schema      *ToolSchema
	Handler     ToolHandler
	Enabled     bool
}

// ToolSchema defines the input/output schema for a tool
type ToolSchema struct {
	InputSchema  map[string]interface{}
	OutputSchema map[string]interface{}
}

// ToolHandler executes tool logic
type ToolHandler func(ctx context.Context, params map[string]interface{}) (interface{}, error)

// NewToolRegistry creates a new tool registry
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools: make(map[string]*ToolDefinition),
	}
}

// Register adds a tool to the registry
func (tr *ToolRegistry) Register(tool *ToolDefinition) error {
	if _, exists := tr.tools[tool.Name]; exists {
		return fmt.Errorf("tool %s already registered", tool.Name)
	}

	tr.tools[tool.Name] = tool
	return nil
}

// Get retrieves a tool by name
func (tr *ToolRegistry) Get(name string) (*ToolDefinition, error) {
	tool, exists := tr.tools[name]
	if !exists {
		return nil, fmt.Errorf("tool %s not found", name)
	}

	if !tool.Enabled {
		return nil, fmt.Errorf("tool %s is disabled", name)
	}

	return tool, nil
}

// List returns all enabled tools
func (tr *ToolRegistry) List() []*ToolDefinition {
	tools := make([]*ToolDefinition, 0)
	for _, tool := range tr.tools {
		if tool.Enabled {
			tools = append(tools, tool)
		}
	}
	return tools
}

// Execute runs a tool with given parameters
func (tr *ToolRegistry) Execute(ctx context.Context, name string, params map[string]interface{}) (interface{}, error) {
	tool, err := tr.Get(name)
	if err != nil {
		return nil, err
	}

	return tool.Handler(ctx, params)
}

// Built-in tools inspired by Zen MCP

// SearchTool performs distributed search across agent knowledge
func SearchTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "search",
		Description: "Search across distributed agent knowledge base",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"query": "string",
				"limit": "integer",
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			query, ok := params["query"].(string)
			if !ok {
				return nil, fmt.Errorf("query parameter required")
			}

			// In a real implementation, this would search distributed state
			return map[string]interface{}{
				"query":   query,
				"results": []interface{}{},
			}, nil
		},
		Enabled: true,
	}
}

// ConsensusToolReaches consensus across multiple agents
func ConsensusTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "consensus",
		Description: "Reach consensus across multiple distributed agents",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"agents":   "array",
				"question": "string",
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			question, ok := params["question"].(string)
			if !ok {
				return nil, fmt.Errorf("question parameter required")
			}

			// In a real implementation, this would coordinate multiple agents
			return map[string]interface{}{
				"question":  question,
				"consensus": "pending",
				"votes":     []interface{}{},
			}, nil
		},
		Enabled: true,
	}
}

// PlannerTool breaks down complex tasks
func PlannerTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "planner",
		Description: "Break down complex projects into structured plans",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"task":        "string",
				"constraints": "object",
			},
		},
		Handler: func(ctx context.Context, params map[string]interface{}) (interface{}, error) {
			task, ok := params["task"].(string)
			if !ok {
				return nil, fmt.Errorf("task parameter required")
			}

			// In a real implementation, this would use AI planning
			return map[string]interface{}{
				"task":  task,
				"steps": []interface{}{},
			}, nil
		},
		Enabled: true,
	}
}

// RegisterBuiltinTools registers all built-in tools
func RegisterBuiltinTools(registry *ToolRegistry) error {
	tools := []*ToolDefinition{
		SearchTool(),
		ConsensusTool(),
		PlannerTool(),
	}

	for _, tool := range tools {
		if err := registry.Register(tool); err != nil {
			return err
		}
	}

	return nil
}

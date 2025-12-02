package tools

import (
	"context"
	"fmt"

	"github.com/seyi/dagens/pkg/agent"
	"github.com/seyi/dagens/pkg/events"
)

// TransferToAgentTool provides LLM-driven agent delegation
// Inspired by ADK's transfer_to_agent function call pattern
// Designed for Spark distributed environments with partition-aware transfers
func TransferToAgentTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "transfer_to_agent",
		Description: "Transfer execution to another agent by name. Use when the current agent is not the best suited to handle the request, and another specialized agent should take over.",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"agent_name": map[string]interface{}{
						"type":        "string",
						"description": "Name of the target agent to transfer execution to",
					},
					"reason": map[string]interface{}{
						"type":        "string",
						"description": "Optional reason for the transfer (for debugging and observability)",
					},
				},
				"required": []string{"agent_name"},
			},
		},
		Handler: transferToAgentHandler,
		Enabled: true,
	}
}

// transferToAgentHandler handles the transfer_to_agent tool call
// Returns a signal that transfer is requested with the target agent name
func transferToAgentHandler(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Extract agent name
	agentName, ok := params["agent_name"].(string)
	if !ok {
		return nil, fmt.Errorf("agent_name parameter is required and must be a string")
	}

	if agentName == "" {
		return nil, fmt.Errorf("agent_name cannot be empty")
	}

	// Extract optional reason
	reason, _ := params["reason"].(string)

	// Get invocation context from params (injected by AutoFlow)
	var invocationCtx *agent.InvocationContext
	if ic, ok := params["__invocation_context__"].(*agent.InvocationContext); ok {
		invocationCtx = ic
	}

	// Get event bus from params (injected by AutoFlow)
	var eventBus events.EventBus
	if eb, ok := params["__event_bus__"].(events.EventBus); ok {
		eventBus = eb
	}

	// Publish transfer request event for observability
	if eventBus != nil && invocationCtx != nil {
		currentAgent := invocationCtx.CurrentAgent
		err := eventBus.Publish(events.NewTransferRequestEvent(
			currentAgent.Name(),
			agentName,
			reason,
			currentAgent.Partition(),
		))
		if err != nil {
			// Log error but don't fail the transfer request
			fmt.Printf("Warning: failed to publish transfer request event: %v\n", err)
		}
	}

	// Return transfer signal
	// The AutoFlow interceptor will handle the actual transfer
	return map[string]interface{}{
		"transfer_requested": true,
		"target_agent":       agentName,
		"reason":             reason,
		"timestamp":          ctx.Value("timestamp"),
	}, nil
}

// TransferScopeValidatorTool provides scope validation for transfers
// Can be used to check if a transfer is allowed before attempting it
func TransferScopeValidatorTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "validate_transfer_scope",
		Description: "Validate if a transfer to a target agent is allowed based on transfer scope rules",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"agent_name": map[string]interface{}{
						"type":        "string",
						"description": "Name of the target agent",
					},
				},
				"required": []string{"agent_name"},
			},
		},
		Handler: validateTransferScopeHandler,
		Enabled: true,
	}
}

// validateTransferScopeHandler checks if a transfer is allowed
func validateTransferScopeHandler(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	agentName, ok := params["agent_name"].(string)
	if !ok {
		return nil, fmt.Errorf("agent_name parameter is required")
	}

	// Get invocation context
	invocationCtx, ok := params["__invocation_context__"].(*agent.InvocationContext)
	if !ok {
		return map[string]interface{}{
			"valid":   false,
			"reason":  "No invocation context available",
			"agent":   agentName,
		}, nil
	}

	// Get transfer scope config
	scope, _ := params["__transfer_scope__"].(string)

	// Try to find the agent
	currentAgent := invocationCtx.CurrentAgent
	if baseAgent, ok := currentAgent.(*agent.BaseAgent); ok {
		// Check if agent exists in hierarchy
		root := baseAgent.GetRoot()
		if rootBase, ok := root.(*agent.BaseAgent); ok {
			targetAgent, err := rootBase.FindAgent(agentName)
			if err != nil {
				return map[string]interface{}{
					"valid":  false,
					"reason": fmt.Sprintf("Agent '%s' not found in hierarchy", agentName),
					"agent":  agentName,
				}, nil
			}

			// Validate based on scope
			// (In AutoFlow implementation, this will be more sophisticated)
			return map[string]interface{}{
				"valid":           true,
				"agent":           agentName,
				"agent_id":        targetAgent.ID(),
				"partition":       targetAgent.Partition(),
				"description":     targetAgent.Description(),
				"transfer_scope":  scope,
			}, nil
		}
	}

	return map[string]interface{}{
		"valid":  false,
		"reason": "Unable to validate scope",
		"agent":  agentName,
	}, nil
}

// GetAvailableAgentsTool lists agents available for transfer
// Helps LLM make informed delegation decisions
func GetAvailableAgentsTool() *ToolDefinition {
	return &ToolDefinition{
		Name:        "get_available_agents",
		Description: "Get a list of agents available for transfer in the current scope. Use this to understand what specialized agents are available before deciding to transfer.",
		Schema: &ToolSchema{
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		Handler: getAvailableAgentsHandler,
		Enabled: true,
	}
}

// getAvailableAgentsHandler returns list of available agents
func getAvailableAgentsHandler(ctx context.Context, params map[string]interface{}) (interface{}, error) {
	// Get invocation context
	invocationCtx, ok := params["__invocation_context__"].(*agent.InvocationContext)
	if !ok {
		return map[string]interface{}{
			"agents": []interface{}{},
			"count":  0,
			"error":  "No invocation context available",
		}, nil
	}

	currentAgent := invocationCtx.CurrentAgent
	availableAgents := []map[string]interface{}{}

	// Get sub-agents if available
	if baseAgent, ok := currentAgent.(*agent.BaseAgent); ok {
		subAgents := baseAgent.SubAgents()
		for _, subAgent := range subAgents {
			availableAgents = append(availableAgents, map[string]interface{}{
				"name":         subAgent.Name(),
				"description":  subAgent.Description(),
				"capabilities": subAgent.Capabilities(),
				"partition":    subAgent.Partition(),
			})
		}

		// Also get sibling agents if parent exists
		parent := baseAgent.Parent()
		if parent != nil {
			if parentBase, ok := parent.(*agent.BaseAgent); ok {
				siblings := parentBase.SubAgents()
				for _, sibling := range siblings {
					if sibling.ID() != currentAgent.ID() {
						availableAgents = append(availableAgents, map[string]interface{}{
							"name":         sibling.Name(),
							"description":  sibling.Description(),
							"capabilities": sibling.Capabilities(),
							"partition":    sibling.Partition(),
							"relationship": "sibling",
						})
					}
				}
			}
		}
	}

	return map[string]interface{}{
		"agents":        availableAgents,
		"count":         len(availableAgents),
		"current_agent": currentAgent.Name(),
	}, nil
}

// RegisterTransferTools registers all transfer-related tools
func RegisterTransferTools(registry *ToolRegistry) error {
	transferTools := []*ToolDefinition{
		TransferToAgentTool(),
		TransferScopeValidatorTool(),
		GetAvailableAgentsTool(),
	}

	for _, tool := range transferTools {
		if err := registry.Register(tool); err != nil {
			return fmt.Errorf("failed to register transfer tool %s: %w", tool.Name, err)
		}
	}

	return nil
}

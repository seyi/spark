# Agent Hierarchy: Parent-Agent and Sub-Agents

## Overview

Agent hierarchy enables building tree-structured multi-agent systems where parent agents coordinate and delegate tasks to specialized child agents (sub-agents). This feature provides **full ADK compatibility** while maintaining our distributed Spark infrastructure.

### Key Benefits

✅ **Modularity** - Organize agents into logical hierarchies
✅ **Specialization** - Delegate to specialist agents
✅ **Dynamic Routing** - LLM-driven task delegation
✅ **Agent Reusability** - Same agent in multiple contexts
✅ **ADK Compatible** - Works like Google ADK Python
✅ **Distributed** - Runs on our Spark-inspired DAG infrastructure

---

## Core Concepts

### Parent-Child Relationships

```
         Coordinator (Parent)
              |
     ┌────────┼────────┐
     ↓        ↓        ↓
  Greeter  TaskDoer  Analyzer (Children/Sub-Agents)
```

- **Parent Agent**: Coordinates and delegates to children
- **Sub-Agents**: Specialized agents that perform specific tasks
- **Single Parent Rule**: Each agent can have only one parent (like ADK)

### Navigation

- `agent.Parent()` - Navigate up the tree
- `agent.SubAgents()` - Get list of children
- `agent.FindAgent(name)` - Search descendants by name
- `agent.GetRoot()` - Find the root of the hierarchy
- `agent.GetPath()` - Get path from root to this agent
- `agent.GetDepth()` - Get depth in hierarchy (0 = root)

---

## Creating Hierarchies

### Method 1: Via Configuration

```go
import (
    "github.com/apache/spark/spark-ai-agents/pkg/agent"
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
)

// Create child agents
greeter := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "greeter",
    Instruction: "Greet users warmly",
}, modelProvider, toolRegistry)

taskDoer := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "task-doer",
    Instruction: "Execute tasks efficiently",
}, modelProvider, toolRegistry)

// Create parent with sub-agents
coordinator := agent.NewAgent(agent.AgentConfig{
    Name:        "coordinator",
    Description: "Coordinates tasks between specialist agents",
    SubAgents:   []agent.Agent{greeter, taskDoer},
    Executor:    coordinatorExecutor,
})

// Parent references are automatically set!
// greeter.Parent() == coordinator
// taskDoer.Parent() == coordinator
```

### Method 2: Dynamic Addition

```go
// Create agents separately
parent := agents.NewLlmAgent(...)
child := agents.NewLlmAgent(...)

// Add child dynamically
err := parent.AddSubAgent(child)
if err != nil {
    log.Fatal(err)
}

// Now child.Parent() == parent
```

### Method 3: Multi-Level Hierarchy

```go
// Create grandchildren
analyzer := agents.NewLlmAgent(...)
reporter := agents.NewLlmAgent(...)

// Create child with its own sub-agents
dataProcessor := agent.NewAgent(agent.AgentConfig{
    Name:      "data-processor",
    SubAgents: []agent.Agent{analyzer, reporter},
})

// Create root with child
coordinator := agent.NewAgent(agent.AgentConfig{
    Name:      "coordinator",
    SubAgents: []agent.Agent{dataProcessor},
})

// Result:
//   coordinator
//       └─ data-processor
//            ├─ analyzer
//            └─ reporter
```

---

## Single Parent Rule (ADK-Compatible)

Like ADK, we enforce the single parent rule:

```go
parent1 := agents.NewLlmAgent(...)
parent2 := agents.NewLlmAgent(...)
child := agents.NewLlmAgent(...)

// Add to first parent - OK
parent1.AddSubAgent(child)

// Try to add to second parent - ERROR
err := parent2.AddSubAgent(child)
// Error: "agent child already has a parent parent1 (single parent rule)"
```

---

## Navigation Methods

### Find Agents

```go
// Find by name (searches all descendants)
specialist, err := coordinator.FindAgent("data-processor")
if err != nil {
    log.Fatal(err)
}

// Find by ID
agent, err := coordinator.FindAgentByID("agent-uuid-123")
```

### Navigate Up

```go
// Get parent
parent := child.Parent()
if parent != nil {
    fmt.Printf("My parent is: %s\n", parent.Name())
}

// Get root
root := child.GetRoot()
fmt.Printf("Root agent: %s\n", root.Name())
```

### Get Path

```go
// Get path from root to this agent
path := grandchild.GetPath()
for _, ag := range path {
    fmt.Printf(" → %s", ag.Name())
}
// Output: → coordinator → data-processor → analyzer
```

### Get Depth

```go
depth := agent.GetDepth()
fmt.Printf("Agent is at depth %d\n", depth)
// Root: 0, Children: 1, Grandchildren: 2, etc.
```

### List Children

```go
subAgents := parent.SubAgents()
fmt.Printf("Parent has %d children:\n", len(subAgents))
for _, child := range subAgents {
    fmt.Printf("  - %s: %s\n", child.Name(), child.Description())
}
```

---

## Agent-as-Tool Pattern

Wrap agents as tools so they can be called by other agents:

### Basic Wrapping

```go
import "github.com/apache/spark/spark-ai-agents/pkg/agents"

// Wrap an agent as a tool
agentTool := agents.WrapAgentAsTool(specialistAgent)

// Add to tool registry
toolRegistry.RegisterTool(agentTool)

// Now LLM agents can call it like any tool
// Tool name: "call_specialist"
```

### Wrap All Sub-Agents

```go
// Automatically wrap all children as tools
toolRegistry := agents.WrapSubAgentsAsTools(parent)

// Each child becomes a callable tool:
// - call_greeter
// - call_task-doer
// - call_analyzer
```

### Transfer-to-Agent Tool

The `transfer_to_agent` tool enables LLM-driven delegation:

```go
// Create transfer tool
transferTool := agents.CreateTransferTool(parent)
toolRegistry.RegisterTool(transferTool)

// LLM can now call:
// transfer_to_agent(agent_name="specialist", instruction="analyze this")
```

### Hierarchical Tool Registry

Get everything in one call:

```go
// Creates registry with:
// 1. transfer_to_agent tool
// 2. All sub-agents as tools
// 3. Any additional tools
toolRegistry := agents.CreateHierarchicalToolRegistry(
    parent,
    customTool1,
    customTool2,
)

// Use with LLM agent
coordinator := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "coordinator",
    Instruction: "Coordinate tasks. Available agents: greeter, task-doer",
    Tools:       toolRegistry.ListToolNames(),
}, modelProvider, toolRegistry)
```

---

## LLM-Driven Delegation

### Example: Customer Service System

```go
// Create specialist agents
greeter := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "greeter",
    Instruction: "Greet customers warmly and professionally",
}, modelProvider, nil)

supportAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "support-specialist",
    Instruction: "Handle technical support questions",
}, modelProvider, nil)

salesAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "sales-specialist",
    Instruction: "Answer product and pricing questions",
}, modelProvider, nil)

// Create coordinator with hierarchical tools
coordinator := agent.NewAgent(agent.AgentConfig{
    Name:      "customer-service",
    SubAgents: []agent.Agent{greeter, supportAgent, salesAgent},
})

// Set up tools for coordinator
toolRegistry := agents.CreateHierarchicalToolRegistry(coordinator)

// Create coordinator LLM agent
coordinatorLLM := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name: "coordinator",
    Instruction: `You are a customer service coordinator.
Available specialist agents:
- greeter: For welcoming customers
- support-specialist: For technical issues
- sales-specialist: For product/pricing questions

Use transfer_to_agent to delegate to the right specialist.`,
    Tools: toolRegistry.ListToolNames(),
}, modelProvider, toolRegistry)

// Execute - LLM will automatically delegate!
output, _ := coordinatorLLM.Execute(ctx, &agent.AgentInput{
    Instruction: "I have a technical problem with my account",
})
// LLM generates: transfer_to_agent(agent_name="support-specialist", ...)
// Support specialist handles the request
```

### How LLM Delegation Works

1. **User sends request** to coordinator
2. **Coordinator LLM analyzes** the request
3. **LLM generates tool call**: `transfer_to_agent(agent_name="specialist", instruction="...")`
4. **Framework finds** the specialist agent
5. **Specialist executes** the task
6. **Result returned** to user

---

## Custom Tool Builder

For advanced scenarios, use the fluent builder:

```go
tool := agents.NewAgentToolBuilder(specialistAgent).
    WithName("custom_specialist").
    WithDescription("Custom description for LLM").
    WithSchema(customSchema).
    WithPreProcess(func(params map[string]interface{}) (*agent.AgentInput, error) {
        // Transform tool params into agent input
        return &agent.AgentInput{
            Instruction: params["task"].(string),
            Context: map[string]interface{}{
                "priority": params["priority"],
            },
        }, nil
    }).
    WithPostProcess(func(output *agent.AgentOutput) (interface{}, error) {
        // Transform agent output into tool result
        return map[string]interface{}{
            "status": "success",
            "data":   output.Result,
        }, nil
    }).
    Build()
```

---

## Shared Session State

All agents in the same invocation share session context (like ADK):

```go
// Create session
session, _ := sessionManager.CreateSession(ctx, coordinator.ID())

// Set shared state
sessionManager.UpdateContext(ctx, session.ID, map[string]interface{}{
    "user_id":      "12345",
    "preferences":  "detailed",
    "language":     "en",
})

// Execute parent
output, _ := coordinator.Execute(ctx, &agent.AgentInput{
    Instruction: "Process user request",
    Context:     session.Context, // Pass shared context
})

// All sub-agents invoked during execution will see:
// - user_id: "12345"
// - preferences: "detailed"
// - language: "en"
```

---

## Integration with Workflow Agents

Hierarchy works seamlessly with workflow agents:

### Sequential with Sub-Agents

```go
// Create agents
collector := agents.NewLlmAgent(...)
analyzer := agents.NewLlmAgent(...)
reporter := agents.NewLlmAgent(...)

// Create sequential workflow
pipeline := agents.NewSequential().
    Add(collector).
    Add(analyzer).
    Add(reporter).
    WithPassOutput(true).
    Build()

// Use as sub-agent
coordinator := agent.NewAgent(agent.AgentConfig{
    Name:      "coordinator",
    SubAgents: []agent.Agent{pipeline},
})
```

### Parallel with Delegation

```go
// Create parallel agent
ensemble := agents.NewParallel(agents.VoteAggregation).
    Add(model1).
    Add(model2).
    Add(model3).
    Build()

// Wrap as tool
ensembleTool := agents.WrapAgentAsTool(ensemble)

// Coordinator can delegate to ensemble
coordinator := agents.NewLlmAgent(agents.LlmAgentConfig{
    Tools: []string{"call_ensemble"},
}, modelProvider, toolRegistryWithEnsemble)
```

---

## Complete Example: Multi-Tier Support System

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/apache/spark/spark-ai-agents/pkg/agent"
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
    "github.com/apache/spark/spark-ai-agents/pkg/model"
)

func main() {
    ctx := context.Background()
    modelProvider := model.NewOpenAIProvider("gpt-4", "your-key")

    // Tier 1: Front-line agents
    greeter := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name:        "greeter",
        Instruction: "Greet customers warmly",
    }, modelProvider, nil)

    triageAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name:        "triage",
        Instruction: "Assess customer needs and categorize requests",
    }, modelProvider, nil)

    // Tier 2: Specialist agents
    technicalSupport := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name:        "tech-support",
        Instruction: "Resolve technical issues",
    }, modelProvider, nil)

    billingSupport := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name:        "billing-support",
        Instruction: "Handle billing and payment questions",
    }, modelProvider, nil)

    productSupport := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name:        "product-support",
        Instruction: "Answer product questions",
    }, modelProvider, nil)

    // Create tier 2 coordinator
    tier2Coordinator := agent.NewAgent(agent.AgentConfig{
        Name: "tier2-coordinator",
        SubAgents: []agent.Agent{
            technicalSupport,
            billingSupport,
            productSupport,
        },
    })

    // Create main coordinator
    mainCoordinator := agent.NewAgent(agent.AgentConfig{
        Name: "main-coordinator",
        SubAgents: []agent.Agent{
            greeter,
            triageAgent,
            tier2Coordinator,
        },
    })

    // Set up tools for main coordinator
    toolRegistry := agents.CreateHierarchicalToolRegistry(mainCoordinator)

    // Add agent descriptions to prompt
    agentList := agents.FormatAgentsForPrompt(mainCoordinator)

    // Create coordinator LLM agent
    coordinator := agents.NewLlmAgent(agents.LlmAgentConfig{
        Name: "coordinator",
        Instruction: fmt.Sprintf(`You are a customer service coordinator.

%s

Always greet new customers first, then triage their request,
then transfer to the appropriate specialist.`, agentList),
        Tools: toolRegistry.ListToolNames(),
    }, modelProvider, toolRegistry)

    // Handle customer request
    output, err := coordinator.Execute(ctx, &agent.AgentInput{
        Instruction: "Hello, I'm having trouble logging into my account",
    })

    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Response: %s\n", output.Result)
}
```

**Execution Flow:**
1. Coordinator greets user (delegates to greeter)
2. Coordinator triages issue (delegates to triage)
3. Triage identifies as technical issue
4. Coordinator transfers to tech-support
5. Tech-support resolves the issue
6. Response returned to user

---

## Performance Considerations

### Hierarchy Overhead

- **Navigation**: O(depth) for parent traversal, O(n) for descendant search
- **Memory**: Minimal - just parent/children pointers
- **Thread-Safety**: RWMutex for concurrent access

### Optimization Tips

1. **Keep hierarchies shallow** (3-4 levels max)
2. **Cache frequently accessed agents** instead of repeated FindAgent calls
3. **Use descriptive names** for fast lookup
4. **Limit sub-agents per parent** (5-10 ideal)

---

## Testing

```bash
# Run hierarchy tests
go test ./pkg/agents -run TestAgentHierarchy -v

# Run agent-as-tool tests
go test ./pkg/agents -run TestAgentAsTool -v
```

---

## Migration Guide

### From Dependencies to Hierarchy

**Before (Phase 3.5):**
```go
coordinator := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:         "coordinator",
    Dependencies: []agent.Agent{greeter, taskDoer},
})
```

**After (with Hierarchy):**
```go
coordinator := agent.NewAgent(agent.AgentConfig{
    Name:      "coordinator",
    SubAgents: []agent.Agent{greeter, taskDoer},
})

// Now you can:
// - Navigate: greeter.Parent(), coordinator.FindAgent("greeter")
// - Delegate: transfer_to_agent tool
// - Tools: WrapAgentAsTool(greeter)
```

---

## ADK Compatibility Matrix

| Feature | ADK Python | Spark AI Agents | Status |
|---------|-----------|-----------------|--------|
| **parent_agent** | ✅ | ✅ `Parent()` | ✅ Full |
| **sub_agents** | ✅ | ✅ `SubAgents()` | ✅ Full |
| **find_agent(name)** | ✅ | ✅ `FindAgent(name)` | ✅ Full |
| **Single parent rule** | ✅ | ✅ Enforced | ✅ Full |
| **Shared session state** | ✅ | ✅ Via Context | ✅ Full |
| **AgentTool pattern** | ✅ | ✅ `WrapAgentAsTool()` | ✅ Full |
| **transfer_to_agent** | ✅ | ✅ `CreateTransferTool()` | ✅ Full |
| **LLM-driven delegation** | ✅ | ✅ Via tools | ✅ Full |
| **Plus: GetRoot()** | ❌ | ✅ | ✅ Enhanced |
| **Plus: GetPath()** | ❌ | ✅ | ✅ Enhanced |
| **Plus: GetDepth()** | ❌ | ✅ | ✅ Enhanced |
| **Plus: Distributed** | ❌ | ✅ DAG-based | ✅ Enhanced |

---

## Summary

Agent Hierarchy provides:

✅ **Full ADK compatibility** - Works like Google ADK Python
✅ **Enhanced navigation** - Additional methods beyond ADK
✅ **Agent-as-tool pattern** - Easy delegation
✅ **LLM-driven routing** - `transfer_to_agent` for dynamic delegation
✅ **Thread-safe** - Safe for concurrent access
✅ **Distributed execution** - Runs on our Spark-inspired DAG infrastructure

Combined with our workflow agents (Sequential, Parallel, Loop) and extended agents (Remote, MapReduce, Router), this creates a comprehensive toolkit for building sophisticated multi-agent systems!

---

## References

- [ADK Multi-Agents Documentation](https://google.github.io/adk-docs/agents/multi-agents/)
- [Agent Types (Phase 3.5)](./PHASE3_5_AGENT_TYPES.md)
- [Extended Agents](./PHASE3_5_EXTENDED_AGENTS.md)
- [Session Management](../pkg/sessions/session.go)

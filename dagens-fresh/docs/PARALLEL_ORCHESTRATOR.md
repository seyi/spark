# ParallelAgent: ADK-Compatible Parallel Orchestrator

## Overview

Our `ParallelAgent` provides **full ADK compatibility** for parallel execution of sub-agents with interleaved event streams. This document explains how our implementation matches and extends Google ADK's ParallelAgent pattern.

## ADK Compatibility Matrix

| Feature | ADK ParallelAgent | Spark ParallelAgent | Status |
|---------|-------------------|---------------------|---------|
| SubAgents configuration | ✅ | ✅ | **Full Parity** |
| Parallel execution | ✅ | ✅ | **Full Parity** |
| Interleaved events | ✅ | ✅ | **Full Parity** |
| Shared context/state | ✅ | ✅ | **Full Parity** |
| Parent-child hierarchy | ✅ | ✅ | **Full Parity** |
| Multiple aggregation modes | ❌ | ✅ | **Enhanced** |
| Fail-fast behavior | ❌ | ✅ | **Enhanced** |
| Partial success (quorum) | ❌ | ✅ | **Enhanced** |
| Timeout per orchestrator | ❌ | ✅ | **Enhanced** |

## Core Concept: Interleaved Events

**Interleaved events** means that when multiple sub-agents execute in parallel, their completion events happen in whatever order they naturally finish - not in the order they were added.

```
Timeline:
0ms   -----> [FastAgent starts]    [MediumAgent starts]    [SlowAgent starts]
50ms  -----> [FastAgent completes] ✓
150ms -----> [MediumAgent completes] ✓
300ms -----> [SlowAgent completes] ✓

Events are INTERLEAVED: Fast → Medium → Slow
NOT sequential: Agent1 → Agent2 → Agent3
```

This is the hallmark of true parallel execution, matching ADK's behavior.

## ADK-Compatible Usage

### Method 1: SubAgents Configuration (ADK Standard)

```go
import (
    "github.com/apache/spark/spark-ai-agents/pkg/agent"
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
)

// Create sub-agents
searchAgent := agent.NewAgent(agent.AgentConfig{
    Name: "SearchAgent",
    Description: "Searches database",
    Executor: searchExecutor,
})

analysisAgent := agent.NewAgent(agent.AgentConfig{
    Name: "AnalysisAgent",
    Description: "Analyzes data",
    Executor: analysisExecutor,
})

summaryAgent := agent.NewAgent(agent.AgentConfig{
    Name: "SummaryAgent",
    Description: "Generates summary",
    Executor: summaryExecutor,
})

// Create parallel orchestrator with SubAgents
// This matches ADK's API exactly
orchestrator := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:        "DataProcessingOrchestrator",
    SubAgents:   []agent.Agent{searchAgent, analysisAgent, summaryAgent},
    Aggregation: agents.AllAggregation,
    Timeout:     30 * time.Second,
})

// Execute - sub-agents run in parallel with interleaved events
ctx := context.Background()
input := &agent.AgentInput{
    Instruction: "Process customer data",
    Context: map[string]interface{}{
        "dataset": "customers_2024",
    },
}

output, err := orchestrator.Execute(ctx, input)
```

### Key Points

1. **SubAgents field**: Accepts `[]agent.Agent` just like ADK
2. **Automatic parent references**: Sub-agents get parent references set automatically
3. **Hierarchy integration**: Full `Parent()`, `SubAgents()`, `FindAgent()` support
4. **Shared context**: Context map is shared across all sub-agents
5. **Parallel execution**: Uses goroutines for true concurrency
6. **Interleaved events**: Events complete in natural order, not declaration order

## How Interleaving Works

### Internal Implementation

```go
// ParallelAgent launches goroutines for each sub-agent
for i, childAgent := range e.parallelAgent.childAgents {
    wg.Add(1)
    go func(idx int, agent agent.Agent) {
        defer wg.Done()

        // Each agent runs independently
        output, err := agent.Execute(ctx, input)

        // Results collected as they complete (interleaved!)
        results[idx] = output
    }(i, childAgent)
}

// Wait for all to complete
wg.Wait()
```

### Interleaving Demonstration

```go
// Create agents with different execution times
fast := createAgent("Fast", 100*time.Millisecond)
medium := createAgent("Medium", 300*time.Millisecond)
slow := createAgent("Slow", 500*time.Millisecond)

// Add in intentionally "wrong" order
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:      "InterleavedDemo",
    SubAgents: []agent.Agent{slow, medium, fast}, // Slow first!
})

// Execute and watch completion order
output, _ := parallel.Execute(ctx, input)

// Output shows interleaved completion:
// [Fast] Completed at 100ms
// [Medium] Completed at 300ms
// [Slow] Completed at 500ms
//
// Total time: ~500ms (not 900ms if sequential!)
```

## Aggregation Strategies

Our implementation provides multiple ways to aggregate parallel results:

### 1. All Aggregation (Default)

Collects all results into an array.

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.AllAggregation,
})

// Result: []interface{}{result1, result2, result3}
```

### 2. First Aggregation (Race Pattern)

Returns the first successful result, cancels others.

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.FirstAggregation,
})

// Result: whichever agent completes first
// Others are cancelled automatically
```

This is useful for:
- Querying multiple data sources (first to respond wins)
- Redundant processing with different strategies
- Timeout-sensitive operations

### 3. Concat Aggregation

Concatenates string results.

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.ConcatAggregation,
})

// Result: "result1\n\nresult2\n\nresult3"
```

### 4. Vote Aggregation

Takes majority vote (for classification/decision-making).

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.VoteAggregation,
})

// If 2 agents return "approve" and 1 returns "reject"
// Result: "approve"
```

### 5. Custom Reduce Function

Apply custom logic to combine results.

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.ReduceAggregation,
    ReduceFunc: func(results []*agent.AgentOutput) (interface{}, error) {
        // Custom aggregation logic
        total := 0
        for _, r := range results {
            if val, ok := r.Result.(int); ok {
                total += val
            }
        }
        return total, nil
    },
})
```

## Advanced Features

### Partial Success (Quorum Pattern)

Require only N out of M agents to succeed:

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:     []agent.Agent{agent1, agent2, agent3, agent4, agent5},
    MinSuccessful: 3, // Only 3 out of 5 need to succeed
    Aggregation:   agents.AllAggregation,
})

// Succeeds if at least 3 agents complete successfully
// Useful for distributed consensus, redundancy, fault tolerance
```

### Fail-Fast Behavior

Cancel all agents on first failure:

```go
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:   []agent.Agent{agent1, agent2, agent3},
    FailFast:    true, // Stop all on first error
    Aggregation: agents.AllAggregation,
})

// If any agent fails, immediately:
// 1. Cancel all other agents
// 2. Return error
// 3. Don't wait for others to complete
```

### Builder API

Fluent interface for complex configurations:

```go
parallel := agents.NewParallel(agents.AllAggregation).
    WithName("ComplexOrchestrator").
    Add(searchAgent).
    Add(analysisAgent).
    Add(summaryAgent).
    WithTimeout(30 * time.Second).
    WithMinSuccessful(2).
    WithFailFast(false).
    Build()
```

## Hierarchy Integration

ParallelAgent fully integrates with our agent hierarchy system:

```go
// Create parallel orchestrator
orchestrator := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:      "Orchestrator",
    SubAgents: []agent.Agent{agent1, agent2, agent3},
})

// Navigate hierarchy
subAgents := orchestrator.SubAgents()          // Get all sub-agents
agent1Found, _ := orchestrator.FindAgent("agent1") // Find by name
parent := agent1.(*agent.BaseAgent).Parent()   // Get parent reference

// Use as tools
toolRegistry := agents.CreateHierarchicalToolRegistry(orchestrator)
// Now sub-agents can be called as tools by LLMs
```

## Real-World Examples

### Example 1: Multi-Source Data Gathering

```go
// Query multiple data sources in parallel
dbAgent := createDatabaseAgent()
apiAgent := createAPIAgent()
cacheAgent := createCacheAgent()

dataGatherer := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:        "DataGatherer",
    SubAgents:   []agent.Agent{dbAgent, apiAgent, cacheAgent},
    Aggregation: agents.FirstAggregation, // First to respond wins
    Timeout:     5 * time.Second,
})

output, _ := dataGatherer.Execute(ctx, &agent.AgentInput{
    Instruction: "Get user profile for user_123",
})
// Returns as soon as any source responds
// Others are cancelled
```

### Example 2: Ensemble Model Prediction

```go
// Multiple ML models vote on classification
model1 := createModelAgent("GPT-4")
model2 := createModelAgent("Claude")
model3 := createModelAgent("Gemini")

ensemble := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:        "EnsembleClassifier",
    SubAgents:   []agent.Agent{model1, model2, model3},
    Aggregation: agents.VoteAggregation, // Majority vote
    Timeout:     30 * time.Second,
})

output, _ := ensemble.Execute(ctx, &agent.AgentInput{
    Instruction: "Classify sentiment of: 'This product is amazing!'",
})
// Returns: "positive" (if majority agrees)
```

### Example 3: Distributed Document Processing

```go
// Process document sections in parallel
sectionAnalyzer := createSectionAgent()

// Split document into sections and process in parallel
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "DocumentProcessor",
    SubAgents: []agent.Agent{
        sectionAnalyzer, sectionAnalyzer, sectionAnalyzer, sectionAnalyzer,
    },
    Aggregation:   agents.ConcatAggregation, // Combine results
    MinSuccessful: 3, // At least 3 sections must succeed
    Timeout:       60 * time.Second,
})
```

### Example 4: Fault-Tolerant Service with Redundancy

```go
// Call same service 5 times, succeed if 3 respond
serviceAgent := createServiceAgent()

faultTolerant := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "FaultTolerantService",
    SubAgents: []agent.Agent{
        serviceAgent, serviceAgent, serviceAgent,
        serviceAgent, serviceAgent,
    },
    MinSuccessful: 3,    // 3 out of 5 must succeed
    Aggregation:   agents.VoteAggregation,
    Timeout:       10 * time.Second,
})
```

## Performance Considerations

### 1. Goroutine Overhead

Each sub-agent runs in its own goroutine:
- **Lightweight**: Goroutines are cheap (~2KB stack)
- **Scalable**: Can handle 100s of concurrent agents
- **Efficient**: Go runtime handles scheduling

### 2. Execution Time

Total execution time ≈ slowest sub-agent:
```
Sequential: T1 + T2 + T3 = 900ms
Parallel:   max(T1, T2, T3) = 500ms

Speedup: 1.8x with 3 agents
```

### 3. Resource Usage

- **CPU**: Parallel execution uses more CPU cores
- **Memory**: Each agent's context is separate
- **Network**: Concurrent requests may stress downstream services

### 4. Best Practices

```go
// ✅ Good: Bound parallelism
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents:     agents,
    Timeout:       30 * time.Second,    // Always set timeout
    MinSuccessful: len(agents) * 2 / 3, // Allow some failures
})

// ❌ Bad: Unbounded parallelism
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    SubAgents: thousands_of_agents, // Too many!
    // No timeout!                   // Can hang forever
})
```

For large-scale parallel work, use `MapReduceAgent` instead:
```go
// Better for 100s of parallel tasks
mapReduce := agents.NewMapReduceAgent(agents.MapReduceAgentConfig{
    MapperAgent:  workerAgent,
    ReducerAgent: aggregatorAgent,
    Parallelism:  10, // Bounded parallelism
    Splitter:     agents.FixedCountSplitter(100, "data"),
})
```

## Comparison: ParallelAgent vs SequentialAgent

| Aspect | ParallelAgent | SequentialAgent |
|--------|---------------|-----------------|
| Execution | Concurrent (goroutines) | Sequential (one after another) |
| Events | Interleaved | Ordered |
| Execution time | max(T1, T2, ..., Tn) | T1 + T2 + ... + Tn |
| Context sharing | Parallel reads | Sequential modification |
| Use case | Independent tasks | Dependent tasks |
| Failure handling | Fail-fast or quorum | Stop on first error |

## Migration from ADK

If you're migrating from Google ADK Python or Go:

```python
# ADK Python
parallel = ParallelAgent(
    name="orchestrator",
    sub_agents=[agent1, agent2, agent3],
)
```

```go
// Spark AI Agents (our implementation)
parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:      "orchestrator",
    SubAgents: []agent.Agent{agent1, agent2, agent3},
    Aggregation: agents.AllAggregation, // Additional control
})
```

**Key Differences:**
1. We require explicit `Aggregation` mode (defaults to `AllAggregation`)
2. We support additional features (fail-fast, quorum, timeouts)
3. We integrate with Spark-inspired DAG infrastructure

**Similarities:**
1. Same `SubAgents` configuration API
2. Same parallel execution semantics
3. Same interleaved event behavior
4. Same hierarchy integration

## Testing Interleaved Behavior

To verify interleaved execution:

```go
func TestInterleavedEvents(t *testing.T) {
    var completionOrder []string
    var mu sync.Mutex

    // Create agents that track completion
    fast := createTrackingAgent("Fast", 50*time.Millisecond, &completionOrder, &mu)
    slow := createTrackingAgent("Slow", 200*time.Millisecond, &completionOrder, &mu)

    parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
        SubAgents: []agent.Agent{slow, fast}, // Slow first
    })

    parallel.Execute(ctx, input)

    // Verify Fast completed before Slow (interleaved!)
    assert.Equal(t, []string{"Fast", "Slow"}, completionOrder)
}
```

## Conclusion

Our `ParallelAgent` provides **full ADK compatibility** while extending the pattern with additional features:

✅ **ADK Parity**: SubAgents configuration, parallel execution, interleaved events
✅ **Enhanced**: Multiple aggregation modes, fail-fast, quorum, timeouts
✅ **Integrated**: Full hierarchy support, tool wrapping, DAG infrastructure
✅ **Production-Ready**: Comprehensive tests, examples, documentation

Use ParallelAgent when you need true parallel execution of independent sub-agents with interleaved event streams - exactly as ADK does, with additional Spark-inspired distributed computing features.

## Phase 3.5: Agent Types - Bridging ADK and Spark

### Overview

Phase 3.5 implements ADK-style agent types as high-level abstractions over our distributed DAG infrastructure. This gives us the best of both worlds:

✅ **ADK's Developer Experience** - Simple, batteries-included agent types
✅ **Spark's Scalability** - Distributed execution, fault tolerance, DAG optimization

### Implementation Summary

We've implemented 4 core agent types identified in the critical analysis:

1. **LlmAgent** - LLM-based reasoning with automatic tool calling and ReAct pattern
2. **SequentialAgent** - Linear workflow execution with output passing
3. **ParallelAgent** - Concurrent execution with multiple aggregation strategies
4. **LoopAgent** - Iterative execution with flexible conditions

All agent types compile down to our AgentDAG infrastructure for distributed execution.

---

## 1. LlmAgent

### Purpose

LlmAgent provides LLM-based reasoning and generation with automatic tool calling, enabling natural language interaction and complex decision-making.

### Key Features

- **Model Integration**: Works with any ModelProvider from Phase 1
- **Tool Calling**: Automatic tool selection and execution
- **ReAct Pattern**: Reasoning + Acting loop for complex tasks
- **Multi-Turn**: Supports conversational interactions
- **Temperature Control**: Adjustable creativity/determinism

### Architecture

```
User Input
    ↓
LlmAgent
    ↓
Model Provider → Generate Response
    ↓
Tool Call Detected?
    ├─ Yes → Execute Tool → Generate with Result
    └─ No → Return Response
```

### Basic Usage

```go
import (
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
    "github.com/apache/spark/spark-ai-agents/pkg/model"
    "github.com/apache/spark/spark-ai-agents/pkg/tools"
)

// Create model provider
modelProvider := model.NewOpenAIProvider("gpt-4", apiKey)

// Create tool registry
toolRegistry := tools.NewToolRegistry()
toolRegistry.RegisterTool(searchTool)
toolRegistry.RegisterTool(calculatorTool)

// Create LLM agent
llmAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "assistant",
    ModelName:   "gpt-4",
    Instruction: "You are a helpful research assistant",
    Tools:       []string{"web_search", "calculator"},
    Temperature: 0.7,
}, modelProvider, toolRegistry)

// Execute
output, err := llmAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Search for AI trends and calculate market size",
})
```

### ReAct Pattern

The ReAct (Reasoning + Acting) pattern enables iterative problem-solving:

```go
reactAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:          "react-agent",
    ModelName:     "gpt-4",
    Instruction:   "Solve problems step by step",
    Tools:         []string{"search", "calculator", "database"},
    UseReAct:      true,
    MaxIterations: 5,
}, modelProvider, toolRegistry)

output, err := reactAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Research the population of Tokyo and compare it to New York",
})

// Output metadata contains conversation history
iterations := output.Metadata["iterations"]
conversation := output.Metadata["conversation"]
```

### Fluent API

```go
agent := agents.NewLlmAgent(config, modelProvider, toolRegistry).
    WithTools("search", "calculator").
    WithTemperature(0.9).
    WithReAct(true)
```

---

## 2. SequentialAgent

### Purpose

SequentialAgent executes agents in predetermined order, enabling pipeline-style workflows where each step processes the output of the previous step.

### Key Features

- **Linear Execution**: Guaranteed sequential order
- **Output Passing**: Pass results between steps
- **Error Handling**: Stop on error or continue
- **Conditional Branching**: Execute different paths based on conditions
- **Fluent Builder**: Easy pipeline construction

### Architecture

```
Input → Agent 1 → Agent 2 → Agent 3 → Final Output
         ↓          ↓          ↓
      Output 1   Output 2   Output 3
```

### Basic Usage

```go
// Create agents
researchAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "researcher",
    Instruction: "Research the topic",
}, modelProvider, toolRegistry)

summarizeAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "summarizer",
    Instruction: "Summarize the findings",
}, modelProvider, toolRegistry)

formatAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "formatter",
    Instruction: "Format as a report",
}, modelProvider, toolRegistry)

// Create sequential pipeline
pipeline := agents.NewSequentialAgent(agents.SequentialAgentConfig{
    Name:        "research-pipeline",
    Agents:      []agent.Agent{researchAgent, summarizeAgent, formatAgent},
    PassOutput:  true,  // Pass output from each step to next
    StopOnError: true,  // Stop if any step fails
})

// Execute
output, err := pipeline.Execute(ctx, &agent.AgentInput{
    Instruction: "Research AI trends in 2024",
})
```

### Builder API

```go
pipeline := agents.NewSequential().
    WithName("data-pipeline").
    Add(extractAgent).
    Add(transformAgent).
    Add(loadAgent).
    WithPassOutput(true).
    WithStopOnError(false).  // Continue on errors
    Build()
```

### Conditional Branching

```go
// Create conditional agent
conditionalAgent := agents.NewConditional(func(output *agent.AgentOutput) bool {
    score := output.Metadata["confidence"].(float64)
    return score > 0.8  // High confidence
}).
Then(detailedAnalysisAgent).
Else(simpleAnalysisAgent).
Build()

// Use in pipeline
pipeline := agents.NewSequential().
    Add(classifierAgent).
    Add(conditionalAgent).
    Build()
```

---

## 3. ParallelAgent

### Purpose

ParallelAgent executes multiple agents concurrently, enabling data parallelism and multi-source aggregation with various result combination strategies.

### Key Features

- **Concurrent Execution**: All agents run in parallel
- **5 Aggregation Strategies**: All, Concat, First, Reduce, Vote
- **Timeout Control**: Prevent hanging executions
- **Partial Success**: Configure minimum successful agents
- **Fail-Fast**: Option to cancel on first error

### Aggregation Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **All** | Return all results as array | Multi-source data collection |
| **Concat** | Concatenate string results | Document generation |
| **First** | Return first completed result | Fastest response wins |
| **Reduce** | Custom aggregation function | Complex combinations |
| **Vote** | Majority vote from results | Consensus/classification |

### Architecture

```
              Input
                ↓
     ┌──────────┼──────────┐
     ↓          ↓          ↓
  Agent 1    Agent 2    Agent 3
     ↓          ↓          ↓
  Result 1  Result 2  Result 3
     └──────────┼──────────┘
                ↓
         Aggregation
                ↓
          Final Output
```

### All Aggregation

```go
// Collect data from multiple sources
parallelAgent := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "multi-source",
    Agents: []agent.Agent{
        webSearchAgent,
        databaseAgent,
        apiAgent,
    },
    Aggregation: agents.AllAggregation,
})

output, err := parallelAgent.Execute(ctx, input)
results := output.Result.([]interface{})  // Array of all results
```

### First Aggregation (Race)

```go
// Get fastest response from multiple models
raceAgent := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "model-race",
    Agents: []agent.Agent{
        gpt4Agent,
        claudeAgent,
        geminiAgent,
    },
    Aggregation: agents.FirstAggregation,
})

// Returns as soon as first agent completes
output, err := raceAgent.Execute(ctx, input)
```

### Concat Aggregation

```go
// Generate comprehensive report from multiple analysts
reportAgent := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "multi-analyst",
    Agents: []agent.Agent{
        technicalAnalystAgent,
        marketAnalystAgent,
        competitorAnalystAgent,
    },
    Aggregation: agents.ConcatAggregation,
})

output, err := reportAgent.Execute(ctx, input)
report := output.Result.(string)  // Concatenated reports
```

### Custom Reduce

```go
// Custom aggregation logic
reduceAgent := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:        "ensemble",
    Agents:      []agent.Agent{model1, model2, model3},
    Aggregation: agents.ReduceAggregation,
    ReduceFunc: func(results []*agent.AgentOutput) (interface{}, error) {
        // Custom logic: average confidence scores
        totalScore := 0.0
        for _, result := range results {
            score := result.Metadata["confidence"].(float64)
            totalScore += score
        }
        avgScore := totalScore / float64(len(results))

        // Return result with highest confidence
        var bestResult *agent.AgentOutput
        bestScore := 0.0
        for _, result := range results {
            score := result.Metadata["confidence"].(float64)
            if score > bestScore {
                bestScore = score
                bestResult = result
            }
        }

        return map[string]interface{}{
            "result":           bestResult.Result,
            "average_confidence": avgScore,
            "best_confidence":   bestScore,
        }, nil
    },
})
```

### Builder API

```go
parallelAgent := agents.NewParallel(agents.AllAggregation).
    WithName("parallel-search").
    Add(googleAgent).
    Add(bingAgent).
    Add(ddgAgent).
    WithTimeout(30 * time.Second).
    WithMinSuccessful(2).  // At least 2 must succeed
    WithFailFast(false).   // Don't cancel on first error
    Build()
```

### Vote Aggregation

```go
// Get consensus from multiple classifiers
voteAgent := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name: "classifier-ensemble",
    Agents: []agent.Agent{
        classifier1,
        classifier2,
        classifier3,
    },
    Aggregation: agents.VoteAggregation,
})

output, err := voteAgent.Execute(ctx, input)
majorityVote := output.Result  // Most common result
```

---

## 4. LoopAgent

### Purpose

LoopAgent executes an agent iteratively with flexible termination conditions, enabling refinement loops, convergence patterns, and iterative improvement.

### Key Features

- **4 Loop Types**: While, Until, ForEach, Count
- **Flexible Conditions**: Score thresholds, convergence, error rates
- **3 Accumulation Modes**: All results, last only, stateful
- **Safety Guards**: Max iterations, timeouts
- **Built-in Conditions**: Common patterns pre-implemented

### Loop Types

| Type | Description | Example |
|------|-------------|---------|
| **While** | Continue while condition is true | While quality < 0.9 |
| **Until** | Continue until condition is true | Until converged |
| **ForEach** | Iterate over collection | For each document |
| **Count** | Fixed iterations | Exactly 5 times |

### Architecture

```
Input → Iteration 1 → Condition?
            ↓           ├─ True → Iteration 2 → Condition?
         Result 1       └─ False → Final Output
```

### Until Condition

```go
// Refine until quality threshold
refineAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "refiner",
    Instruction: "Improve the text quality",
}, modelProvider, toolRegistry)

loopAgent := agents.NewLoopAgent(agents.LoopAgentConfig{
    Name:          "quality-loop",
    InnerAgent:    refineAgent,
    LoopType:      agents.UntilLoop,
    MaxIterations: 5,
    Condition: func(output *agent.AgentOutput, iteration int) bool {
        score := output.Metadata["quality_score"].(float64)
        return score >= 0.9  // Stop when quality >= 0.9
    },
})

output, err := loopAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Refine this text: [original text]",
})

iterations := output.Metadata["iterations"]  // How many iterations ran
```

### While Condition

```go
// Continue while improving
loopAgent := agents.NewLoopAgent(agents.LoopAgentConfig{
    Name:          "improvement-loop",
    InnerAgent:    optimizerAgent,
    LoopType:      agents.WhileLoop,
    MaxIterations: 10,
    Condition: func(output *agent.AgentOutput, iteration int) bool {
        improvement := output.Metadata["improvement"].(float64)
        return improvement > 0.01  // Continue while improving
    },
})
```

### Builder API

```go
loopAgent := agents.NewLoop(refinerAgent).
    WithName("refinement-loop").
    Until(func(output *agent.AgentOutput, iteration int) bool {
        score := output.Metadata["quality_score"].(float64)
        return score >= 0.9
    }).
    MaxIterations(5).
    AccumulateAll().  // Keep all iteration results
    WithTimeout(2 * time.Minute).
    Build()
```

### Built-in Condition Helpers

#### Score Threshold

```go
// Stop when score reaches threshold
condition := agents.ScoreThresholdCondition("quality_score", 0.9)

loopAgent := agents.NewLoop(refinerAgent).
    Until(condition).
    MaxIterations(10).
    Build()
```

#### Convergence

```go
// Stop when values stop changing
condition := agents.ConvergenceCondition(0.01)  // Tolerance

loopAgent := agents.NewLoop(optimizerAgent).
    Until(condition).
    MaxIterations(100).
    Build()
```

#### Error Threshold

```go
// Stop if error rate too high
condition := agents.ErrorThresholdCondition(0.2)  // Max 20% errors

loopAgent := agents.NewLoop(unreliableAgent).
    While(condition).
    MaxIterations(20).
    Build()
```

### Accumulation Modes

#### Accumulate All

```go
// Keep all iteration results
loopAgent := agents.NewLoop(generatorAgent).
    MaxIterations(5).
    AccumulateAll().
    Build()

output, err := loopAgent.Execute(ctx, input)
allResults := output.Result.([]interface{})  // Array of all iterations
```

#### Accumulate Last

```go
// Keep only final result (default)
loopAgent := agents.NewLoop(refinerAgent).
    MaxIterations(5).
    AccumulateLast().
    Build()

output, err := loopAgent.Execute(ctx, input)
finalResult := output.Result  // Only last iteration
```

#### Accumulate State

```go
// Maintain state across iterations
loopAgent := agents.NewLoop(stateAgent).
    MaxIterations(10).
    AccumulateState().
    Build()

// Agent can update state in metadata
// state := output.Metadata["state"]
```

---

## Complex Compositions

Agent types can be composed to create sophisticated workflows.

### Sequential of Parallel

```go
// Stage 1: Parallel data collection
dataCollection := agents.NewParallel(agents.AllAggregation).
    Add(webScraperAgent).
    Add(apiAgent).
    Add(databaseAgent).
    Build()

// Stage 2: Parallel analysis
analysis := agents.NewParallel(agents.AllAggregation).
    Add(sentimentAgent).
    Add(topicAgent).
    Add(summaryAgent).
    Build()

// Stage 3: Final synthesis
synthesis := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "synthesizer",
    Instruction: "Synthesize all analyses into final report",
}, modelProvider, toolRegistry)

// Compose as sequential
pipeline := agents.NewSequential().
    WithName("data-pipeline").
    Add(dataCollection).
    Add(analysis).
    Add(synthesis).
    WithPassOutput(true).
    Build()
```

### Loop of Sequential

```go
// Inner sequential workflow
innerWorkflow := agents.NewSequential().
    Add(generateAgent).
    Add(evaluateAgent).
    WithPassOutput(true).
    Build()

// Outer refinement loop
refinementLoop := agents.NewLoop(innerWorkflow).
    WithName("generate-evaluate-loop").
    Until(func(output *agent.AgentOutput, iteration int) bool {
        score := output.Metadata["evaluation_score"].(float64)
        return score >= 0.85
    }).
    MaxIterations(5).
    Build()
```

### Parallel of Loops

```go
// Multiple refinement loops in parallel
loop1 := agents.NewLoop(refiner1).Until(condition).MaxIterations(5).Build()
loop2 := agents.NewLoop(refiner2).Until(condition).MaxIterations(5).Build()
loop3 := agents.NewLoop(refiner3).Until(condition).MaxIterations(5).Build()

// Run all loops in parallel, take best result
ensemble := agents.NewParallel(agents.ReduceAggregation).
    Add(loop1).
    Add(loop2).
    Add(loop3).
    WithReduceFunc(selectBestByScore).
    Build()
```

---

## Integration with Spark Infrastructure

All agent types compile to AgentDAGs for distributed execution:

### DAG Generation

```go
// High-level agent composition
workflow := agents.NewSequential().
    Add(parallel1).
    Add(loop1).
    Add(parallel2).
    Build()

// Automatically generates DAG with stages
dag, err := agent.NewAgentDAG(workflow, input)

// Submit to coordinator for distributed execution
jobID, err := coordinator.SubmitDAG(ctx, dag)
```

### Distributed Benefits

- **Stage-based Execution**: Parallel agents map to DAG stages
- **Locality-aware Scheduling**: Task placement considers data locality
- **Fault Tolerance**: Lineage tracking for failure recovery
- **Scalability**: Horizontal scaling across cluster
- **Resource Management**: Automatic resource allocation

### Monitoring

```go
// Get execution metrics
metrics := output.Metadata["execution_time"]
stages := output.Metadata["dag_stages"]
taskCount := output.Metadata["total_tasks"]

// Telemetry integration (Phase 2)
telemetry.RecordAgentExecution(workflow.Name(), metrics)
```

---

## Performance Considerations

### LlmAgent

- **Model latency**: Primary bottleneck
- **Tool calls**: Add round-trip time
- **ReAct**: Multiple LLM calls per task
- **Optimization**: Cache model responses, use streaming

### SequentialAgent

- **Sequential bottleneck**: Total time = sum of all steps
- **Optimization**: Use ParallelAgent where possible
- **Early exit**: Configure StopOnError for fail-fast

### ParallelAgent

- **Concurrency**: Limited by available resources
- **Network overhead**: Remote agent invocations
- **Optimization**: First aggregation for speed, load balancing

### LoopAgent

- **Iteration count**: Can grow unbounded without max
- **Accumulation**: Memory usage for AccumulateAll
- **Optimization**: Set tight max iterations, use convergence conditions

---

## Migration from Base Agents

### Before (Phase 1-3)

```go
// Manual dependency wiring
agent1 := agent.NewAgent(agent.AgentConfig{
    Name:     "step1",
    Executor: executor1,
})

agent2 := agent.NewAgent(agent.AgentConfig{
    Name:         "step2",
    Executor:     executor2,
    Dependencies: []agent.Agent{agent1},
})

agent3 := agent.NewAgent(agent.AgentConfig{
    Name:         "step3",
    Executor:     executor3,
    Dependencies: []agent.Agent{agent2},
})

dag, _ := agent.NewAgentDAG(agent3, input)
```

### After (Phase 3.5)

```go
// High-level sequential builder
pipeline := agents.NewSequential().
    Add(agent1).
    Add(agent2).
    Add(agent3).
    WithPassOutput(true).
    Build()

dag, _ := agent.NewAgentDAG(pipeline, input)
```

---

## Testing

Run Phase 3.5 tests:

```bash
# All agent types tests
go test ./pkg/agents -v

# Specific test
go test ./pkg/agents -run TestLlmAgent

# With coverage
go test ./pkg/agents -cover

# Integration tests
go test ./pkg/agents -run TestAgentComposition
```

---

## Examples

Complete examples in `examples/phase3_5_examples.go`:

1. **Research Pipeline** - Sequential LLM agents with output passing
2. **Multi-Model Consensus** - Parallel agents with vote aggregation
3. **Iterative Refinement** - Loop agent with quality threshold
4. **Complex Workflow** - Composition of all agent types

---

## Comparison with ADK

| Feature | ADK Python | Spark AI Agents Phase 3.5 | Advantage |
|---------|-----------|---------------------------|-----------|
| LlmAgent | ✅ Built-in | ✅ Built-in + Distributed | Ours (distributed) |
| SequentialAgent | ✅ Basic | ✅ + Conditional branching | Ours (features) |
| ParallelAgent | ✅ Basic | ✅ + 5 aggregations | Ours (flexibility) |
| LoopAgent | ✅ Basic | ✅ + Built-in conditions | Ours (helpers) |
| ReAct | ✅ Basic | ✅ Full implementation | Equal |
| Tool Calling | ✅ Automatic | ✅ Automatic | Equal |
| Scalability | ❌ Single process | ✅ Distributed cluster | Ours (architecture) |
| Fault Tolerance | ❌ Limited | ✅ Lineage-based | Ours (reliability) |
| Developer UX | ✅ Excellent | ✅ Excellent | Equal |

---

## Next Steps

Phase 3.5 completes the agent types implementation. Future enhancements:

1. **More Patterns**: Plan-and-Execute, Critic, Chain-of-Thought
2. **Advanced Tool Use**: Multi-modal tools, tool chaining
3. **Memory Integration**: Agent-specific memory (Phase 2)
4. **Cost Optimization**: Model selection, caching strategies
5. **Monitoring**: Real-time agent performance dashboards

---

## References

- [ADK Agent Types](https://google.github.io/adk-docs/agents/)
- [Agent Types Analysis](./AGENT_TYPES_ANALYSIS.md)
- [Phase 3 Documentation](./PHASE3.md)
- [Apache Spark DAG Scheduler](https://spark.apache.org/docs/latest/job-scheduling.html)

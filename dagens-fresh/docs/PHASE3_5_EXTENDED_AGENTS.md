# Phase 3.5 Extended: Additional Agent Types

This document covers extended agent types discovered from deeper ADK analysis and distributed systems patterns.

## Overview

After the initial Phase 3.5 implementation (Llm, Sequential, Parallel, Loop), we identified additional critical agent types:

1. **RemoteAgent** - Easy invocation of remote A2A agents
2. **MapReduceAgent** - Distributed map-reduce pattern
3. **RouterAgent** - Dynamic routing based on conditions
4. **LoadBalancerAgent** - Load balancing across agents
5. **FanOutAgent** - Distribute work without reduce phase
6. **IntentRouter** - Intent-based routing with classification

---

## 1. RemoteAgent

### Purpose

RemoteAgent wraps remote A2A-compatible agents, providing easy invocation over HTTP with retry logic, caching, and streaming support.

### Key Features

- **Multiple Input Formats**: Agent ID, endpoint URL, or pre-fetched AgentCard
- **Retry Logic**: Automatic retry with exponential backoff
- **Agent Card Caching**: Reduces discovery overhead
- **Streaming Support**: Server-sent events for real-time responses
- **Load Balancing**: RemoteAgentPool for distributing requests
- **Discovery Integration**: Find agents by capability

### Architecture

```
Local Agent
    ↓
RemoteAgent Wrapper
    ↓
HTTP/A2A Protocol
    ↓
Remote Agent Service
    ↓
Response with Metadata
```

### Basic Usage

```go
import (
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
)

// Create remote agent
remoteAgent, err := agents.NewRemote("research-agent", "http://agents.example.com").
    WithName("remote-researcher").
    WithTimeout(30 * time.Second).
    WithRetryCount(3).
    WithCaching(true, 5*time.Minute).
    Build()

if err != nil {
    log.Fatal(err)
}

// Execute
output, err := remoteAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Research quantum computing trends",
})
```

### With Agent Card

```go
// Fetch agent card once
card, err := a2aClient.GetAgentCard(ctx, "research-agent")

// Create multiple agents with same card
agent1, _ := agents.NewRemote("research-agent", endpoint).
    WithAgentCard(card).
    Build()

agent2, _ := agents.NewRemote("research-agent", endpoint2).
    WithAgentCard(card).
    Build()
```

### Streaming

```go
// Create streaming remote agent
streamingAgent, err := agents.NewStreamingRemoteAgent(
    agents.StreamingRemoteAgentConfig{
        RemoteAgentConfig: agents.RemoteAgentConfig{
            Name:     "streaming-agent",
            AgentID:  "stream-1",
            Endpoint: "http://agents.example.com",
        },
        BufferSize: 20,
    },
)

// Stream execution
stream, err := streamingAgent.StreamExecute(ctx, input)

// Process chunks
for chunk := range stream {
    fmt.Print(chunk.Content)
}

// Or use callback
err = streamingAgent.StreamWithCallback(ctx, input, func(chunk *a2a.StreamChunk) error {
    fmt.Print(chunk.Content)
    return nil
})
```

### Load Balancing Pool

```go
// Create pool of remote agents
agent1, _ := agents.NewRemote("agent-1", "http://host1").Build()
agent2, _ := agents.NewRemote("agent-2", "http://host2").Build()
agent3, _ := agents.NewRemote("agent-3", "http://host3").Build()

pool := agents.NewRemoteAgentPool(agent1, agent2, agent3)

// Round-robin execution
output, err := pool.Execute(ctx, input)

// Fallback execution (tries agents in order)
output, err := pool.ExecuteWithFallback(ctx, input)
```

### Discovery

```go
// Create discovery client
discovery := agents.NewRemoteAgentDiscovery(registry)

// Find agents by capability
researchAgents, err := discovery.DiscoverByCapability("research")

// Execute on any discovered agent
output, err := researchAgents[0].Execute(ctx, input)
```

### Retry Logic

RemoteAgent automatically retries on:
- Network errors (connection refused, reset)
- Timeouts
- Server errors (5xx status codes)

**Not retried:**
- Client errors (4xx)
- Invalid input errors

```go
// Configure retry behavior
remoteAgent, _ := agents.NewRemote("agent", endpoint).
    WithRetryCount(5).                    // Max 5 retries
    WithTimeout(10 * time.Second).        // 10s timeout per attempt
    Build()

// Exponential backoff: 1s, 4s, 9s, 16s, 25s
```

---

## 2. MapReduceAgent

### Purpose

MapReduceAgent implements the map-reduce pattern for distributed data processing, enabling parallel processing of data splits with result aggregation.

### Key Features

- **Flexible Splitting**: Custom splitter functions
- **Controlled Parallelism**: Limit concurrent mappers
- **Error Collection**: Continue or fail-fast on errors
- **Built-in Splitters**: Line, list, document, fixed-count
- **Reducer Aggregation**: Combine results with custom logic

### Architecture

```
Input Data
    ↓
Split Function
    ↓
┌─────────┼─────────┐
↓         ↓         ↓
Map 1    Map 2    Map 3  (Parallel)
↓         ↓         ↓
└─────────┼─────────┘
          ↓
     Reduce Function
          ↓
    Final Result
```

### Basic Usage

```go
// Create mapper agent
mapper := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "document-analyzer",
    Instruction: "Analyze document and extract key points",
}, modelProvider, toolRegistry)

// Create reducer agent
reducer := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "summary-combiner",
    Instruction: "Combine analyses into coherent summary",
}, modelProvider, toolRegistry)

// Define splitter
splitter := agents.DocumentSplitter("documents")

// Create map-reduce agent
mrAgent := agents.NewMapReduce(mapper, reducer, splitter).
    WithName("document-processor").
    WithParallelism(10).
    WithTimeout(5 * time.Minute).
    Build()

// Execute
output, err := mrAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Process and summarize documents",
    Context: map[string]interface{}{
        "documents": []interface{}{doc1, doc2, doc3, ...},
    },
})
```

### Built-in Splitters

#### Line Splitter

```go
// Split text into chunks of N characters
splitter := agents.LineSplitter(1000)

input := &agent.AgentInput{
    Context: map[string]interface{}{
        "text": longText,
    },
}
```

#### List Splitter

```go
// Split list into chunks
splitter := agents.ListSplitter("items", 10)  // 10 items per chunk

input := &agent.AgentInput{
    Context: map[string]interface{}{
        "items": []interface{}{item1, item2, ..., item100},
    },
}
```

#### Document Splitter

```go
// One mapper per document
splitter := agents.DocumentSplitter("documents")

input := &agent.AgentInput{
    Context: map[string]interface{}{
        "documents": []interface{}{doc1, doc2, doc3},
    },
}
```

#### Fixed Count Splitter

```go
// Split data into N equal parts
splitter := agents.FixedCountSplitter(5, "data")  // 5 splits

input := &agent.AgentInput{
    Context: map[string]interface{}{
        "data": largeArray,
    },
}
```

### Custom Splitter

```go
// Custom splitter function
splitter := func(input *agent.AgentInput) ([]*agent.AgentInput, error) {
    data := input.Context["data"].([]MyDataType)

    var splits []*agent.AgentInput
    for _, item := range data {
        splits = append(splits, &agent.AgentInput{
            Instruction: input.Instruction,
            Context: map[string]interface{}{
                "item": item,
            },
        })
    }

    return splits, nil
}
```

### Error Handling

```go
// Continue on mapper errors
mrAgent := agents.NewMapReduce(mapper, reducer, splitter).
    WithCollectErrors(true).  // Don't fail on individual mapper errors
    Build()

output, err := mrAgent.Execute(ctx, input)

// Check error metadata
failedMaps := output.Metadata["failed_maps"].(int)
successfulMaps := output.Metadata["successful_maps"].(int)
```

### Example: Log Analysis

```go
// Mapper: Analyze individual log files
logAnalyzer := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "log-analyzer",
    Instruction: "Extract errors and warnings from log",
}, modelProvider, nil)

// Reducer: Combine findings
findingsCombiner := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "findings-combiner",
    Instruction: "Summarize all errors and warnings with counts",
}, modelProvider, nil)

// Splitter: One file per mapper
logSplitter := agents.DocumentSplitter("log_files")

// Create map-reduce
logProcessor := agents.NewMapReduce(logAnalyzer, findingsCombiner, logSplitter).
    WithName("log-processor").
    WithParallelism(20).  // Process 20 logs concurrently
    Build()

// Process logs
output, err := logProcessor.Execute(ctx, &agent.AgentInput{
    Instruction: "Analyze logs for issues",
    Context: map[string]interface{}{
        "log_files": []interface{}{log1Content, log2Content, ...},
    },
})
```

---

## 3. RouterAgent

### Purpose

RouterAgent dynamically routes inputs to different agents based on conditions, enabling intent-based routing, A/B testing, and conditional workflows.

### Key Features

- **Condition-Based Routing**: Route by keywords, context, patterns
- **Priority Routes**: Higher priority routes checked first
- **Fallback Modes**: Error, default agent, or first route
- **Condition Combinators**: AND, OR, NOT logic
- **Intent Routing**: Classify then route

### Architecture

```
Input
  ↓
Route Evaluation (Priority Order)
  ├─ Route 1: Condition → Agent A
  ├─ Route 2: Condition → Agent B
  ├─ Route 3: Condition → Agent C
  └─ No Match → Fallback
        ↓
    Selected Agent
        ↓
      Output
```

### Basic Usage

```go
// Create specialized agents
searchAgent := agents.NewLlmAgent(...)
calcAgent := agents.NewLlmAgent(...)
defaultAgent := agents.NewLlmAgent(...)

// Create router
router := agents.NewRouter().
    WithName("intent-router").
    AddRoute("search", agents.KeywordCondition("search", "find", "lookup"), searchAgent).
    AddRoute("calc", agents.KeywordCondition("calculate", "compute"), calcAgent).
    WithDefault(defaultAgent).
    Build()

// Route based on input
output, err := router.Execute(ctx, &agent.AgentInput{
    Instruction: "search for AI trends",
})
// Routes to searchAgent

output2, err := router.Execute(ctx, &agent.AgentInput{
    Instruction: "calculate 2+2",
})
// Routes to calcAgent
```

### Priority Routing

```go
// Higher priority routes checked first
router := agents.NewRouter().
    AddRouteWithPriority("urgent", urgentCondition, urgentAgent, 100).
    AddRouteWithPriority("normal", normalCondition, normalAgent, 50).
    AddRouteWithPriority("low", lowCondition, lowAgent, 10).
    Build()
```

### Built-in Conditions

#### Keyword Condition

```go
condition := agents.KeywordCondition("search", "find", "lookup")
// Matches if any keyword present in instruction
```

#### Context Key Condition

```go
condition := agents.ContextKeyCondition("tier", "premium")
// Matches if context["tier"] == "premium"
```

#### Context Exists Condition

```go
condition := agents.ContextExistsCondition("user_id")
// Matches if context has "user_id" key
```

#### Prefix Condition

```go
condition := agents.PrefixCondition("/api/")
// Matches if instruction starts with "/api/"
```

### Condition Combinators

```go
// AND: All conditions must match
andCondition := agents.AndCondition(
    agents.KeywordCondition("search"),
    agents.ContextKeyCondition("tier", "premium"),
)

// OR: Any condition matches
orCondition := agents.OrCondition(
    agents.KeywordCondition("help"),
    agents.KeywordCondition("support"),
)

// NOT: Condition doesn't match
notCondition := agents.NotCondition(
    agents.KeywordCondition("exclude"),
)

// Complex combinations
complexCondition := agents.AndCondition(
    agents.KeywordCondition("search"),
    agents.OrCondition(
        agents.ContextKeyCondition("tier", "premium"),
        agents.ContextKeyCondition("tier", "enterprise"),
    ),
)
```

### Custom Conditions

```go
// Custom condition function
customCondition := func(input *agent.AgentInput) bool {
    // Check instruction length
    return len(input.Instruction) > 100
}

router.AddRoute("long-query", customCondition, detailedAgent)
```

### Fallback Modes

```go
// Error if no match (default)
router := agents.NewRouter().
    WithFallback(agents.FallbackError).
    Build()

// Use default agent
router := agents.NewRouter().
    WithDefault(defaultAgent).
    WithFallback(agents.FallbackDefault).
    Build()

// Use first route as fallback
router := agents.NewRouter().
    WithFallback(agents.FallbackFirst).
    Build()
```

### Example: Multi-Tier Service

```go
// Premium agent with advanced features
premiumAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "premium-service",
    ModelName:   "gpt-4",
    Temperature: 0.7,
}, gpt4Provider, advancedTools)

// Basic agent with standard features
basicAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "basic-service",
    ModelName:   "gpt-3.5",
    Temperature: 0.7,
}, gpt35Provider, basicTools)

// Route by tier
router := agents.NewRouter().
    WithName("tier-router").
    AddRoute("premium", agents.ContextKeyCondition("tier", "premium"), premiumAgent).
    AddRoute("enterprise", agents.ContextKeyCondition("tier", "enterprise"), premiumAgent).
    WithDefault(basicAgent).
    Build()

// Usage
output, err := router.Execute(ctx, &agent.AgentInput{
    Instruction: "Analyze this data",
    Context: map[string]interface{}{
        "tier":    "premium",
        "user_id": "12345",
    },
})
```

---

## 4. LoadBalancerAgent

### Purpose

LoadBalancerAgent distributes requests across multiple agents using various load balancing strategies.

### Strategies

- **Round Robin**: Distribute requests evenly in rotation
- **Random**: Select agent randomly
- **Least Loaded**: Select agent with least load (requires metrics)

### Usage

```go
// Create pool of agents
agent1 := agents.NewLlmAgent(...)
agent2 := agents.NewLlmAgent(...)
agent3 := agents.NewLlmAgent(...)

// Create load balancer
lb := agents.NewLoadBalancerAgent(agents.LoadBalancerAgentConfig{
    Name:     "load-balancer",
    Agents:   []agent.Agent{agent1, agent2, agent3},
    Strategy: agents.RoundRobin,
})

// Requests distributed in round-robin
output1, _ := lb.Execute(ctx, input)  // → agent1
output2, _ := lb.Execute(ctx, input)  // → agent2
output3, _ := lb.Execute(ctx, input)  // → agent3
output4, _ := lb.Execute(ctx, input)  // → agent1 (wraps around)
```

---

## 5. FanOutAgent

### Purpose

FanOutAgent distributes work to multiple agents in parallel without a reduce phase. Each split goes to a dedicated worker.

### Usage

```go
// Create worker agents
workers := []agent.Agent{worker1, worker2, worker3}

// Splitter creates 3 tasks
splitter := func(input *agent.AgentInput) ([]*agent.AgentInput, error) {
    return []*agent.AgentInput{
        {Instruction: "task1", Context: map[string]interface{}{"data": data1}},
        {Instruction: "task2", Context: map[string]interface{}{"data": data2}},
        {Instruction: "task3", Context: map[string]interface{}{"data": data3}},
    }, nil
}

// Create fan-out
fanOut := agents.NewFanOutAgent(agents.FanOutAgentConfig{
    Name:        "fanout",
    Workers:     workers,
    Splitter:    splitter,
    Parallelism: 3,
})

// Execute - each worker gets its task
output, err := fanOut.Execute(ctx, input)

// Result contains array of outputs
results := output.Result.([]*agent.AgentOutput)
```

---

## 6. IntentRouter

### Purpose

IntentRouter combines classification with routing - first classifies the intent, then routes to the appropriate agent.

### Usage

```go
// Create classifier agent
classifier := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "intent-classifier",
    Instruction: "Classify user intent as: search, calculate, translate, or other",
}, modelProvider, nil)

// Create specialized agents
searchAgent := agents.NewLlmAgent(...)
calcAgent := agents.NewLlmAgent(...)
translateAgent := agents.NewLlmAgent(...)
defaultAgent := agents.NewLlmAgent(...)

// Create intent router
intentRouter := agents.NewIntentRouter(agents.IntentRouterConfig{
    Name:       "intent-router",
    Classifier: classifier,
    IntentToAgent: map[string]agent.Agent{
        "search":    searchAgent,
        "calculate": calcAgent,
        "translate": translateAgent,
    },
    DefaultAgent: defaultAgent,
})

// Automatically classifies then routes
output, err := intentRouter.Execute(ctx, &agent.AgentInput{
    Instruction: "Find information about quantum computers",
})
// Classifier returns "search" → routes to searchAgent
```

---

## Complex Compositions

### Map-Reduce with Remote Agents

```go
// Use remote agents as mappers
remoteMapper, _ := agents.NewRemote("mapper", "http://mappers.example.com").Build()
remoteReducer, _ := agents.NewRemote("reducer", "http://reducers.example.com").Build()

mrAgent := agents.NewMapReduce(remoteMapper, remoteReducer, splitter).
    WithParallelism(50).
    Build()
```

### Router with Load Balanced Backends

```go
// Load balanced backend for each route
searchLB := agents.NewLoadBalancerAgent(agents.LoadBalancerAgentConfig{
    Agents:   []agent.Agent{searchAgent1, searchAgent2, searchAgent3},
    Strategy: agents.RoundRobin,
})

calcLB := agents.NewLoadBalancerAgent(agents.LoadBalancerAgentConfig{
    Agents:   []agent.Agent{calcAgent1, calcAgent2},
    Strategy: agents.RoundRobin,
})

router := agents.NewRouter().
    AddRoute("search", agents.KeywordCondition("search"), searchLB).
    AddRoute("calc", agents.KeywordCondition("calculate"), calcLB).
    Build()
```

### Sequential Map-Reduce Pipeline

```go
// Stage 1: Map-reduce for data collection
dataCollection := agents.NewMapReduce(collector, aggregator, splitter1).Build()

// Stage 2: Map-reduce for analysis
dataAnalysis := agents.NewMapReduce(analyzer, synthesizer, splitter2).Build()

// Stage 3: Final formatting
formatter := agents.NewLlmAgent(...)

// Combine as sequential
pipeline := agents.NewSequential().
    Add(dataCollection).
    Add(dataAnalysis).
    Add(formatter).
    WithPassOutput(true).
    Build()
```

---

## Performance Considerations

### RemoteAgent

- **Network Latency**: Primary bottleneck
- **Connection Pooling**: Reuse HTTP connections
- **Caching**: Cache agent cards to reduce discovery calls
- **Timeout**: Set appropriate timeouts for network calls
- **Retry**: Limit retries to avoid cascading failures

### MapReduceAgent

- **Parallelism**: Balance between throughput and resource usage
- **Chunk Size**: Larger chunks = less overhead, smaller = better distribution
- **Memory**: Monitor memory usage with large result sets
- **Reducer Overhead**: Consider if reduce is needed (use FanOut if not)

### RouterAgent

- **Condition Complexity**: Keep conditions fast (< 1ms)
- **Priority**: Use priorities to short-circuit expensive checks
- **Caching**: Cache classification results if applicable

---

## Testing

Run extended agent tests:

```bash
# All tests
go test ./pkg/agents -v -run Extended

# Specific tests
go test ./pkg/agents -run TestRemoteAgent
go test ./pkg/agents -run TestMapReduceAgent
go test ./pkg/agents -run TestRouterAgent
```

---

## Summary

Phase 3.5 Extended adds 6 powerful agent types:

| Agent Type | Purpose | Key Benefit |
|------------|---------|-------------|
| **RemoteAgent** | Invoke remote A2A agents | Distributed agent architecture |
| **MapReduceAgent** | Distributed data processing | Spark-style parallelism |
| **RouterAgent** | Conditional routing | Intent-based workflows |
| **LoadBalancerAgent** | Distribute load | High availability |
| **FanOutAgent** | Parallel distribution | Simpler than map-reduce |
| **IntentRouter** | Classify then route | Smart routing |

Combined with the core agent types (Llm, Sequential, Parallel, Loop), this gives us a comprehensive toolkit for building sophisticated AI agent systems.

---

## References

- [Phase 3.5 Core Agent Types](./PHASE3_5_AGENT_TYPES.md)
- [A2A Protocol](../pkg/a2a/protocol.go)
- [Agent Base Implementation](../pkg/agent/agent.go)
- [MapReduce Paper](https://research.google/pubs/pub62/)

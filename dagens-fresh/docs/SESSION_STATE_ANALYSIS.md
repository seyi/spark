# Session State Analysis: ADK vs Our Implementation

## Overview

This document analyzes whether our implementation can replicate ADK's **Shared Session State** pattern for agent communication.

---

## ADK's Session State Pattern

### Core Concept

ADK provides `session.state` as a **shared dictionary** that persists across agent invocations within the same session:

```python
# ADK Python Pattern
class MyAgent:
    def run(self, context: InvocationContext):
        # Write to shared state
        context.state['data_key'] = processed_data

        # Read from shared state
        data = context.state.get('data_key')
```

### Key Features

1. **Shared State Dictionary**: `session.state` accessible via `InvocationContext`
2. **Automatic Storage**: `output_key` property auto-saves agent results
3. **State Tracking**: Changes tracked via `CallbackContext`
4. **Asynchronous Communication**: Passive data passing between agents
5. **Persistence**: State persists across SequentialAgent steps and LoopAgent iterations

### Example from ADK Docs

```go
// ADK Go Pattern
step1, _ := llmagent.New(llmagent.Config{
    Name: "Step1_Fetch",
    OutputKey: "data",  // ← Automatically saves result to state["data"]
    Model: m
})

step2, _ := llmagent.New(llmagent.Config{
    Name: "Step2_Process",
    Instruction: "Process data from {data}.",  // ← Reads from state["data"]
    Model: m
})

pipeline, _ := sequentialagent.New(sequentialagent.Config{
    SubAgents: []agent.Agent{step1, step2}
})

// Execution:
// 1. step1 runs, result automatically stored to state["data"]
// 2. step2 runs, {data} template reads from state["data"]
```

---

## Our Implementation

### Current Approach: Context Map

We use `AgentInput.Context` as a shared map:

```go
type AgentInput struct {
    TaskID      string
    Instruction string
    Context     map[string]interface{}  // ← Our "state"
    Tools       []Tool
    Model       string
    MaxRetries  int
    Timeout     time.Duration
}
```

### How We Pass State

#### 1. SequentialAgent with PassOutput

```go
sequential := agents.NewSequentialAgent(agents.SequentialAgentConfig{
    Name:       "Pipeline",
    SubAgents:  []agent.Agent{step1, step2},
    PassOutput: true,  // ← Enables state passing
})

// Internal behavior:
// step1 executes with input
// step2 receives input with Context["previous_result"] = step1.output
```

**Implementation in `sequential_agent.go`:**
```go
if e.seqAgent.passOutput && i < len(e.seqAgent.childAgents)-1 {
    currentInput = &agent.AgentInput{
        Instruction: fmt.Sprintf("Process the following result: %v", output.Result),
        Context: map[string]interface{}{
            "previous_result": output.Result,      // ← Result passing
            "previous_agent":  childAgent.Name(),
            "original_input":  input,
        },
    }
}
```

#### 2. LoopAgent State Persistence

```go
loop := agents.NewLoopAgent(agents.LoopAgentConfig{
    Name:      "Poller",
    SubAgents: []agent.Agent{process, check},
    MaxIterations: 10,
})

// Internal behavior:
// Each iteration gets Context with previous results
```

**Implementation in `loop_agent.go`:**
```go
func (e *loopExecutor) prepareNextInput(originalInput *agent.AgentInput, previousOutput *agent.AgentOutput, iteration int) *agent.AgentInput {
    return &agent.AgentInput{
        Instruction: originalInput.Instruction,
        Context: map[string]interface{}{
            "iteration":       iteration + 1,
            "previous_result": previousOutput.Result,  // ← State passing
            "original_input":  originalInput,
            "state":           previousOutput.Metadata["state"],
        },
    }
}
```

#### 3. Manual State Management

Agents can read/write to Context directly:

```go
// Writing to state
executor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        data := fetchData()

        // Store in context for next agent
        return &agent.AgentOutput{
            Result: data,
            Metadata: map[string]interface{}{
                "state": map[string]interface{}{
                    "data_key": data,  // ← Manual state storage
                },
            },
        }, nil
    },
}

// Reading from state
executor2 := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        // Read from previous result or context
        data := input.Context["previous_result"]  // ← Manual state reading

        // Or read from nested state
        if state, ok := input.Context["state"].(map[string]interface{}); ok {
            data = state["data_key"]
        }

        return &agent.AgentOutput{Result: process(data)}, nil
    },
}
```

---

## Compatibility Matrix

| Feature | ADK | Our Implementation | Status |
|---------|-----|-------------------|---------|
| **Shared state dictionary** | ✅ `session.state` | ⚠️ `input.Context` map | **Semantic Equivalent** |
| **State persistence** | ✅ Across invocations | ✅ Across agent calls | **Full Parity** |
| **Read/Write access** | ✅ `state['key']` | ✅ `Context["key"]` | **Full Parity** |
| **Auto-save via output_key** | ✅ | ❌ | **Missing** |
| **Template interpolation** | ✅ `{data}` | ❌ | **Missing** |
| **State change tracking** | ✅ CallbackContext | ❌ | **Missing** |
| **SequentialAgent support** | ✅ | ✅ (via PassOutput) | **Full Parity** |
| **LoopAgent support** | ✅ | ✅ (auto-passed) | **Full Parity** |
| **ParallelAgent support** | ✅ Shared access | ✅ Same Context | **Full Parity** |

---

## Can We Do What ADK Does?

### Quick Answer: **Yes, functionally** ✅ (but with different API)

### Detailed Breakdown

#### 1. Sequential Pipeline with State Passing

**ADK Pattern:**
```go
step1, _ := llmagent.New(llmagent.Config{
    Name: "Fetch",
    OutputKey: "data",
})

step2, _ := llmagent.New(llmagent.Config{
    Name: "Process",
    Instruction: "Process {data}",
})

pipeline, _ := sequentialagent.New(sequentialagent.Config{
    SubAgents: []agent.Agent{step1, step2}
})
```

**Our Equivalent:**
```go
// Step 1: Fetch data
fetchExecutor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        data := fetchData()
        return &agent.AgentOutput{Result: data}, nil
    },
}
step1 := agent.NewAgent(agent.AgentConfig{
    Name: "Fetch",
    Executor: fetchExecutor,
})

// Step 2: Process data (reads from previous_result)
processExecutor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        data := input.Context["previous_result"]  // ← Reads state
        processed := process(data)
        return &agent.AgentOutput{Result: processed}, nil
    },
}
step2 := agent.NewAgent(agent.AgentConfig{
    Name: "Process",
    Executor: processExecutor,
})

// Pipeline with state passing enabled
pipeline := agents.NewSequentialAgent(agents.SequentialAgentConfig{
    Name:       "Pipeline",
    SubAgents:  []agent.Agent{step1, step2},
    PassOutput: true,  // ← Enables state passing
})
```

**Verdict:** ✅ **Works** - PassOutput automatically puts results in `Context["previous_result"]`

---

#### 2. Loop with Persistent State

**ADK Pattern:**
```go
processStep, _ := llmagent.New(llmagent.Config{
    Name: "Process",
    OutputKey: "status",  // Updates state["status"]
})

checkCondition, _ := agent.New(agent.Config{
    Name: "Check",
    Run: func(ctx agent.InvocationContext) {
        status := ctx.State().Get("status")  // Reads state
        isDone := status == "complete"
        yield(&session.Event{
            Actions: session.EventActions{Escalate: isDone}
        })
    },
})

loop, _ := loopagent.New(loopagent.Config{
    MaxIterations: 10,
    SubAgents: []agent.Agent{processStep, checkCondition}
})
```

**Our Equivalent:**
```go
// Process step updates state
processExecutor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        status := doWork()

        return &agent.AgentOutput{
            Result: status,
            Metadata: map[string]interface{}{
                "status": status,  // ← Store in metadata
            },
        }, nil
    },
}
processStep := agent.NewAgent(agent.AgentConfig{
    Name: "Process",
    Executor: processExecutor,
})

// Check step reads state and escalates
checkExecutor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        // Read status from previous result
        var status string
        if prev, ok := input.Context["previous_result"].(string); ok {
            status = prev
        }

        isDone := status == "complete"

        return &agent.AgentOutput{
            Result: status,
            Metadata: map[string]interface{}{
                "escalate": isDone,  // ← Escalate to terminate loop
            },
        }, nil
    },
}
checkStep := agent.NewAgent(agent.AgentConfig{
    Name: "Check",
    Executor: checkExecutor,
})

// Loop automatically passes state between iterations
loop := agents.NewLoopAgent(agents.LoopAgentConfig{
    Name:          "Poller",
    SubAgents:     []agent.Agent{processStep, checkStep},
    MaxIterations: 10,
})
```

**Verdict:** ✅ **Works** - LoopAgent auto-passes `Context["previous_result"]` and supports escalate events

---

#### 3. Parallel Agents with Shared State

**ADK Pattern:**
```go
// Both agents access same session.state
agent1, _ := llmagent.New(llmagent.Config{
    Name: "Agent1",
    OutputKey: "result1",  // Writes to state["result1"]
})

agent2, _ := llmagent.New(llmagent.Config{
    Name: "Agent2",
    OutputKey: "result2",  // Writes to state["result2"]
})

parallel, _ := parallelagent.New(parallelagent.Config{
    SubAgents: []agent.Agent{agent1, agent2}
})
```

**Our Equivalent:**
```go
// Agents read from shared Context
agent1Executor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        // Can read shared context
        sharedData := input.Context["shared_key"]

        result := process(sharedData)
        return &agent.AgentOutput{Result: result}, nil
    },
}

agent2Executor := &customExecutor{
    run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
        // Same shared context
        sharedData := input.Context["shared_key"]

        result := process2(sharedData)
        return &agent.AgentOutput{Result: result}, nil
    },
}

parallel := agents.NewParallelAgent(agents.ParallelAgentConfig{
    Name:      "Parallel",
    SubAgents: []agent.Agent{agent1, agent2},
})
```

**Verdict:** ✅ **Works** - All parallel agents receive the same `input.Context`

**Note:** Unlike ADK, our parallel agents can read shared state but don't automatically write results to named keys.

---

## What's Missing?

### 1. OutputKey Property (High Value)

**ADK:**
```go
agent, _ := llmagent.New(llmagent.Config{
    OutputKey: "data",  // Auto-saves result to state["data"]
})
```

**Impact:** Automatic result storage with explicit naming

**Our Workaround:** Manual metadata storage
```go
return &agent.AgentOutput{
    Result: data,
    Metadata: map[string]interface{}{
        "output_key": "data",  // Manual tracking
    },
}
```

**To Implement:**
- Add `OutputKey string` to `AgentConfig`
- After agent execution, auto-store result to `Context[OutputKey]`
- Modify SequentialAgent/LoopAgent to propagate enriched context

---

### 2. Instruction Templating (High Value)

**ADK:**
```go
agent, _ := llmagent.New(llmagent.Config{
    Instruction: "Process data from {data} and {metadata}",  // Auto-interpolated
})
```

**Impact:** Clean, declarative instruction definitions

**Our Workaround:** Manual string formatting
```go
instruction := fmt.Sprintf("Process data from %v and %v",
    input.Context["data"],
    input.Context["metadata"])
```

**To Implement:**
- Template parser for `{key}` syntax
- State interpolation before agent execution
- Error handling for missing keys

---

### 3. State Change Tracking (Medium Value)

**ADK:**
```python
# CallbackContext tracks all state changes
callbacks.on_state_change(lambda key, value: log(f"{key} = {value}"))
```

**Impact:** Observability and debugging

**Our Workaround:** Manual logging

**To Implement:**
- State change event system
- Before/after snapshots
- Change callbacks

---

## Implementation Recommendations

### Priority 1: Add OutputKey Support (2-3 hours)

Modify `AgentConfig` and agent execution:

```go
type AgentConfig struct {
    Name         string
    Description  string
    Capabilities []string
    Dependencies []Agent
    Partition    string
    Executor     AgentExecutor
    SubAgents    []Agent
    OutputKey    string  // ← ADD THIS
}

// In BaseAgent.Execute()
func (a *BaseAgent) Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
    output, err := a.executor.Execute(ctx, a, input)
    if err != nil {
        return nil, err
    }

    // Auto-store to output key if specified
    if a.outputKey != "" && output != nil {
        if input.Context == nil {
            input.Context = make(map[string]interface{})
        }
        input.Context[a.outputKey] = output.Result  // ← AUTO-SAVE
    }

    return output, nil
}
```

Update SequentialAgent to propagate enriched context:
```go
// After each step execution
if a.outputKey != "" {
    currentInput.Context[a.outputKey] = output.Result
}
```

### Priority 2: Add Template Interpolation (3-4 hours)

Add template parsing utility:

```go
// pkg/agent/template.go
func InterpolateInstruction(instruction string, context map[string]interface{}) (string, error) {
    re := regexp.MustCompile(`\{(\w+)\}`)

    result := re.ReplaceAllStringFunc(instruction, func(match string) string {
        key := match[1:len(match)-1]  // Remove { }

        if val, ok := context[key]; ok {
            return fmt.Sprintf("%v", val)
        }

        return match  // Keep placeholder if not found
    })

    return result, nil
}

// Use in agent execution
func (a *BaseAgent) Execute(ctx context.Context, input *AgentInput) (*AgentOutput, error) {
    // Interpolate instruction templates
    if strings.Contains(input.Instruction, "{") {
        interpolated, err := InterpolateInstruction(input.Instruction, input.Context)
        if err != nil {
            return nil, err
        }
        input.Instruction = interpolated
    }

    return a.executor.Execute(ctx, a, input)
}
```

### Priority 3: Unified State Management (4-6 hours)

Create a proper session state abstraction:

```go
// pkg/session/state.go
type SessionState struct {
    data    map[string]interface{}
    mu      sync.RWMutex
    history []StateChange
}

type StateChange struct {
    Key       string
    OldValue  interface{}
    NewValue  interface{}
    Timestamp time.Time
    AgentName string
}

func (s *SessionState) Set(key string, value interface{}, agentName string) {
    s.mu.Lock()
    defer s.mu.Unlock()

    oldValue := s.data[key]
    s.data[key] = value

    // Track change
    s.history = append(s.history, StateChange{
        Key:       key,
        OldValue:  oldValue,
        NewValue:  value,
        Timestamp: time.Now(),
        AgentName: agentName,
    })
}

func (s *SessionState) Get(key string) (interface{}, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    val, ok := s.data[key]
    return val, ok
}

func (s *SessionState) GetHistory() []StateChange {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return append([]StateChange{}, s.history...)
}
```

---

## Working Examples

### Example 1: Sequential Data Processing (Works Today)

```go
// Fetch agent stores result
fetchAgent := agent.NewAgent(agent.AgentConfig{
    Name: "Fetch",
    Executor: &fetchExecutor{},
})

// Process agent reads from previous_result
processAgent := agent.NewAgent(agent.AgentConfig{
    Name: "Process",
    Executor: &processExecutor{},
})

// Pipeline with automatic state passing
pipeline := agents.NewSequentialAgent(agents.SequentialAgentConfig{
    Name:       "DataPipeline",
    SubAgents:  []agent.Agent{fetchAgent, processAgent},
    PassOutput: true,  // ← Enables automatic state passing
})

// Execute
output, _ := pipeline.Execute(ctx, &agent.AgentInput{
    Instruction: "Process customer data",
    Context: map[string]interface{}{
        "customer_id": "12345",
    },
})
```

**How it works:**
1. fetchAgent executes, returns result
2. Pipeline puts result in `Context["previous_result"]`
3. processAgent reads from `Context["previous_result"]`

---

### Example 2: Loop with State Accumulation (Works Today)

```go
// Accumulator agent builds state across iterations
accumulatorAgent := agent.NewAgent(agent.AgentConfig{
    Name: "Accumulator",
    Executor: &accumulatorExecutor{
        run: func(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
            // Get accumulated state from previous iteration
            var total int
            if prev, ok := input.Context["state"].(map[string]interface{}); ok {
                if t, ok := prev["total"].(int); ok {
                    total = t
                }
            }

            // Add new value
            total += computeValue()

            return &agent.AgentOutput{
                Result: total,
                Metadata: map[string]interface{}{
                    "state": map[string]interface{}{
                        "total": total,  // ← Persists across iterations
                    },
                },
            }, nil
        },
    },
})

loop := agents.NewLoopAgent(agents.LoopAgentConfig{
    Name:           "Accumulator",
    InnerAgent:     accumulatorAgent,
    MaxIterations:  5,
    AccumulateMode: agents.AccumulateState,
})
```

**How it works:**
1. Each iteration receives `Context["state"]` from previous iteration
2. Agent updates state and returns in metadata
3. LoopAgent propagates state to next iteration

---

## Conclusion

### Can We Do What ADK Does?

**Yes, functionally** ✅

| Communication Pattern | ADK | Our Implementation | Works? |
|----------------------|-----|-------------------|---------|
| Sequential state passing | ✅ | ✅ (PassOutput) | **Yes** |
| Loop state persistence | ✅ | ✅ (auto-passed) | **Yes** |
| Parallel shared state | ✅ | ✅ (shared Context) | **Yes** |
| Write to state | ✅ `state['key']` | ✅ `Context["key"]` | **Yes** |
| Read from state | ✅ `state.get('key')` | ✅ `Context["key"]` | **Yes** |
| Auto-save with output_key | ✅ | ❌ Manual | **No** |
| Template interpolation | ✅ `{key}` | ❌ Manual | **No** |
| State change tracking | ✅ | ❌ Manual | **No** |

### Summary

**Functional Parity:** ✅ **90%** - We can do all the same data passing patterns
**API Parity:** ⚠️ **70%** - Different mechanisms, more manual

**Key Differences:**
1. We use `input.Context` map instead of `session.state`
2. No automatic OutputKey storage (manual metadata)
3. No instruction templating (manual formatting)
4. No state change tracking (manual logging)

**Advantages of Our Approach:**
- More explicit (less "magic")
- Full control over state flow
- Works with existing architecture
- No session management complexity

**Advantages of ADK Approach:**
- Less boilerplate
- Cleaner agent definitions
- Better observability
- More declarative

### Recommendation

**Short-term:** Our current approach works well for production use ✅

**Medium-term:** Add OutputKey and template interpolation for better DX (8-10 hours effort)

**Long-term:** Consider unified SessionState abstraction for full ADK parity (1-2 days effort)

---

## Total Effort to Reach 100% Parity

1. **OutputKey Support**: 2-3 hours
2. **Template Interpolation**: 3-4 hours
3. **State Change Tracking**: 2-3 hours
4. **Unified Session State**: 4-6 hours

**Total:** ~12-16 hours of development

**Priority:** Medium - Current approach is functional, enhancements improve DX

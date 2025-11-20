# State Change Callbacks: Feasibility Analysis

## Overview

This document critically examines whether our implementation can support ADK's state change callback pattern:

```python
# ADK Python Pattern
callbacks.on_state_change(lambda key, value: log(f"{key} = {value}"))
```

**Quick Answer:** Yes, it's feasible ✅ (with multiple implementation approaches)

---

## ADK's Callback Pattern

### What It Does

In ADK, state change callbacks provide **observability** into agent communication:

```python
# Python ADK
def log_state_change(key: str, value: Any):
    print(f"State changed: {key} = {value}")

# Register callback
callbacks.on_state_change(log_state_change)

# Or with lambda
callbacks.on_state_change(lambda k, v: print(f"{k} = {v}"))

# Now when any agent writes to state:
context.state['data'] = processed_data  # ← Triggers callback
# Output: "State changed: data = processed_data"
```

### Key Features

1. **Registration**: Register multiple callbacks
2. **Async Notification**: Callbacks fired when state changes
3. **Full Access**: Receive key, old value, new value
4. **Non-blocking**: Callbacks don't block agent execution
5. **Debugging**: Primary use case is observability/logging

---

## Implementation Feasibility in Go

### Challenges

| Challenge | Go Reality | Feasible? |
|-----------|-----------|-----------|
| Lambda functions | Go has anonymous functions/closures | ✅ Yes |
| Callback registration | Standard pattern in Go | ✅ Yes |
| State observation | Need wrapper or interceptor | ✅ Yes |
| Thread safety | Requires mutex/sync | ✅ Yes |
| Performance overhead | Some cost, but acceptable | ✅ Yes |
| Type safety | Go is statically typed | ⚠️ Requires interface{} |

**Verdict:** Completely feasible with standard Go patterns ✅

---

## Implementation Approaches

### Approach 1: Observable Context Map (Recommended)

**Concept:** Wrap the Context map in an observable struct that triggers callbacks on changes.

```go
// pkg/agent/observable_context.go

package agent

import (
    "sync"
    "time"
)

// StateChangeCallback is called when state changes
type StateChangeCallback func(key string, oldValue, newValue interface{})

// ObservableContext wraps a context map with change tracking
type ObservableContext struct {
    data      map[string]interface{}
    mu        sync.RWMutex
    callbacks []StateChangeCallback
    history   []StateChange
}

// StateChange records a change to context
type StateChange struct {
    Key       string
    OldValue  interface{}
    NewValue  interface{}
    Timestamp time.Time
    AgentName string
}

// NewObservableContext creates a new observable context
func NewObservableContext() *ObservableContext {
    return &ObservableContext{
        data:      make(map[string]interface{}),
        callbacks: []StateChangeCallback{},
        history:   []StateChange{},
    }
}

// Set sets a value and triggers callbacks
func (c *ObservableContext) Set(key string, value interface{}) {
    c.mu.Lock()

    // Get old value for callback
    oldValue := c.data[key]

    // Update value
    c.data[key] = value

    // Record change
    change := StateChange{
        Key:       key,
        OldValue:  oldValue,
        NewValue:  value,
        Timestamp: time.Now(),
    }
    c.history = append(c.history, change)

    // Copy callbacks to avoid holding lock
    callbacks := make([]StateChangeCallback, len(c.callbacks))
    copy(callbacks, c.callbacks)

    c.mu.Unlock()

    // Fire callbacks asynchronously (non-blocking)
    for _, callback := range callbacks {
        go callback(key, oldValue, value)
    }
}

// Get retrieves a value
func (c *ObservableContext) Get(key string) (interface{}, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    val, ok := c.data[key]
    return val, ok
}

// Delete removes a value and triggers callbacks
func (c *ObservableContext) Delete(key string) {
    c.mu.Lock()

    oldValue := c.data[key]
    delete(c.data, key)

    change := StateChange{
        Key:       key,
        OldValue:  oldValue,
        NewValue:  nil,
        Timestamp: time.Now(),
    }
    c.history = append(c.history, change)

    callbacks := make([]StateChangeCallback, len(c.callbacks))
    copy(callbacks, c.callbacks)

    c.mu.Unlock()

    for _, callback := range callbacks {
        go callback(key, oldValue, nil)
    }
}

// OnStateChange registers a callback for state changes
func (c *ObservableContext) OnStateChange(callback StateChangeCallback) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.callbacks = append(c.callbacks, callback)
}

// GetHistory returns all state changes
func (c *ObservableContext) GetHistory() []StateChange {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return append([]StateChange{}, c.history...)
}

// AsMap returns a copy of the internal map (for compatibility)
func (c *ObservableContext) AsMap() map[string]interface{} {
    c.mu.RLock()
    defer c.mu.RUnlock()

    result := make(map[string]interface{}, len(c.data))
    for k, v := range c.data {
        result[k] = v
    }
    return result
}

// FromMap populates from an existing map
func (c *ObservableContext) FromMap(m map[string]interface{}) {
    c.mu.Lock()
    defer c.mu.Unlock()

    for k, v := range m {
        c.data[k] = v
    }
}
```

#### Usage Example

```go
// Create observable context
ctx := agent.NewObservableContext()

// Register callback (ADK pattern)
ctx.OnStateChange(func(key string, oldValue, newValue interface{}) {
    log.Printf("State changed: %s = %v (was: %v)", key, newValue, oldValue)
})

// Register multiple callbacks
ctx.OnStateChange(func(key, old, new interface{}) {
    metrics.RecordStateChange(key, new)
})

ctx.OnStateChange(func(key, old, new interface{}) {
    if key == "error" && new != nil {
        alerts.TriggerAlert("Agent error: %v", new)
    }
})

// Now when agents modify state, callbacks fire
ctx.Set("data", processedData)  // ← Triggers all callbacks
// Output: "State changed: data = processedData (was: <nil>)"

ctx.Set("status", "complete")   // ← Triggers all callbacks
// Output: "State changed: status = complete (was: <nil>)"
```

**Pros:**
- ✅ Clean API matching ADK's pattern
- ✅ Thread-safe
- ✅ Non-blocking (goroutines)
- ✅ History tracking included
- ✅ Easy to integrate

**Cons:**
- ⚠️ Requires changing AgentInput.Context from map to ObservableContext
- ⚠️ Some memory overhead for history
- ⚠️ Migration needed for existing code

---

### Approach 2: Event Bus Pattern

**Concept:** Central event bus that agents publish state changes to.

```go
// pkg/events/event_bus.go

package events

import (
    "sync"
)

type EventType string

const (
    EventStateChange EventType = "state_change"
    EventAgentStart  EventType = "agent_start"
    EventAgentEnd    EventType = "agent_end"
)

// Event represents a system event
type Event struct {
    Type      EventType
    Data      interface{}
    AgentName string
    Timestamp time.Time
}

// StateChangeEvent contains state change details
type StateChangeEvent struct {
    Key      string
    OldValue interface{}
    NewValue interface{}
}

// EventHandler processes events
type EventHandler func(Event)

// EventBus manages event subscriptions and publishing
type EventBus struct {
    handlers map[EventType][]EventHandler
    mu       sync.RWMutex
}

// NewEventBus creates a new event bus
func NewEventBus() *EventBus {
    return &EventBus{
        handlers: make(map[EventType][]EventHandler),
    }
}

// Subscribe registers a handler for an event type
func (b *EventBus) Subscribe(eventType EventType, handler EventHandler) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.handlers[eventType] = append(b.handlers[eventType], handler)
}

// Publish publishes an event to all subscribers
func (b *EventBus) Publish(event Event) {
    b.mu.RLock()
    handlers := b.handlers[event.Type]
    b.mu.RUnlock()

    // Fire handlers asynchronously
    for _, handler := range handlers {
        go handler(event)
    }
}

// OnStateChange is syntactic sugar for subscribing to state changes
func (b *EventBus) OnStateChange(callback func(key string, oldValue, newValue interface{})) {
    b.Subscribe(EventStateChange, func(e Event) {
        if change, ok := e.Data.(StateChangeEvent); ok {
            callback(change.Key, change.OldValue, change.NewValue)
        }
    })
}
```

#### Usage Example

```go
// Global event bus
eventBus := events.NewEventBus()

// Register callback (ADK pattern)
eventBus.OnStateChange(func(key string, old, new interface{}) {
    log.Printf("%s = %v", key, new)
})

// In agent executor
func (e *myExecutor) Execute(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
    data := process()

    // Publish state change event
    eventBus.Publish(events.Event{
        Type:      events.EventStateChange,
        AgentName: ag.Name(),
        Data: events.StateChangeEvent{
            Key:      "data",
            OldValue: input.Context["data"],
            NewValue: data,
        },
        Timestamp: time.Now(),
    })

    // Update context
    input.Context["data"] = data

    return &agent.AgentOutput{Result: data}, nil
}
```

**Pros:**
- ✅ More flexible (can handle any event type)
- ✅ Decoupled from context implementation
- ✅ Easy to add new event types
- ✅ No migration needed

**Cons:**
- ⚠️ More boilerplate (manual event publishing)
- ⚠️ Agents must remember to publish events
- ⚠️ No automatic state change detection

---

### Approach 3: Middleware/Interceptor Pattern

**Concept:** Intercept agent execution to detect state changes.

```go
// pkg/agent/middleware.go

package agent

import (
    "context"
    "reflect"
)

// ExecutionMiddleware wraps agent execution
type ExecutionMiddleware func(Agent, *AgentInput, ExecutorFunc) (*AgentOutput, error)

// ExecutorFunc is the actual execution function
type ExecutorFunc func(context.Context, Agent, *AgentInput) (*AgentOutput, error)

// StateChangeTracker middleware tracks state changes
func StateChangeTracker(callbacks []StateChangeCallback) ExecutionMiddleware {
    return func(ag Agent, input *AgentInput, next ExecutorFunc) (*AgentOutput, error) {
        // Take snapshot of state before execution
        before := make(map[string]interface{})
        for k, v := range input.Context {
            before[k] = v
        }

        // Execute agent
        output, err := next(context.Background(), ag, input)

        // Compare state after execution
        after := input.Context

        // Detect changes
        for key, newValue := range after {
            oldValue, existed := before[key]

            if !existed || !reflect.DeepEqual(oldValue, newValue) {
                // State changed, fire callbacks
                for _, callback := range callbacks {
                    go callback(key, oldValue, newValue)
                }
            }
        }

        // Detect deletions
        for key, oldValue := range before {
            if _, exists := after[key]; !exists {
                // Key was deleted
                for _, callback := range callbacks {
                    go callback(key, oldValue, nil)
                }
            }
        }

        return output, err
    }
}

// WithMiddleware wraps an executor with middleware
func WithMiddleware(executor AgentExecutor, middleware ...ExecutionMiddleware) AgentExecutor {
    return &middlewareExecutor{
        wrapped:    executor,
        middleware: middleware,
    }
}

type middlewareExecutor struct {
    wrapped    AgentExecutor
    middleware []ExecutionMiddleware
}

func (m *middlewareExecutor) Execute(ctx context.Context, ag Agent, input *AgentInput) (*AgentOutput, error) {
    // Build middleware chain
    next := m.wrapped.Execute

    for i := len(m.middleware) - 1; i >= 0; i-- {
        middleware := m.middleware[i]
        currentNext := next

        next = func(ctx context.Context, ag Agent, input *AgentInput) (*AgentOutput, error) {
            return middleware(ag, input, currentNext)
        }
    }

    return next(ctx, ag, input)
}
```

#### Usage Example

```go
// Define callbacks
var callbacks []agent.StateChangeCallback

callbacks = append(callbacks, func(key string, old, new interface{}) {
    log.Printf("State changed: %s = %v", key, new)
})

// Wrap executor with state tracking middleware
baseExecutor := &myExecutor{}
trackedExecutor := agent.WithMiddleware(
    baseExecutor,
    agent.StateChangeTracker(callbacks),
)

// Create agent with tracked executor
myAgent := agent.NewAgent(agent.AgentConfig{
    Name:     "TrackedAgent",
    Executor: trackedExecutor,
})
```

**Pros:**
- ✅ No changes to Context structure
- ✅ Works with existing code
- ✅ Composable (can add other middleware)
- ✅ Automatic change detection

**Cons:**
- ⚠️ Performance overhead (deep comparison)
- ⚠️ Snapshot copies memory
- ⚠️ Can't detect changes within nested structures easily

---

## Comparative Analysis

| Aspect | Observable Context | Event Bus | Middleware |
|--------|-------------------|-----------|------------|
| **API Match** | ✅ Exact | ⚠️ Close | ⚠️ Different |
| **Automatic** | ✅ Yes | ❌ Manual | ✅ Yes |
| **Performance** | ✅ Good | ✅ Good | ⚠️ Overhead |
| **Thread Safety** | ✅ Built-in | ✅ Built-in | ✅ Built-in |
| **Migration** | ⚠️ Required | ✅ None | ✅ None |
| **Flexibility** | ⚠️ Context only | ✅ Any event | ⚠️ Execution only |
| **Complexity** | ⚠️ Medium | ⚠️ Medium | ⚠️ High |

---

## Recommended Implementation

### Phase 1: Event Bus (Short-term)

**Why:** No migration needed, works with existing code

```go
// Add global event bus
var GlobalEventBus = events.NewEventBus()

// Users can register callbacks
GlobalEventBus.OnStateChange(func(key, old, new interface{}) {
    log.Printf("%s = %v", key, new)
})

// Agents manually publish changes (opt-in)
func (e *myExecutor) Execute(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
    oldData := input.Context["data"]
    newData := process()

    if oldData != newData {
        GlobalEventBus.Publish(events.Event{
            Type: events.EventStateChange,
            Data: events.StateChangeEvent{
                Key:      "data",
                OldValue: oldData,
                NewValue: newData,
            },
        })
    }

    input.Context["data"] = newData
    return &agent.AgentOutput{Result: newData}, nil
}
```

**Effort:** 2-3 hours
**Migration:** None
**Parity:** 70% (manual publishing required)

---

### Phase 2: Observable Context (Long-term)

**Why:** Best ADK parity, automatic tracking

```go
// Modify AgentInput
type AgentInput struct {
    TaskID      string
    Instruction string
    Context     *ObservableContext  // ← Changed from map
    Tools       []Tool
    Model       string
}

// Usage (matches ADK exactly)
ctx := agent.NewObservableContext()

// ADK pattern
ctx.OnStateChange(func(key, old, new interface{}) {
    log.Printf("%s = %v", key, new)
})

// Agents just use normally
ctx.Set("data", processedData)  // ← Automatic callback trigger
```

**Effort:** 4-6 hours (+ migration time)
**Migration:** Update AgentInput type
**Parity:** 95% (nearly exact ADK match)

---

## Go-Specific Considerations

### 1. Anonymous Functions (Lambda Equivalent)

**Python:**
```python
callbacks.on_state_change(lambda k, v: print(f"{k} = {v}"))
```

**Go:**
```go
ctx.OnStateChange(func(key string, old, new interface{}) {
    fmt.Printf("%s = %v\n", key, new)
})
```

✅ **Works perfectly** - Go closures are equivalent to Python lambdas

---

### 2. Thread Safety

**Challenge:** Multiple agents modifying state concurrently

**Solution:** Use `sync.RWMutex`

```go
type ObservableContext struct {
    data map[string]interface{}
    mu   sync.RWMutex  // ← Thread-safe access
}

func (c *ObservableContext) Set(key string, value interface{}) {
    c.mu.Lock()         // ← Exclusive lock for writes
    defer c.mu.Unlock()
    c.data[key] = value
}

func (c *ObservableContext) Get(key string) (interface{}, bool) {
    c.mu.RLock()        // ← Shared lock for reads
    defer c.mu.RUnlock()
    val, ok := c.data[key]
    return val, ok
}
```

✅ **Solved** - Standard Go pattern

---

### 3. Async Callbacks (Non-blocking)

**Challenge:** Callbacks shouldn't block agent execution

**Solution:** Fire callbacks in goroutines

```go
func (c *ObservableContext) Set(key string, value interface{}) {
    // ... update state ...

    // Fire callbacks asynchronously
    for _, callback := range c.callbacks {
        go callback(key, oldValue, value)  // ← Non-blocking
    }
}
```

✅ **Solved** - Goroutines are perfect for this

---

### 4. Type Safety

**Challenge:** Go is statically typed, Python uses `Any`

**Solution:** Use `interface{}`

```go
type StateChangeCallback func(key string, oldValue, newValue interface{})
```

⚠️ **Tradeoff:** Less type safety than Python's `Any`, but necessary for flexibility

Users can type assert if needed:
```go
ctx.OnStateChange(func(key string, old, new interface{}) {
    if str, ok := new.(string); ok {
        log.Printf("String value: %s = %s", key, str)
    }
})
```

---

## Performance Considerations

### Memory Overhead

**Observable Context:**
```go
// Extra memory per change
type StateChange struct {
    Key       string          // ~16 bytes
    OldValue  interface{}     // ~16 bytes (pointer)
    NewValue  interface{}     // ~16 bytes
    Timestamp time.Time       // 24 bytes
    AgentName string          // ~16 bytes
}
// Total: ~88 bytes per change
```

For 1000 state changes: ~88 KB (negligible)

**Verdict:** ✅ Acceptable overhead

---

### Callback Execution Time

**Async callbacks:** Don't block main execution

```go
// Callback takes 100ms
ctx.OnStateChange(func(k, o, n interface{}) {
    time.Sleep(100 * time.Millisecond)  // Slow operation
    log.Printf("%s = %v", k, n)
})

// Agent execution not blocked
start := time.Now()
ctx.Set("data", value)  // Returns immediately
fmt.Println(time.Since(start))  // < 1ms
```

**Verdict:** ✅ No performance impact on agents

---

### Snapshot Comparison (Middleware)

**Cost:** O(n) where n = number of keys

```go
// Before: 10 keys
// After: 10 keys
// Comparison: 20 map lookups + 10 DeepEqual calls
```

For typical context sizes (< 50 keys): ~1ms overhead

**Verdict:** ⚠️ Some overhead, but acceptable for debugging

---

## Migration Path

### Step 1: Add Event Bus (Week 1)

```go
// pkg/events/event_bus.go - new file
// Add global event bus
// No breaking changes
```

**Migration:** None
**Impact:** Additive only

---

### Step 2: Helper Functions (Week 2)

```go
// pkg/agent/context_helpers.go
func EmitStateChange(ctx map[string]interface{}, key string, oldValue, newValue interface{}) {
    events.GlobalEventBus.Publish(events.Event{
        Type: events.EventStateChange,
        Data: events.StateChangeEvent{
            Key:      key,
            OldValue: oldValue,
            NewValue: newValue,
        },
    })
}

// Agents can opt-in to emit events
EmitStateChange(input.Context, "data", old, new)
```

**Migration:** Optional opt-in
**Impact:** None on existing code

---

### Step 3: Observable Context (Month 2)

```go
// Modify AgentInput
type AgentInput struct {
    Context *ObservableContext  // ← Breaking change
}

// Provide compatibility helper
func (c *ObservableContext) AsMap() map[string]interface{} {
    return c.data
}
```

**Migration:** Update agent executors
**Impact:** Breaking change, but with compatibility layer

---

## Production Example

### Complete Working Implementation

```go
// 1. Create event bus
eventBus := events.NewEventBus()

// 2. Register callbacks for different use cases

// Logging
eventBus.OnStateChange(func(key string, old, new interface{}) {
    logger.Info("State changed",
        "key", key,
        "old_value", old,
        "new_value", new,
    )
})

// Metrics
eventBus.OnStateChange(func(key string, old, new interface{}) {
    metrics.IncrementCounter("agent.state_changes",
        "key", key,
    )
})

// Alerting
eventBus.OnStateChange(func(key string, old, new interface{}) {
    if key == "error" && new != nil {
        alerts.SendAlert("Agent error detected: %v", new)
    }
})

// Debugging (conditional)
if debug {
    eventBus.OnStateChange(func(key string, old, new interface{}) {
        fmt.Printf("[DEBUG] %s: %v → %v\n", key, old, new)
    })
}

// Audit trail
eventBus.OnStateChange(func(key string, old, new interface{}) {
    auditLog.Record(AuditEntry{
        Type:      "state_change",
        Key:       key,
        OldValue:  fmt.Sprintf("%v", old),
        NewValue:  fmt.Sprintf("%v", new),
        Timestamp: time.Now(),
    })
})

// 3. Agents publish changes (manual for now)
func (e *myExecutor) Execute(ctx context.Context, ag agent.Agent, input *agent.AgentInput) (*agent.AgentOutput, error) {
    oldData := input.Context["data"]
    newData := processData()

    // Publish change
    eventBus.Publish(events.Event{
        Type:      events.EventStateChange,
        AgentName: ag.Name(),
        Data: events.StateChangeEvent{
            Key:      "data",
            OldValue: oldData,
            NewValue: newData,
        },
        Timestamp: time.Now(),
    })

    input.Context["data"] = newData
    return &agent.AgentOutput{Result: newData}, nil
}
```

---

## Conclusion

### Can We Do This?

**Yes, absolutely** ✅

| Aspect | Feasibility | Notes |
|--------|-------------|-------|
| **Technical** | ✅ 100% | Standard Go patterns |
| **API Match** | ✅ 95% | Close to ADK with minor differences |
| **Performance** | ✅ Good | Negligible overhead |
| **Thread Safety** | ✅ Yes | sync.RWMutex handles it |
| **Migration** | ⚠️ Gradual | Phase 1 (Event Bus) = no migration |

---

### Recommended Approach

**Short-term (2-3 hours):**
- Implement Event Bus pattern
- Add `GlobalEventBus.OnStateChange()` API
- Provide helper functions for agents
- Fully backward compatible

**Medium-term (4-6 hours):**
- Implement ObservableContext
- Migrate AgentInput.Context type
- Automatic state change detection
- 95% ADK parity

**Long-term (Optional):**
- Add middleware for automatic tracking
- Enhance with batching, filtering
- Integration with distributed tracing

---

### Example Usage (Final API)

```go
// Matches ADK pattern almost exactly
agent.GlobalEventBus.OnStateChange(func(key string, old, new interface{}) {
    log.Printf("%s = %v", key, new)
})

// Or with observable context (future)
ctx := agent.NewObservableContext()
ctx.OnStateChange(func(key, old, new interface{}) {
    log.Printf("%s = %v", key, new)
})
```

**Verdict:** Completely feasible and straightforward to implement ✅

**Total Effort:** 2-6 hours depending on approach

**Priority:** Medium (nice-to-have for observability, not blocking core functionality)

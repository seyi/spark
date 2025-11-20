# Code Execution Sandbox Tool

## Overview

The Code Execution Sandbox tool provides a secure, isolated environment where LLM agents can **write and execute code** as part of their reasoning and problem-solving process. This feature is inspired by ADK's `geminitool.CodeExecution` but designed for distributed systems with enhanced security controls.

## Features

- ✅ **Multi-language support**: Python, JavaScript, Go
- ✅ **Docker-based isolation**: Secure containerized execution
- ✅ **Resource limits**: CPU, memory, and execution time constraints
- ✅ **Network isolation**: Disabled by default for security
- ✅ **Read-only filesystem**: Prevents unauthorized file modifications
- ✅ **Output size limits**: Prevents memory exhaustion
- ✅ **Timeout enforcement**: Automatic termination of long-running code
- ✅ **Security hardening**: Dropped capabilities, no new privileges
- ✅ **Distributed-aware**: Works across partitioned agent systems

## Quick Start

### Basic Usage

```go
import (
    "github.com/apache/spark/spark-ai-agents/pkg/tools"
    "github.com/apache/spark/spark-ai-agents/pkg/agents"
)

// Create tool registry
registry := tools.NewToolRegistry()

// Register code execution tool with defaults
tools.RegisterCodeExecutionTools(registry, tools.DefaultCodeExecutionConfig())

// Create LLM agent with code execution capability
llmAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:        "coding_agent",
    ModelName:   "gpt-4",
    Instruction: "You can write and execute code to solve problems",
    Tools:       []string{"execute_code"},
}, modelProvider, registry)

// The agent can now execute code!
output, _ := llmAgent.Execute(ctx, &agent.AgentInput{
    Instruction: "Calculate the first 10 Fibonacci numbers",
})
```

### Python-Only Agent

```go
// Create a Python-only code execution tool (more restrictive)
registry := tools.NewToolRegistry()
registry.Register(tools.PythonOnlyCodeExecutionTool())

llmAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:  "math_agent",
    Tools: []string{"execute_code"},
}, modelProvider, registry)
```

### Custom Configuration

```go
config := tools.CodeExecutionConfig{
    Timeout:          10 * time.Second,           // Max execution time
    MaxMemoryMB:      512,                        // Memory limit
    AllowedLanguages: []string{"python"},         // Restrict to Python
    AllowNetwork:     false,                      // Disable network (secure)
    AllowFileWrites:  false,                      // Read-only filesystem
    MaxOutputSize:    5 * 1024 * 1024,           // 5MB output limit
    DockerImages: map[string]string{
        "python": "python:3.11-alpine",
    },
}

tool := tools.CodeExecutionTool(config)
registry.Register(tool)
```

## Use Cases

### 1. Mathematical Computations

```python
# Agent can generate and execute code for complex calculations
import math

def calculate_compound_interest(principal, rate, time, n):
    return principal * (1 + rate/n)**(n*time)

result = calculate_compound_interest(1000, 0.05, 10, 12)
print(f"Final amount: ${result:.2f}")
```

### 2. Data Analysis

```python
import statistics

data = [15, 20, 35, 40, 50, 60, 75, 80, 95, 100]

print(f"Mean: {statistics.mean(data)}")
print(f"Median: {statistics.median(data)}")
print(f"Std Dev: {statistics.stdev(data):.2f}")
```

### 3. Algorithm Implementation

```python
def quicksort(arr):
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quicksort(left) + middle + quicksort(right)

result = quicksort([3, 6, 8, 10, 1, 2, 1])
print(result)
```

### 4. ReAct Pattern with Code Execution

```
User: "What is 123^456 mod 789?"

Agent Thought: I need to calculate this using modular exponentiation
Agent Action: execute_code
Agent Action Input: {"code": "print(pow(123, 456, 789))", "language": "python"}
Agent Observation: 699
Agent Thought: I have the answer
Agent: The result of 123^456 mod 789 is 699
```

## Security Model

### Docker Isolation

Each code execution runs in an isolated Docker container with:
- **No network access** (unless explicitly enabled)
- **Read-only filesystem** (except /tmp with noexec)
- **Memory limits** (default: 256MB)
- **CPU limits** (default: 0.5 CPU)
- **Dropped capabilities** (no privileged operations)
- **Timeout enforcement** (default: 30 seconds)

### Security Best Practices

1. **Never enable network access** unless absolutely necessary
2. **Keep timeouts short** to prevent resource exhaustion
3. **Limit memory** to prevent OOM attacks
4. **Validate output size** to prevent memory exhaustion
5. **Use minimal Docker images** (alpine variants)
6. **Monitor execution metrics** via callbacks

### Example: Secure Configuration

```go
config := tools.CodeExecutionConfig{
    Timeout:         5 * time.Second,      // Short timeout
    MaxMemoryMB:     128,                   // Limited memory
    AllowNetwork:    false,                 // No network
    AllowFileWrites: false,                 // Read-only
    AllowedLanguages: []string{"python"},   // Single language
}
```

## Tool Schema

The code execution tool accepts the following parameters:

```json
{
  "code": "print('Hello World')",        // Required: Code to execute
  "language": "python",                   // Required: python|javascript|go
  "timeout": 10                           // Optional: Max seconds (capped at 60)
}
```

Returns:

```json
{
  "success": true,
  "stdout": "Hello World\n",
  "stderr": "",
  "exit_code": 0,
  "execution_time": 0.234,
  "language": "python",
  "truncated": false,
  "error": null
}
```

## LLM Integration

When integrated with an LLM agent, the agent will automatically call the code execution tool when it needs to:

1. Perform complex calculations
2. Process data
3. Implement algorithms
4. Verify mathematical claims
5. Generate visualizations (if libraries available)

### Example Agent Behavior

```
User: "Sort these numbers: 64, 34, 25, 12, 22, 11, 90"

Agent thinks: I'll write a sorting algorithm

Agent generates tool call:
{
  "tool": "execute_code",
  "params": {
    "code": "numbers = [64, 34, 25, 12, 22, 11, 90]\nprint(sorted(numbers))",
    "language": "python"
  }
}

Agent receives: [11, 12, 22, 25, 34, 64, 90]

Agent responds: "The sorted numbers are: [11, 12, 22, 25, 34, 64, 90]"
```

## Supported Languages

### Python (Recommended)
- **Image**: `python:3.11-alpine`
- **Best for**: Math, data analysis, scientific computing
- **Standard library**: Full Python 3.11 stdlib

### JavaScript
- **Image**: `node:20-alpine`
- **Best for**: JSON processing, algorithms
- **Standard library**: Node.js 20 runtime

### Go
- **Image**: `golang:1.21-alpine`
- **Best for**: Systems programming, performance-critical code
- **Note**: Requires compilation step (slower)

## Monitoring and Callbacks

### Track Code Executions

```go
beforeToolCallback := func(ctx context.Context, toolName string, params map[string]interface{}) error {
    if toolName == "execute_code" {
        fmt.Printf("Executing code: %v\n", params["code"])
    }
    return nil
}

afterToolCallback := func(ctx context.Context, toolName string, params map[string]interface{}, result interface{}) error {
    if toolName == "execute_code" {
        resultMap := result.(map[string]interface{})
        fmt.Printf("Execution completed in %v seconds\n", resultMap["execution_time"])
    }
    return nil
}

llmAgent := agents.NewLlmAgent(agents.LlmAgentConfig{
    Name:       "monitored_agent",
    Tools:      []string{"execute_code"},
    BeforeTool: []agents.ToolCallback{beforeToolCallback},
    AfterTool:  []agents.ToolResultCallback{afterToolCallback},
}, modelProvider, registry)
```

## Requirements

- **Docker**: Must be installed and accessible
- **Docker images**: Will be pulled automatically on first use
- **Permissions**: User must have Docker permissions

### Verify Docker Setup

```bash
docker version
docker run --rm python:3.11-alpine python -c "print('Docker works')"
```

## Testing

The implementation includes comprehensive tests that verify:

- ✅ Basic code execution (Python, JavaScript, Go)
- ✅ Mathematical calculations
- ✅ Timeout enforcement
- ✅ Error handling
- ✅ Language validation
- ✅ Parameter validation
- ✅ Data analysis capabilities
- ✅ Custom timeout override
- ✅ Execution time tracking
- ✅ Tool registry integration

Run tests:

```bash
# Requires Docker
go test -v ./pkg/tools -run TestCodeExecution
```

## Distributed Systems Support

The code execution tool is designed to work in distributed agent systems:

```go
// Each partition can execute code independently
agent1 := createAgent("partition-1", registry)  // Can execute code
agent2 := createAgent("partition-2", registry)  // Can execute code

// Code execution is local to each partition
// No cross-partition dependencies
```

## Comparison with ADK

| Feature | Our Implementation | ADK |
|---------|-------------------|-----|
| Code Execution | ✅ Docker-based | ✅ gVisor-based |
| Python Support | ✅ Python 3.11 | ✅ Python |
| JavaScript Support | ✅ Node 20 | ❌ |
| Go Support | ✅ Go 1.21 | ❌ |
| Resource Limits | ✅ CPU + Memory | ✅ Memory |
| Timeout | ✅ Configurable | ✅ Configurable |
| Network Isolation | ✅ Disabled by default | ✅ Disabled |
| Distributed Support | ✅ Partition-aware | ❌ Single machine |
| Cloud Integration | ✅ Any Docker host | ✅ Google Cloud |

## Limitations

1. **Requires Docker**: Not available in all environments
2. **Container overhead**: ~1-2 second startup time per execution
3. **No persistent state**: Each execution is stateless
4. **Limited libraries**: Only packages in base Docker images (no pip install)
5. **No interactive input**: Code must be fully autonomous

## Future Enhancements

Potential improvements:

- [ ] Support for additional languages (Rust, Ruby, etc.)
- [ ] Custom package installation (e.g., `pip install pandas`)
- [ ] Persistent workspace for multi-step workflows
- [ ] GPU support for ML workloads
- [ ] Integration with Jupyter notebooks
- [ ] Code caching for repeated executions
- [ ] Visualization output (matplotlib, etc.)

## Examples

### Fibonacci Calculator

```go
params := map[string]interface{}{
    "code": `
def fibonacci(n):
    if n <= 1: return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b

print([fibonacci(i) for i in range(10)])
`,
    "language": "python",
}

result, _ := tool.Handler(ctx, params)
// Output: [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
```

### Data Statistics

```go
params := map[string]interface{}{
    "code": `
import statistics
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(f"Mean: {statistics.mean(data)}")
print(f"Median: {statistics.median(data)}")
print(f"Std Dev: {statistics.stdev(data):.2f}")
`,
    "language": "python",
}
```

### JavaScript Algorithm

```go
params := map[string]interface{}{
    "code": `
const isPrime = (n) => {
    if (n <= 1) return false;
    for (let i = 2; i * i <= n; i++) {
        if (n % i === 0) return false;
    }
    return true;
};

const primes = Array.from({length: 100}, (_, i) => i).filter(isPrime);
console.log(JSON.stringify(primes));
`,
    "language": "javascript",
}
```

## License

Apache License 2.0 - See LICENSE file for details.

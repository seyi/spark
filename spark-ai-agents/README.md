# Dagens AI Agents

A distributed AI agent execution framework inspired by Apache Spark's distributed computing architecture. This framework brings Spark's powerful concepts—DAG scheduling, fault tolerance, locality-aware scheduling, and resilient execution—to AI agent orchestration.

## Overview

Dagens AI Agents enables you to build and execute distributed AI agent workflows that:

- **Scale horizontally** across multiple executor workers
- **Handle failures gracefully** through lineage tracking and checkpointing
- **Optimize resource usage** with locality-aware task scheduling
- **Compose hierarchically** with agent dependencies forming DAGs
- **Execute efficiently** with parallel stage execution

## Key Features

### 🌟 Distributed Architecture (Spark-Inspired)

- **DAG Scheduler**: High-level workflow coordination with stage-based execution
- **Task Scheduler**: Low-level task assignment with locality awareness
- **Executor Pool**: Distributed agent workers with resource management
- **Fault Tolerance**: Lineage tracking and automatic retry on failure
- **State Management**: Checkpointing for long-running workflows

### 🔧 AI Agent Capabilities

- **Hierarchical Composition**: Define agent dependencies for complex workflows
- **Tool-Based Architecture**: Composable capabilities (search, consensus, planning)
- **Multi-Model Support**: Flexible AI model selection per agent
- **Context Management**: Shared state and knowledge across agents
- **Metrics & Observability**: Built-in tracking of execution metrics

### 🐍 Python Interface

- **Simple API**: Easy-to-use Python bindings over high-performance Go core
- **Type-Safe**: Full type hints and dataclass models
- **Context Manager**: Resource management with `with` statements
- **Pythonic**: Follows Python conventions and best practices

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Agent Orchestrator                        │
│  (Like DAGScheduler - coordinates agent workflows)          │
│  - Builds DAG of agent tasks                                │
│  - Handles dependencies between agents                      │
│  - Tracks agent execution state                             │
└──────────────────┬──────────────────────────────────────────┘
                   │
       ┌───────────┴───────────┐
       │   Agent Scheduler      │
       │  (Like TaskScheduler)  │
       │  - Task assignment     │
       │  - Resource allocation │
       │  - Locality scheduling │
       └───────────┬───────────┘
                   │
      ┌────────────┴────────────┐
      │      RPC Layer          │
      │   (gRPC + Protocol      │
      │    Buffers)             │
      └────────────┬────────────┘
                   │
    ┌──────────────┼──────────────┐
    │              │              │
┌───▼────┐    ┌───▼────┐    ┌───▼────┐
│ Agent  │    │ Agent  │    │ Agent  │
│ Worker │    │ Worker │    │ Worker │
│   1    │    │   2    │    │   N    │
└────────┘    └────────┘    └────────┘
```

## Quick Start

### Installation

#### Python (Recommended for most users)

```bash
pip install spark-agents
```

#### From Source

```bash
# Clone repository
git clone https://github.com/seyi/dagens
cd dagens

# Build Go core
go build -buildmode=c-shared -o python/bindings/libsparkagents.so python/bindings/spark_agents.go

# Install Python package
cd python
pip install -e .
```

### Simple Example

```python
from spark_agents import SparkAgentCoordinator, Agent

# Initialize coordinator with 4 executor workers
coordinator = SparkAgentCoordinator(num_executors=4)

# Create an agent
researcher = Agent(
    name="Researcher",
    description="AI agent for information gathering",
    capabilities=["search", "analyze", "summarize"]
)

# Register agent
agent_id = coordinator.register_agent(researcher)

# Submit job
job_id = coordinator.submit_job(
    agent_id=agent_id,
    instruction="Research distributed AI systems",
    model="gpt-4"
)

# Check status
status = coordinator.get_job_status(job_id)
print(f"Job {status.job_id}: {status.state}")

# Cleanup
coordinator.shutdown()
```

### Hierarchical Agents Example

```python
from spark_agents import SparkAgentCoordinator, Agent

coordinator = SparkAgentCoordinator(num_executors=8)

# Create agent pipeline with dependencies
data_collector = Agent(
    name="DataCollector",
    description="Collects data from various sources",
    capabilities=["search", "fetch", "extract"]
)

analyzer = Agent(
    name="Analyzer",
    description="Analyzes collected data",
    capabilities=["analyze", "classify"],
    dependencies=[data_collector]  # Depends on data_collector
)

report_generator = Agent(
    name="ReportGenerator",
    description="Generates reports from analysis",
    capabilities=["format", "write"],
    dependencies=[analyzer]  # Depends on analyzer
)

# Register all agents
for agent in [data_collector, analyzer, report_generator]:
    coordinator.register_agent(agent)

# Submit job - automatically schedules all dependencies in DAG
job_id = coordinator.submit_job(
    agent_id=report_generator.id,
    instruction="Generate comprehensive report on AI trends"
)

# Execution order (automatic DAG scheduling):
# Stage 2: DataCollector (deepest dependency)
# Stage 1: Analyzer (depends on DataCollector)
# Stage 0: ReportGenerator (depends on Analyzer)
```

## Core Concepts

### 1. Agents

Agents are the basic unit of computation. Each agent has:
- **Capabilities**: Tools and skills the agent can use
- **Dependencies**: Other agents it depends on
- **Partition**: Locality hint for scheduling

```python
agent = Agent(
    name="MyAgent",
    description="What this agent does",
    capabilities=["tool1", "tool2"],
    dependencies=[other_agent],
    partition="partition-key"
)
```

### 2. DAG Scheduling

Like Spark, agent workflows form Directed Acyclic Graphs (DAGs):
- Agents with dependencies create stages
- Stages execute in topological order
- Tasks within a stage execute in parallel

### 3. Fault Tolerance

The framework provides multiple levels of fault tolerance:
- **Lineage Tracking**: Records execution history for recomputation
- **Checkpointing**: Saves agent state at stage boundaries
- **Automatic Retry**: Configurable retry on task failure
- **Executor Health**: Monitors and excludes failed executors

### 4. Locality-Aware Scheduling

Tasks are scheduled with locality preferences (like Spark):
- **PROCESS_LOCAL**: Same partition/executor
- **NODE_LOCAL**: Same node
- **RACK_LOCAL**: Same rack/region
- **ANY**: Any available executor

Scheduler uses delay scheduling to wait for better locality.

## Spark Concepts Mapping

| Spark Concept | Dagens AI Agents | Purpose |
|---------------|-----------------|---------|
| RDD | Agent | Basic unit of distributed computation |
| DAGScheduler | Agent Orchestrator | High-level workflow coordination |
| TaskScheduler | Task Scheduler | Low-level task assignment |
| Executor | Agent Worker | Executes tasks on worker nodes |
| Stage | Agent Stage | Group of tasks that run in parallel |
| Shuffle | Agent Communication | Data exchange between stages |
| Lineage | Execution Lineage | Track dependencies for fault recovery |
| Checkpoint | State Checkpoint | Save state to truncate lineage |

## Advanced Features

### Built-in Tools

The framework includes composable tools inspired by Zen MCP:

- **search**: Distributed search across agent knowledge
- **consensus**: Multi-agent consensus reaching
- **planner**: Task decomposition and planning

### Custom Tools

Register custom tools for your agents:

```python
from spark_agents.tools import ToolRegistry, ToolDefinition

registry = coordinator.get_tool_registry()

registry.register(ToolDefinition(
    name="custom_tool",
    description="My custom tool",
    schema={"input": "string"},
    handler=my_handler_function,
    enabled=True
))
```

### State Management

Access state management for checkpointing:

```python
state_mgr = coordinator.get_state_manager()

# Checkpoint agent state
state_mgr.checkpoint_manager().checkpoint(checkpoint_data)

# Track lineage
lineage_tracker = state_mgr.lineage_tracker()
```

### Metrics & Monitoring

Get real-time metrics:

```python
metrics = coordinator.get_metrics()
print(f"Active tasks: {metrics.active_tasks}")
print(f"Executors: {metrics.total_executors}")
print(f"Agents: {metrics.registered_agents}")
```

## Building from Source

### Requirements

- Go 1.21+
- Python 3.8+
- Protocol Buffers compiler (for gRPC)

### Build Steps

```bash
# Install Go dependencies
cd spark-ai-agents
go mod download

# Build shared library for Python
go build -buildmode=c-shared -o python/bindings/libsparkagents.so python/bindings/spark_agents.go

# Generate protobuf code (optional, for RPC)
protoc --go_out=. --go-grpc_out=. pkg/rpc/protocol.proto

# Install Python package
cd python
pip install -e .

# Run examples
python examples/simple_agent.py
python examples/hierarchical_agents.py
```

### Running Tests

```bash
# Go tests
go test ./...

# Python tests
pytest python/tests/
```

## Performance Considerations

### Scaling

- **Horizontal Scaling**: Add more executors for parallelism
- **Vertical Scaling**: Increase tasks per executor for concurrency
- **Partition Tuning**: Align partitions with data locality

### Resource Management

- Configure executor pool size based on workload
- Set appropriate timeouts for long-running agents
- Use checkpointing for workflows with long lineages

### Optimization Tips

1. **Minimize Shuffles**: Reduce agent dependencies to minimize data exchange
2. **Locality**: Set partition keys to improve locality scheduling
3. **Parallelism**: Break work into independent agents when possible
4. **Checkpointing**: Checkpoint at stage boundaries for fault recovery

## Comparison with Other Frameworks

| Feature | Dagens AI Agents | ADK Python | Zen MCP |
|---------|----------------|------------|---------|
| Distributed Execution | ✅ Yes (Spark-inspired) | ❌ No | ❌ No |
| Fault Tolerance | ✅ Lineage + Checkpointing | ❌ Limited | ❌ Limited |
| Locality Scheduling | ✅ Yes (multi-level) | ❌ No | ❌ No |
| DAG Workflows | ✅ Yes (automatic) | ⚠️ Manual | ⚠️ Manual |
| Horizontal Scaling | ✅ Yes | ❌ No | ❌ No |
| Multi-Language | ✅ Go + Python | ❌ Python only | ⚠️ CLI-based |
| Tool Composition | ✅ Yes | ✅ Yes | ✅ Yes |
| Agent Hierarchy | ✅ Yes (DAG) | ✅ Yes | ⚠️ Limited |

## Use Cases

- **Multi-Agent Research**: Coordinate multiple specialized agents for research tasks
- **Data Processing Pipelines**: Build ETL workflows with AI agents
- **Distributed Analysis**: Analyze large datasets with parallel agents
- **Complex Decision Making**: Hierarchical agents for multi-step decisions
- **Scalable AI Services**: Production-grade AI agent systems

## Roadmap

- [ ] gRPC-based distributed RPC (currently in-memory)
- [ ] Kubernetes integration for executor management
- [ ] Advanced checkpoint storage (S3, HDFS)
- [ ] Web UI for monitoring (like Spark UI)
- [ ] More built-in tools and integrations
- [ ] Performance benchmarks and optimizations
- [ ] Multi-language support (Rust, Java)

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - See [LICENSE](../LICENSE) for details.

## Acknowledgments

This framework is inspired by:
- **Apache Spark**: Distributed computing architecture and fault tolerance
- **Zen MCP**: Tool-based agent composition patterns
- **ADK Python**: Agent framework design and hierarchical agents

## Citation

If you use Dagens AI Agents in your research, please cite:

```bibtex
@software{spark_ai_agents,
  title = {Dagens AI Agents: Distributed AI Agent Framework},
  author = {Apache Spark Community},
  year = {2024},
  url = {https://github.com/seyi/dagens}
}
```

## Support

- **Documentation**: [docs/](docs/)
- **Examples**: [examples/](examples/)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions

# Phase 4: Spark Integration Deep Dive - Design

**Goal**: Make AI agents a first-class citizen in the Apache Spark ecosystem with native DataFrame integration and PySpark bindings.

## Overview

Phase 4 bridges the gap between our Go-based agent framework and Spark's Python/Scala APIs, enabling seamless integration with DataFrames, Structured Streaming, and the Catalyst optimizer.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    PySpark Layer                         │
│  DataFrame API │ Streaming API │ RDD API                │
└─────────────────┬───────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│              Python Agent Bindings                       │
│  spark_ai_agents Python package                         │
│  - AgentUDF                                             │
│  - DataFrame extensions                                 │
│  - Streaming processors                                 │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────▼─────────┐
        │   gRPC/REST API   │ (Communication Layer)
        └─────────┬─────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│              Go Agent Runtime                            │
│  - AgentRuntime                                         │
│  - Distributed Services                                 │
│  - Backend Storage (Redis/PostgreSQL)                   │
└─────────────────────────────────────────────────────────┘
```

## Components

### 1. gRPC Service Layer

**Purpose**: Enable Python ↔ Go communication

**Proto Definition**:
```protobuf
syntax = "proto3";

package spark_ai_agents;

service AgentService {
    // Execute single agent request
    rpc Execute(AgentRequest) returns (AgentResponse);

    // Execute batch of requests (streaming)
    rpc BatchExecute(stream AgentRequest) returns (stream AgentResponse);

    // Health check
    rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}

message AgentRequest {
    string agent_id = 1;
    string input = 2;
    map<string, string> context = 3;
    string session_id = 4;
}

message AgentResponse {
    string output = 1;
    map<string, string> metadata = 2;
    repeated Event events = 3;
    bool success = 4;
    string error = 5;
}

message Event {
    string type = 1;
    string content = 2;
    int64 timestamp = 3;
}
```

**Go Server Implementation**:
```go
type AgentGRPCServer struct {
    runtime *runtime.AgentRuntime
    agents  map[string]agent.Agent
}

func (s *AgentGRPCServer) Execute(ctx context.Context, req *pb.AgentRequest) (*pb.AgentResponse, error) {
    // Get agent
    ag, ok := s.agents[req.AgentId]
    if !ok {
        return nil, fmt.Errorf("agent not found: %s", req.AgentId)
    }

    // Create input
    input := &agent.AgentInput{
        Message: req.Input,
        Context: req.Context,
    }

    // Execute
    output, err := s.runtime.Run(ctx, input)
    if err != nil {
        return &pb.AgentResponse{Success: false, Error: err.Error()}, nil
    }

    return &pb.AgentResponse{
        Output:  output.Response,
        Metadata: output.Metadata,
        Success: true,
    }, nil
}
```

### 2. Python Package Structure

```
spark_ai_agents/
├── __init__.py
├── client.py          # gRPC client
├── agents.py          # Agent wrappers
├── udf.py             # Spark UDF integration
├── dataframe.py       # DataFrame extensions
├── streaming.py       # Streaming integration
├── utils.py           # Utilities
└── proto/
    └── agent_pb2.py   # Generated protobuf
```

### 3. Agent UDF Framework

**Python Implementation**:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from spark_ai_agents import AgentClient

class AgentUDF:
    def __init__(self, agent_id: str, server_url: str = "localhost:50051"):
        self.agent_id = agent_id
        self.client = AgentClient(server_url)

    def __call__(self, return_type=StringType()):
        """Create Spark UDF from agent"""
        def agent_func(text: str) -> str:
            response = self.client.execute(self.agent_id, text)
            return response.output

        return udf(agent_func, return_type)

# Usage
classify_agent = AgentUDF("text-classifier")
classify_udf = classify_agent()

df.withColumn("category", classify_udf(df.text))
```

### 4. DataFrame Extensions

**Monkey-patching DataFrame**:
```python
from pyspark.sql import DataFrame
from typing import Callable

def with_agent(self, agent_id: str, input_col: str, output_col: str = None):
    """Apply agent to DataFrame column"""
    if output_col is None:
        output_col = f"{input_col}_agent_output"

    agent_udf = AgentUDF(agent_id)()
    return self.withColumn(output_col, agent_udf(self[input_col]))

# Extend DataFrame
DataFrame.with_agent = with_agent

# Usage
df.with_agent("summarizer", "text", "summary")
```

### 5. Batch Processing

**Optimized Batch Execution**:
```python
class AgentBatchProcessor:
    def __init__(self, agent_id: str, batch_size: int = 32):
        self.agent_id = agent_id
        self.batch_size = batch_size
        self.client = AgentClient()

    def process_partition(self, partition):
        """Process entire partition with batching"""
        batch = []

        for row in partition:
            batch.append(row)

            if len(batch) >= self.batch_size:
                # Send batch to server
                responses = self.client.batch_execute(
                    self.agent_id,
                    [r.text for r in batch]
                )

                for r, resp in zip(batch, responses):
                    yield (r.id, resp.output)

                batch = []

        # Process remaining
        if batch:
            responses = self.client.batch_execute(
                self.agent_id,
                [r.text for r in batch]
            )
            for r, resp in zip(batch, responses):
                yield (r.id, resp.output)

# Usage
processor = AgentBatchProcessor("summarizer", batch_size=32)
results_rdd = df.rdd.mapPartitions(processor.process_partition)
```

### 6. Structured Streaming Integration

**Streaming Processor**:
```python
class AgentStreamProcessor:
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.client = AgentClient()

    def process_batch(self, batch_df, batch_id):
        """Process streaming micro-batch"""
        # Convert to RDD and process
        results = batch_df.rdd.map(
            lambda row: self.client.execute(self.agent_id, row.text)
        )

        # Convert back to DataFrame
        return results.toDF()

# Usage
stream = spark.readStream.format("kafka").load()

processor = AgentStreamProcessor("sentiment-analyzer")
processed_stream = stream.writeStream \
    .foreachBatch(processor.process_batch) \
    .start()
```

### 7. Stateful Streaming

**Stateful Agent Processing**:
```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout

def stateful_agent_process(key, values, state: GroupState):
    """Maintain conversation state across batches"""
    # Get existing state
    if state.exists:
        conversation = state.get()
    else:
        conversation = {"history": []}

    # Process new messages
    for value in values:
        response = client.execute(
            "conversational-agent",
            value.text,
            context={"history": conversation["history"]}
        )

        conversation["history"].append({
            "user": value.text,
            "agent": response.output
        })

    # Update state
    state.update(conversation)

    return conversation

# Usage
stream.groupByKey(lambda row: row.user_id) \
      .mapGroupsWithState(
          stateful_agent_process,
          StateType=ConversationState,
          timeout=GroupStateTimeout.ProcessingTimeTimeout
      )
```

## Implementation Plan

### Phase 4.1: gRPC Foundation (Week 1)

**Tasks**:
1. Define protobuf schema
2. Generate Go and Python code
3. Implement Go gRPC server
4. Implement Python gRPC client
5. Add connection pooling and retry logic

**Deliverables**:
- `pkg/grpc/agent_service.proto`
- `pkg/grpc/server.go`
- `python/spark_ai_agents/client.py`

### Phase 4.2: Python Package (Week 2)

**Tasks**:
1. Create Python package structure
2. Implement Agent wrappers
3. Implement AgentUDF
4. Add DataFrame extensions
5. Create setup.py for distribution

**Deliverables**:
- `python/spark_ai_agents/` package
- `setup.py` for pip install
- Unit tests

### Phase 4.3: Batch Processing (Week 3)

**Tasks**:
1. Implement batch execution in gRPC
2. Create AgentBatchProcessor
3. Add partition-aware processing
4. Optimize for large datasets

**Deliverables**:
- Batch execution support
- Performance benchmarks
- Optimization guide

### Phase 4.4: Streaming Integration (Week 4)

**Tasks**:
1. Implement foreachBatch integration
2. Add stateful processing support
3. Create streaming examples
4. Add checkpoint integration

**Deliverables**:
- Streaming processors
- Stateful agent support
- Streaming examples

### Phase 4.5: Testing & Documentation (Week 5)

**Tasks**:
1. Integration tests with real Spark cluster
2. Performance benchmarks
3. API documentation
4. Usage examples

**Deliverables**:
- Test suite
- Benchmarks report
- Documentation
- Example notebooks

## Example Use Cases

### Use Case 1: Large-Scale Document Summarization

```python
from pyspark.sql import SparkSession
from spark_ai_agents import AgentUDF

spark = SparkSession.builder.appName("DocumentSummarization").getOrCreate()

# Load 1 million documents
df = spark.read.json("s3://documents/*.json")

# Create summarization UDF
summarize = AgentUDF("gpt-4-summarizer")()

# Apply to all documents
summaries = df.withColumn("summary", summarize(df.content))

# Save results
summaries.write.parquet("s3://output/summaries")
```

**Performance**:
- 1M documents
- 1000 Spark executors
- ~30 minutes total (vs. 500 hours sequential)

### Use Case 2: Real-Time Sentiment Analysis

```python
from spark_ai_agents import AgentStreamProcessor

# Read from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "tweets") \
    .load()

# Process with agent
processor = AgentStreamProcessor("sentiment-analyzer")

processed = stream.writeStream \
    .foreachBatch(processor.process_batch) \
    .outputMode("append") \
    .format("delta") \
    .option("path", "s3://analytics/sentiments") \
    .option("checkpointLocation", "s3://checkpoints/sentiments") \
    .start()

processed.awaitTermination()
```

### Use Case 3: Conversational Agents at Scale

```python
# Maintain conversation state per user
stream = spark.readStream.format("websocket").load()

conversations = stream \
    .groupByKey(lambda msg: msg.user_id) \
    .mapGroupsWithState(
        func=conversational_agent_process,
        stateType=ConversationState,
        outputType=MessageOutput,
        timeout=GroupStateTimeout.EventTimeTimeout("10 minutes")
    )

conversations.writeStream \
    .format("websocket") \
    .start()
```

## Performance Optimizations

### 1. Connection Pooling

```python
class AgentClientPool:
    def __init__(self, server_url: str, pool_size: int = 10):
        self.pool = [
            AgentClient(server_url)
            for _ in range(pool_size)
        ]
        self.semaphore = threading.Semaphore(pool_size)

    def execute(self, agent_id: str, input: str):
        with self.semaphore:
            client = self.pool.pop()
            try:
                return client.execute(agent_id, input)
            finally:
                self.pool.append(client)
```

### 2. Adaptive Batching

```python
class AdaptiveBatchProcessor:
    def __init__(self, agent_id: str, min_batch: int = 8, max_batch: int = 128):
        self.batch_size = min_batch
        self.min_batch = min_batch
        self.max_batch = max_batch

    def adjust_batch_size(self, latency: float):
        """Adjust batch size based on latency"""
        if latency < 100:  # ms
            self.batch_size = min(self.batch_size * 2, self.max_batch)
        elif latency > 500:
            self.batch_size = max(self.batch_size // 2, self.min_batch)
```

### 3. Catalyst Optimizer Hints

```python
# Predicate pushdown
df.filter(col("language") == "en") \  # Pushed to data source
  .select(agent_classify(col("text")))

# Partition pruning
df.filter(col("date") > "2024-01-01") \  # Skip old partitions
  .transform(agent_process)

# Broadcast join for small agent configs
agent_configs = spark.read.json("configs.json")
broadcast(agent_configs).join(df, "agent_id")
```

## Deployment Architecture

### Development
```
Local Machine
├── Spark (local mode)
├── Agent gRPC Server (localhost:50051)
└── Redis (localhost:6379)
```

### Production
```
Kubernetes Cluster
├── Spark on K8s
│   ├── Driver Pod
│   └── Executor Pods (1000+)
├── Agent gRPC Service
│   ├── LoadBalancer
│   └── Agent Pods (autoscaled)
├── Redis Cluster
└── PostgreSQL (RDS)
```

## Monitoring & Observability

### Metrics to Track

**Agent Execution**:
- Requests per second
- Latency (p50, p95, p99)
- Error rate
- Throughput (tokens/sec)

**Spark Integration**:
- Task duration
- Data shuffle size
- Partition skew
- Executor utilization

**gRPC Service**:
- Connection pool usage
- Request queue depth
- Network bandwidth
- Serialization overhead

### Logging

```python
import logging

logger = logging.getLogger("spark_ai_agents")

# Log agent execution
logger.info(f"Executing agent {agent_id} on partition {partition_id}")
logger.debug(f"Input: {input[:100]}...")
logger.info(f"Completed in {duration}ms")
```

## Security Considerations

### Authentication

```python
# gRPC with TLS + token auth
channel_credentials = grpc.ssl_channel_credentials(
    root_certificates=open('ca.pem', 'rb').read()
)

token_credentials = grpc.access_token_call_credentials(
    os.getenv('AGENT_API_TOKEN')
)

composite_credentials = grpc.composite_channel_credentials(
    channel_credentials,
    token_credentials
)

channel = grpc.secure_channel(
    'agents.example.com:443',
    composite_credentials
)
```

### Data Privacy

```python
# Encrypt sensitive data before sending
from cryptography.fernet import Fernet

cipher = Fernet(os.getenv('ENCRYPTION_KEY'))

encrypted_input = cipher.encrypt(sensitive_data.encode())
response = client.execute(agent_id, encrypted_input)
decrypted_output = cipher.decrypt(response.output)
```

## Testing Strategy

### Unit Tests
```python
# Test Agent UDF
def test_agent_udf():
    mock_client = MockAgentClient()
    udf = AgentUDF("test-agent", client=mock_client)

    result = udf()("test input")
    assert result == "test output"
```

### Integration Tests
```python
# Test with real Spark
def test_spark_integration():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([("hello",)], ["text"])

    result = df.with_agent("echo", "text")
    assert result.count() == 1
```

### Performance Tests
```python
# Benchmark scaling
def benchmark_scaling():
    for num_executors in [1, 10, 100, 1000]:
        spark.conf.set("spark.executor.instances", num_executors)

        df = spark.range(1000000)
        start = time.time()

        df.rdd.mapPartitions(agent_process).count()

        duration = time.time() - start
        throughput = 1000000 / duration

        print(f"{num_executors} executors: {throughput} docs/sec")
```

## Success Metrics

- ✅ PySpark package installable via pip
- ✅ Agent UDF working with DataFrames
- ✅ Streaming integration functional
- ✅ Performance: > 10K requests/sec per executor
- ✅ Latency: < 100ms overhead (vs. direct agent call)
- ✅ Scaling: Linear speedup to 1000 executors
- ✅ Documentation complete with examples

## Next Steps After Phase 4

### Phase 5: Advanced Optimizations
- Catalyst optimizer custom rules
- Adaptive query execution integration
- Dynamic partition coalescing
- Speculative execution for agents

### Phase 6: MLlib Integration
- Agent-based feature engineering
- Integration with Spark ML Pipelines
- Model serving via agents
- AutoML with agents

## Conclusion

Phase 4 transforms the AI agents framework from "code that runs on Spark" to a **native Spark component**, enabling:
- Seamless DataFrame integration
- Native Python API (PySpark)
- Streaming support
- Production-scale deployment

This unlocks **massive scale** for AI agent workloads: processing millions of documents, real-time sentiment analysis, conversational agents at scale.

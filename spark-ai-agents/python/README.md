# Spark AI Agents - PySpark Integration

Native PySpark integration for distributed AI agent execution.

## Installation

```bash
pip install dagens-ai-agents
```

## Quick Start

```python
from pyspark.sql import SparkSession
from spark_ai_agents import LlmAgent, agent_udf

# Create Spark session
spark = SparkSession.builder.appName("AgentExample").getOrCreate()

# Load data
df = spark.read.json("documents.json")

# Create agent
summarizer = LlmAgent("gpt-4-summarizer")

# Create UDF
summarize = agent_udf(summarizer)

# Apply to DataFrame
result = df.withColumn("summary", summarize(df.content))

result.show()
```

## Features

- **DataFrame Integration**: Native DataFrame methods for agent execution
- **Batch Processing**: Automatic batching for improved performance
- **Streaming Support**: Structured Streaming integration
- **Multiple Agent Types**: LLM, Sequential, Parallel, Loop agents
- **Production Ready**: Connection pooling, error handling, retries

## Documentation

See the [main documentation](../docs/PHASE4_SPARK_INTEGRATION_DESIGN.md) for detailed usage.

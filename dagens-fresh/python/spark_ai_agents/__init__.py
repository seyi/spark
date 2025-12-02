"""
Spark AI Agents - PySpark integration for distributed AI agent execution.

This package provides native PySpark integration for the Spark AI Agents framework,
enabling seamless execution of AI agents on DataFrames and Structured Streaming.

Two execution modes are supported:
1. RPC-based: Agents communicate with external Go server via HTTP
2. Native: Agents run directly on Spark executors (10-100x faster)
"""

__version__ = "0.1.0"

# Core imports (always available)
from .client import AgentClient, AgentClientPool, AgentResponse
from .agents import Agent, LlmAgent, SequentialAgent, ParallelAgent, LoopAgent, create_agent

# Native agent imports (no PySpark required for core classes)
from .native_providers import (
    NativeLLMProvider, LLMConfig, LLMResponse,
    OpenAIProvider, AnthropicProvider, AzureOpenAIProvider, GoogleVertexProvider,
    MockProvider, create_provider, register_provider
)
from .native_agents import (
    NativeAgent, NativeAgentConfig, NativeAgentResponse,
    NativeSequentialAgent, NativeParallelAgent,
    create_native_agent, create_native_sequential_agent, create_native_parallel_agent
)

__all__ = [
    # Core (RPC-based)
    "AgentClient",
    "AgentClientPool",
    "AgentResponse",
    "Agent",
    "LlmAgent",
    "SequentialAgent",
    "ParallelAgent",
    "LoopAgent",
    "create_agent",

    # Native LLM Providers
    "NativeLLMProvider",
    "LLMConfig",
    "LLMResponse",
    "OpenAIProvider",
    "AnthropicProvider",
    "AzureOpenAIProvider",
    "GoogleVertexProvider",
    "MockProvider",
    "create_provider",
    "register_provider",

    # Native Agents (NO RPC!)
    "NativeAgent",
    "NativeAgentConfig",
    "NativeAgentResponse",
    "NativeSequentialAgent",
    "NativeParallelAgent",
    "create_native_agent",
    "create_native_sequential_agent",
    "create_native_parallel_agent",
]

# PySpark-dependent imports (optional)
try:
    from .udf import agent_udf, AgentUDF, batch_agent_udf, BatchAgentUDF
    from .native_udf import (
        NativeAgentUDF, NativeBatchAgentUDF, NativePartitionProcessor,
        native_agent_udf, native_batch_agent_udf, native_partition_executor
    )
    from .dataframe import register_dataframe_extensions

    # Register DataFrame extensions on import
    register_dataframe_extensions()

    __all__.extend([
        # RPC-based UDFs
        "agent_udf",
        "AgentUDF",
        "batch_agent_udf",
        "BatchAgentUDF",

        # Native UDFs (NO RPC!)
        "NativeAgentUDF",
        "NativeBatchAgentUDF",
        "NativePartitionProcessor",
        "native_agent_udf",
        "native_batch_agent_udf",
        "native_partition_executor",
    ])

    _HAS_PYSPARK = True
except ImportError:
    # PySpark not available - that's okay for core client usage
    _HAS_PYSPARK = False
    agent_udf = None
    AgentUDF = None
    batch_agent_udf = None
    BatchAgentUDF = None
    NativeAgentUDF = None
    NativeBatchAgentUDF = None
    NativePartitionProcessor = None
    native_agent_udf = None
    native_batch_agent_udf = None
    native_partition_executor = None

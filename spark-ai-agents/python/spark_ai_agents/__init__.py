"""
Spark AI Agents - PySpark integration for distributed AI agent execution.

This package provides native PySpark integration for the Spark AI Agents framework,
enabling seamless execution of AI agents on DataFrames and Structured Streaming.
"""

__version__ = "0.1.0"

# Core imports (always available)
from .client import AgentClient, AgentClientPool, AgentResponse
from .agents import Agent, LlmAgent, SequentialAgent, ParallelAgent, LoopAgent, create_agent

__all__ = [
    # Core
    "AgentClient",
    "AgentClientPool",
    "AgentResponse",
    "Agent",
    "LlmAgent",
    "SequentialAgent",
    "ParallelAgent",
    "LoopAgent",
    "create_agent",
]

# PySpark-dependent imports (optional)
try:
    from .udf import agent_udf, AgentUDF, batch_agent_udf, BatchAgentUDF
    from .dataframe import register_dataframe_extensions

    # Register DataFrame extensions on import
    register_dataframe_extensions()

    __all__.extend([
        "agent_udf",
        "AgentUDF",
        "batch_agent_udf",
        "BatchAgentUDF",
    ])

    _HAS_PYSPARK = True
except ImportError:
    # PySpark not available - that's okay for core client usage
    _HAS_PYSPARK = False
    agent_udf = None
    AgentUDF = None
    batch_agent_udf = None
    BatchAgentUDF = None

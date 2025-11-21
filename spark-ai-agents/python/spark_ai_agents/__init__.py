"""
Spark AI Agents - PySpark integration for distributed AI agent execution.

This package provides native PySpark integration for the Spark AI Agents framework,
enabling seamless execution of AI agents on DataFrames and Structured Streaming.
"""

__version__ = "0.1.0"

from .client import AgentClient
from .agents import Agent, LlmAgent, SequentialAgent, ParallelAgent
from .udf import agent_udf, AgentUDF
from .dataframe import register_dataframe_extensions

# Register DataFrame extensions on import
register_dataframe_extensions()

__all__ = [
    "AgentClient",
    "Agent",
    "LlmAgent",
    "SequentialAgent",
    "ParallelAgent",
    "agent_udf",
    "AgentUDF",
]

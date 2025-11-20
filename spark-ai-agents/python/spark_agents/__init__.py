"""
Spark AI Agents - Distributed AI agent framework inspired by Apache Spark

This library provides a distributed AI agent execution framework that leverages
Spark's distributed computing concepts: DAG scheduling, fault tolerance,
locality-aware task scheduling, and resilient execution.

Example:
    >>> from spark_agents import SparkAgentCoordinator, Agent
    >>>
    >>> # Initialize coordinator
    >>> coordinator = SparkAgentCoordinator(num_executors=4)
    >>>
    >>> # Register an agent
    >>> agent = Agent(
    ...     name="researcher",
    ...     description="Research agent for information gathering",
    ...     capabilities=["search", "analyze", "summarize"]
    ... )
    >>> coordinator.register_agent(agent)
    >>>
    >>> # Submit a job
    >>> job_id = coordinator.submit_job(
    ...     agent.id,
    ...     instruction="Research distributed AI systems",
    ...     model="gpt-4"
    ... )
    >>>
    >>> # Check status
    >>> status = coordinator.get_job_status(job_id)
    >>> print(f"Job {status.job_id}: {status.state}")
"""

from .coordinator import SparkAgentCoordinator
from .agent import Agent, AgentInput
from .models import JobStatus, JobState, Metrics

__version__ = "0.1.0"
__all__ = [
    "SparkAgentCoordinator",
    "Agent",
    "AgentInput",
    "JobStatus",
    "JobState",
    "Metrics",
]

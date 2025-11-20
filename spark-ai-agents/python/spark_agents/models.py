"""
Data models for job status and metrics
"""

from dataclasses import dataclass
from typing import Optional
from enum import Enum


class JobState(str, Enum):
    """Job execution state."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass
class JobStatus:
    """
    Status of a distributed agent job.

    Attributes:
        job_id: Unique job identifier
        state: Current execution state
        total_stages: Total number of stages in DAG
        completed_stages: Number of completed stages
        total_tasks: Total number of tasks
        completed_tasks: Number of completed tasks
        error: Error message if failed
    """

    job_id: str
    state: str
    total_stages: int
    completed_stages: int
    total_tasks: int
    completed_tasks: int
    error: Optional[str] = None

    @property
    def is_complete(self) -> bool:
        """Check if job is complete (success or failure)."""
        return self.state in [JobState.COMPLETED.value, JobState.FAILED.value]

    @property
    def is_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.state == JobState.COMPLETED.value

    @property
    def progress(self) -> float:
        """Get job progress as percentage (0.0 to 1.0)."""
        if self.total_stages == 0:
            return 0.0
        return self.completed_stages / self.total_stages

    def __repr__(self) -> str:
        progress_pct = self.progress * 100
        return (
            f"JobStatus(id={self.job_id}, state={self.state}, "
            f"progress={progress_pct:.1f}%, "
            f"stages={self.completed_stages}/{self.total_stages})"
        )


@dataclass
class Metrics:
    """
    Coordinator metrics.

    Attributes:
        registered_agents: Number of registered agents
        total_executors: Total executor workers
        active_tasks: Currently running tasks
        queued_tasks: Tasks waiting for execution
        registered_tools: Number of available tools
    """

    registered_agents: int
    total_executors: int
    active_tasks: int
    queued_tasks: int
    registered_tools: int

    def __repr__(self) -> str:
        return (
            f"Metrics(agents={self.registered_agents}, "
            f"executors={self.total_executors}, "
            f"active_tasks={self.active_tasks}, "
            f"queued_tasks={self.queued_tasks})"
        )

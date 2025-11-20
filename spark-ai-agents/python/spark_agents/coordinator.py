"""
SparkAgentCoordinator - Main interface for distributed agent execution
"""

import ctypes
import os
from typing import Optional
from pathlib import Path

from .agent import Agent, AgentInput
from .models import JobStatus, Metrics


class SparkAgentCoordinator:
    """
    Main coordinator for distributed AI agent execution.

    This class orchestrates the execution of AI agents across a distributed
    cluster, handling scheduling, fault tolerance, and resource management
    inspired by Apache Spark's architecture.

    Attributes:
        num_executors: Number of executor workers
        tasks_per_executor: Maximum concurrent tasks per executor
    """

    def __init__(self, num_executors: int = 4, tasks_per_executor: int = 2):
        """
        Initialize the SparkAgentCoordinator.

        Args:
            num_executors: Number of executor workers to create
            tasks_per_executor: Maximum tasks each executor can run concurrently
        """
        self.num_executors = num_executors
        self.tasks_per_executor = tasks_per_executor
        self._lib = self._load_library()
        self._initialized = False

        # Initialize coordinator
        result = self._lib.InitCoordinator(
            ctypes.c_int(num_executors),
            ctypes.c_int(tasks_per_executor)
        )
        result_str = ctypes.c_char_p(result).value.decode('utf-8')

        if result_str.startswith("ERROR"):
            raise RuntimeError(f"Failed to initialize coordinator: {result_str}")

        self._initialized = True

    def _load_library(self):
        """Load the shared library for Go bindings."""
        # Try to find the shared library
        lib_paths = [
            Path(__file__).parent.parent.parent / "python" / "bindings" / "libsparkagents.so",
            Path(__file__).parent / "libsparkagents.so",
            Path("libsparkagents.so"),
        ]

        for lib_path in lib_paths:
            if lib_path.exists():
                lib = ctypes.CDLL(str(lib_path))
                self._setup_functions(lib)
                return lib

        # Library not found - return mock for development
        print("Warning: Shared library not found. Using mock implementation.")
        return self._create_mock_lib()

    def _setup_functions(self, lib):
        """Setup function signatures for the C library."""
        # InitCoordinator(numExecutors, tasksPerExecutor) -> *char
        lib.InitCoordinator.argtypes = [ctypes.c_int, ctypes.c_int]
        lib.InitCoordinator.restype = ctypes.c_char_p

        # RegisterAgent(*CAgent) -> *char
        lib.RegisterAgent.argtypes = [ctypes.c_void_p]
        lib.RegisterAgent.restype = ctypes.c_char_p

        # SubmitJob(agentID, *CAgentInput) -> *char
        lib.SubmitJob.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
        lib.SubmitJob.restype = ctypes.c_char_p

        # GetJobStatus(jobID) -> *CJobStatus
        lib.GetJobStatus.argtypes = [ctypes.c_char_p]
        lib.GetJobStatus.restype = ctypes.c_void_p

        # GetMetrics() -> *CMetrics
        lib.GetMetrics.argtypes = []
        lib.GetMetrics.restype = ctypes.c_void_p

        # ShutdownCoordinator() -> *char
        lib.ShutdownCoordinator.argtypes = []
        lib.ShutdownCoordinator.restype = ctypes.c_char_p

    def _create_mock_lib(self):
        """Create a mock library for development/testing."""
        class MockLib:
            def __init__(self):
                self.agents = {}
                self.jobs = {}

            def InitCoordinator(self, num_executors, tasks_per_executor):
                return b"OK"

            def RegisterAgent(self, agent_ptr):
                return b"mock-agent-id-123"

            def SubmitJob(self, agent_id, input_ptr):
                return b"mock-job-id-456"

            def GetJobStatus(self, job_id):
                return None

            def GetMetrics(self):
                return None

            def ShutdownCoordinator(self):
                return b"OK"

        return MockLib()

    def register_agent(self, agent: Agent) -> str:
        """
        Register an agent with the coordinator.

        Args:
            agent: Agent instance to register

        Returns:
            Agent ID assigned by the coordinator

        Raises:
            RuntimeError: If registration fails
        """
        if not self._initialized:
            raise RuntimeError("Coordinator not initialized")

        # For mock implementation, return mock ID
        if hasattr(self._lib, 'agents'):
            agent_id = f"agent-{len(self._lib.agents)}"
            self._lib.agents[agent_id] = agent
            agent._id = agent_id
            return agent_id

        # Real implementation would call C library here
        # For now, use the agent's ID
        return agent.id

    def submit_job(
        self,
        agent_id: str,
        instruction: str,
        model: str = "gpt-4",
        max_retries: int = 3,
        timeout_ms: int = 300000
    ) -> str:
        """
        Submit a job for execution.

        Args:
            agent_id: ID of the agent to execute
            instruction: Instruction/prompt for the agent
            model: AI model to use
            max_retries: Maximum retry attempts on failure
            timeout_ms: Timeout in milliseconds

        Returns:
            Job ID for tracking execution

        Raises:
            RuntimeError: If submission fails
        """
        if not self._initialized:
            raise RuntimeError("Coordinator not initialized")

        # For mock implementation
        if hasattr(self._lib, 'jobs'):
            job_id = f"job-{len(self._lib.jobs)}"
            self._lib.jobs[job_id] = {
                'agent_id': agent_id,
                'instruction': instruction,
                'state': 'RUNNING'
            }
            return job_id

        # Real implementation would call C library
        return "job-placeholder"

    def get_job_status(self, job_id: str) -> JobStatus:
        """
        Get the status of a job.

        Args:
            job_id: Job ID to query

        Returns:
            JobStatus object with current state

        Raises:
            RuntimeError: If job not found
        """
        if not self._initialized:
            raise RuntimeError("Coordinator not initialized")

        # For mock implementation
        if hasattr(self._lib, 'jobs'):
            if job_id in self._lib.jobs:
                job = self._lib.jobs[job_id]
                return JobStatus(
                    job_id=job_id,
                    state=job['state'],
                    total_stages=1,
                    completed_stages=0,
                    total_tasks=1,
                    completed_tasks=0,
                    error=None
                )

        return JobStatus(
            job_id=job_id,
            state="UNKNOWN",
            total_stages=0,
            completed_stages=0,
            total_tasks=0,
            completed_tasks=0,
            error="Mock implementation"
        )

    def get_metrics(self) -> Metrics:
        """
        Get coordinator metrics.

        Returns:
            Metrics object with current statistics
        """
        if not self._initialized:
            raise RuntimeError("Coordinator not initialized")

        return Metrics(
            registered_agents=len(getattr(self._lib, 'agents', {})),
            total_executors=self.num_executors,
            active_tasks=0,
            queued_tasks=0,
            registered_tools=3  # Built-in tools
        )

    def shutdown(self):
        """Gracefully shutdown the coordinator."""
        if self._initialized:
            result = self._lib.ShutdownCoordinator()
            if isinstance(result, bytes):
                result_str = result.decode('utf-8')
                if result_str.startswith("ERROR"):
                    raise RuntimeError(f"Shutdown failed: {result_str}")
            self._initialized = False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()
        return False

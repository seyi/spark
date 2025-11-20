"""
Agent definitions and input models
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import uuid


@dataclass
class Agent:
    """
    Represents a distributed AI agent.

    An agent encapsulates specific capabilities and can be composed
    hierarchically with dependencies on other agents.

    Attributes:
        name: Human-readable name
        description: What the agent does
        capabilities: List of tools/capabilities
        dependencies: Other agents this depends on
        partition: Partition key for locality-aware scheduling
        id: Unique identifier (auto-generated)
    """

    name: str
    description: str
    capabilities: List[str] = field(default_factory=list)
    dependencies: List['Agent'] = field(default_factory=list)
    partition: str = "default"
    _id: Optional[str] = field(default=None, init=False)

    def __post_init__(self):
        """Initialize agent ID if not set."""
        if self._id is None:
            self._id = str(uuid.uuid4())

    @property
    def id(self) -> str:
        """Get agent ID."""
        return self._id

    def add_capability(self, capability: str):
        """Add a capability to the agent."""
        if capability not in self.capabilities:
            self.capabilities.append(capability)

    def add_dependency(self, agent: 'Agent'):
        """Add a dependency agent."""
        if agent not in self.dependencies:
            self.dependencies.append(agent)

    def __repr__(self) -> str:
        return (
            f"Agent(id={self.id}, name={self.name}, "
            f"capabilities={len(self.capabilities)}, "
            f"dependencies={len(self.dependencies)})"
        )


@dataclass
class AgentInput:
    """
    Input for agent execution.

    Attributes:
        instruction: The task/prompt for the agent
        context: Additional context data
        model: AI model to use
        max_retries: Maximum retry attempts
        timeout_ms: Timeout in milliseconds
        task_id: Optional task identifier
    """

    instruction: str
    context: Dict[str, Any] = field(default_factory=dict)
    model: str = "gpt-4"
    max_retries: int = 3
    timeout_ms: int = 300000  # 5 minutes
    task_id: Optional[str] = field(default=None, init=False)

    def __post_init__(self):
        """Initialize task ID if not set."""
        if self.task_id is None:
            self.task_id = str(uuid.uuid4())

    def add_context(self, key: str, value: Any):
        """Add context data."""
        self.context[key] = value

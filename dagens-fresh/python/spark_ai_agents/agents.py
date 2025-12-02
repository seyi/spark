"""
Agent wrappers for PySpark integration.
"""

from typing import Dict, List, Optional
from .client import AgentClient, AgentResponse


class Agent:
    """Base class for agent wrappers"""

    def __init__(self, agent_id: str, server_url: str = None):
        """
        Initialize agent wrapper.

        Args:
            agent_id: ID of the agent
            server_url: URL of the agent server
        """
        self.agent_id = agent_id
        self.server_url = server_url
        self._client = None

    @property
    def client(self) -> AgentClient:
        """Get or create client (lazy initialization)"""
        if self._client is None:
            self._client = AgentClient(self.server_url)
        return self._client

    def execute(self, input_text: str, context: Dict[str, str] = None,
                session_id: str = None, user_id: str = None) -> AgentResponse:
        """
        Execute agent with input.

        Args:
            input_text: Input text
            context: Additional context
            session_id: Session ID for stateful execution
            user_id: User ID

        Returns:
            AgentResponse
        """
        return self.client.execute(
            self.agent_id,
            input_text,
            context=context,
            session_id=session_id,
            user_id=user_id
        )

    def batch_execute(self, inputs: List[str],
                      context: Dict[str, str] = None) -> List[AgentResponse]:
        """
        Execute agent on batch of inputs.

        Args:
            inputs: List of input texts
            context: Shared context

        Returns:
            List of AgentResponse objects
        """
        return self.client.batch_execute(
            self.agent_id,
            inputs,
            context=context
        )

    def __call__(self, input_text: str) -> str:
        """
        Execute agent and return output text (simplified interface).

        Args:
            input_text: Input text

        Returns:
            Output text
        """
        response = self.execute(input_text)
        return response.output if response.success else ""

    def __repr__(self):
        return f"{self.__class__.__name__}(agent_id='{self.agent_id}')"


class LlmAgent(Agent):
    """
    LLM-based agent wrapper.

    Example:
        agent = LlmAgent("gpt-4-summarizer")
        summary = agent("Long document text...")
    """

    def __init__(self, agent_id: str, model: str = None,
                 system_prompt: str = None, server_url: str = None):
        """
        Initialize LLM agent.

        Args:
            agent_id: Agent ID
            model: LLM model name (optional, configured on server)
            system_prompt: System prompt (optional, configured on server)
            server_url: Server URL
        """
        super().__init__(agent_id, server_url)
        self.model = model
        self.system_prompt = system_prompt


class SequentialAgent(Agent):
    """
    Sequential agent wrapper - executes sub-agents in sequence.

    Example:
        agent = SequentialAgent("analysis-pipeline")
        result = agent("Input data")
    """

    def __init__(self, agent_id: str, sub_agents: List[str] = None,
                 server_url: str = None):
        """
        Initialize sequential agent.

        Args:
            agent_id: Agent ID
            sub_agents: List of sub-agent IDs (optional, configured on server)
            server_url: Server URL
        """
        super().__init__(agent_id, server_url)
        self.sub_agents = sub_agents or []


class ParallelAgent(Agent):
    """
    Parallel agent wrapper - executes sub-agents in parallel.

    Example:
        agent = ParallelAgent("multi-perspective-analysis")
        result = agent("Input data")
    """

    def __init__(self, agent_id: str, sub_agents: List[str] = None,
                 server_url: str = None):
        """
        Initialize parallel agent.

        Args:
            agent_id: Agent ID
            sub_agents: List of sub-agent IDs (optional, configured on server)
            server_url: Server URL
        """
        super().__init__(agent_id, server_url)
        self.sub_agents = sub_agents or []


class LoopAgent(Agent):
    """
    Loop agent wrapper - iterative execution until condition met.

    Example:
        agent = LoopAgent("iterative-refinement")
        result = agent("Initial input")
    """

    def __init__(self, agent_id: str, max_iterations: int = 10,
                 server_url: str = None):
        """
        Initialize loop agent.

        Args:
            agent_id: Agent ID
            max_iterations: Maximum iterations (optional, configured on server)
            server_url: Server URL
        """
        super().__init__(agent_id, server_url)
        self.max_iterations = max_iterations


# Convenience functions for creating agents

def create_agent(agent_type: str, agent_id: str, **kwargs) -> Agent:
    """
    Factory function to create agents.

    Args:
        agent_type: Type of agent ('llm', 'sequential', 'parallel', 'loop')
        agent_id: Agent ID
        **kwargs: Additional arguments for specific agent type

    Returns:
        Agent instance

    Example:
        agent = create_agent('llm', 'summarizer', model='gpt-4')
    """
    agent_classes = {
        'llm': LlmAgent,
        'sequential': SequentialAgent,
        'parallel': ParallelAgent,
        'loop': LoopAgent,
    }

    agent_class = agent_classes.get(agent_type.lower(), Agent)
    return agent_class(agent_id, **kwargs)

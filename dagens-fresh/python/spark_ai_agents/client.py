"""
AgentClient provides gRPC client for communicating with the Go agent runtime.
"""

import os
import time
from typing import Dict, List, Optional, Iterator
import json


class AgentResponse:
    """Response from agent execution"""

    def __init__(self, output: str, metadata: Dict[str, str] = None,
                 success: bool = True, error: str = None, duration_ms: int = 0):
        self.output = output
        self.metadata = metadata or {}
        self.success = success
        self.error = error
        self.duration_ms = duration_ms

    def __repr__(self):
        return f"AgentResponse(output={self.output[:50]}..., success={self.success})"


class AgentClient:
    """
    Client for communicating with the Go agent runtime via gRPC.

    For Phase 4 initial implementation, this uses a simple HTTP/JSON API.
    Future: Migrate to gRPC for better performance.
    """

    def __init__(self, server_url: str = None, timeout: int = 30):
        """
        Initialize agent client.

        Args:
            server_url: URL of the agent server (default: localhost:8080)
            timeout: Request timeout in seconds
        """
        self.server_url = server_url or os.getenv("AGENT_SERVER_URL", "http://localhost:8080")
        self.timeout = timeout
        self._session = None

    def _get_session(self):
        """Get or create HTTP session (lazy initialization)"""
        if self._session is None:
            try:
                import requests
                self._session = requests.Session()
            except ImportError:
                raise ImportError(
                    "requests library required for AgentClient. "
                    "Install with: pip install requests"
                )
        return self._session

    def execute(self, agent_id: str, input_text: str,
                context: Dict[str, str] = None,
                session_id: str = None,
                user_id: str = None) -> AgentResponse:
        """
        Execute agent with given input.

        Args:
            agent_id: ID of the agent to execute
            input_text: Input text for the agent
            context: Additional context (optional)
            session_id: Session ID for stateful agents (optional)
            user_id: User ID (optional)

        Returns:
            AgentResponse with agent output
        """
        session = self._get_session()

        payload = {
            "agent_id": agent_id,
            "input": input_text,
            "context": context or {},
            "session_id": session_id,
            "user_id": user_id
        }

        start_time = time.time()

        try:
            response = session.post(
                f"{self.server_url}/api/v1/agents/execute",
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()

            duration_ms = int((time.time() - start_time) * 1000)
            result = response.json()

            return AgentResponse(
                output=result.get("output", ""),
                metadata=result.get("metadata", {}),
                success=result.get("success", True),
                error=result.get("error"),
                duration_ms=duration_ms
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return AgentResponse(
                output="",
                success=False,
                error=str(e),
                duration_ms=duration_ms
            )

    def batch_execute(self, agent_id: str, inputs: List[str],
                      context: Dict[str, str] = None) -> List[AgentResponse]:
        """
        Execute agent on batch of inputs.

        Args:
            agent_id: ID of the agent to execute
            inputs: List of input texts
            context: Shared context for all inputs

        Returns:
            List of AgentResponse objects
        """
        session = self._get_session()

        payload = {
            "agent_id": agent_id,
            "inputs": inputs,
            "context": context or {}
        }

        try:
            response = session.post(
                f"{self.server_url}/api/v1/agents/batch_execute",
                json=payload,
                timeout=self.timeout * len(inputs)  # Scale timeout with batch size
            )
            response.raise_for_status()

            results = response.json().get("results", [])

            return [
                AgentResponse(
                    output=r.get("output", ""),
                    metadata=r.get("metadata", {}),
                    success=r.get("success", True),
                    error=r.get("error")
                )
                for r in results
            ]

        except Exception as e:
            # Return error response for all inputs
            return [
                AgentResponse(output="", success=False, error=str(e))
                for _ in inputs
            ]

    def health_check(self) -> bool:
        """
        Check if agent server is healthy.

        Returns:
            True if server is healthy, False otherwise
        """
        session = self._get_session()

        try:
            response = session.get(
                f"{self.server_url}/health",
                timeout=5
            )
            return response.status_code == 200
        except:
            return False

    def list_agents(self) -> List[Dict[str, str]]:
        """
        List available agents.

        Returns:
            List of agent info dicts
        """
        session = self._get_session()

        try:
            response = session.get(
                f"{self.server_url}/api/v1/agents",
                timeout=self.timeout
            )
            response.raise_for_status()

            return response.json().get("agents", [])
        except:
            return []

    def close(self):
        """Close HTTP session"""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class AgentClientPool:
    """
    Connection pool for agent clients to improve performance.
    """

    def __init__(self, server_url: str = None, pool_size: int = 10):
        """
        Initialize client pool.

        Args:
            server_url: URL of the agent server
            pool_size: Number of clients in the pool
        """
        self.server_url = server_url
        self.pool_size = pool_size
        self.clients = [AgentClient(server_url) for _ in range(pool_size)]
        self.current_index = 0

    def get_client(self) -> AgentClient:
        """
        Get next available client (round-robin).

        Returns:
            AgentClient instance
        """
        client = self.clients[self.current_index]
        self.current_index = (self.current_index + 1) % self.pool_size
        return client

    def execute(self, agent_id: str, input_text: str, **kwargs) -> AgentResponse:
        """Execute using pool"""
        return self.get_client().execute(agent_id, input_text, **kwargs)

    def batch_execute(self, agent_id: str, inputs: List[str], **kwargs) -> List[AgentResponse]:
        """Batch execute using pool"""
        return self.get_client().batch_execute(agent_id, inputs, **kwargs)

    def close_all(self):
        """Close all clients in pool"""
        for client in self.clients:
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

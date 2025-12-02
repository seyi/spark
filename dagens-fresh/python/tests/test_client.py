"""
Unit tests for spark_ai_agents client library.

These tests use mocks to test the client without requiring a running server.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add the package to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_ai_agents.client import AgentResponse, AgentClient, AgentClientPool
from spark_ai_agents.agents import (
    Agent, LlmAgent, SequentialAgent, ParallelAgent, LoopAgent, create_agent
)


class TestAgentResponse(unittest.TestCase):
    """Tests for AgentResponse class"""

    def test_init_success(self):
        """Test successful response initialization"""
        response = AgentResponse(
            output="test output",
            metadata={"key": "value"},
            success=True,
            duration_ms=100
        )

        self.assertEqual(response.output, "test output")
        self.assertEqual(response.metadata, {"key": "value"})
        self.assertTrue(response.success)
        self.assertIsNone(response.error)
        self.assertEqual(response.duration_ms, 100)

    def test_init_error(self):
        """Test error response initialization"""
        response = AgentResponse(
            output="",
            success=False,
            error="Something went wrong"
        )

        self.assertEqual(response.output, "")
        self.assertFalse(response.success)
        self.assertEqual(response.error, "Something went wrong")

    def test_init_defaults(self):
        """Test default values"""
        response = AgentResponse(output="test")

        self.assertEqual(response.output, "test")
        self.assertEqual(response.metadata, {})
        self.assertTrue(response.success)
        self.assertIsNone(response.error)
        self.assertEqual(response.duration_ms, 0)

    def test_repr(self):
        """Test string representation"""
        response = AgentResponse(output="a" * 100, success=True)
        repr_str = repr(response)

        self.assertIn("AgentResponse", repr_str)
        self.assertIn("success=True", repr_str)
        # Output should be truncated to 50 chars
        self.assertIn("...", repr_str)


def create_mock_session():
    """Helper to create a mock requests session"""
    return MagicMock()


def mock_response(json_data, status_code=200):
    """Helper to create a mock response"""
    resp = Mock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status = Mock()
    return resp


class TestAgentClient(unittest.TestCase):
    """Tests for AgentClient class"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = AgentClient(server_url="http://test-server:8080")
        # Inject mock session
        self.mock_session = create_mock_session()
        self.client._session = self.mock_session

    def tearDown(self):
        """Clean up"""
        self.client.close()

    def test_execute_success(self):
        """Test successful agent execution"""
        self.mock_session.post.return_value = mock_response({
            "output": "Hello!",
            "metadata": {"agent_type": "echo"},
            "success": True
        })

        response = self.client.execute("echo", "Hello")

        self.assertTrue(response.success)
        self.assertEqual(response.output, "Hello!")
        self.assertEqual(response.metadata["agent_type"], "echo")

        # Verify request
        self.mock_session.post.assert_called_once()
        call_args = self.mock_session.post.call_args
        self.assertIn("/api/v1/agents/execute", call_args[0][0])

    def test_execute_with_context(self):
        """Test execution with context"""
        self.mock_session.post.return_value = mock_response({
            "output": "test", "success": True
        })

        self.client.execute(
            "test-agent",
            "input",
            context={"key": "value"},
            session_id="sess-123",
            user_id="user-456"
        )

        # Verify payload
        call_args = self.mock_session.post.call_args
        payload = call_args[1]["json"]

        self.assertEqual(payload["agent_id"], "test-agent")
        self.assertEqual(payload["input"], "input")
        self.assertEqual(payload["context"]["key"], "value")
        self.assertEqual(payload["session_id"], "sess-123")
        self.assertEqual(payload["user_id"], "user-456")

    def test_execute_http_error(self):
        """Test HTTP error handling"""
        self.mock_session.post.side_effect = Exception("Connection refused")

        response = self.client.execute("agent", "input")

        self.assertFalse(response.success)
        self.assertIn("Connection refused", response.error)

    def test_batch_execute_success(self):
        """Test successful batch execution"""
        self.mock_session.post.return_value = mock_response({
            "results": [
                {"output": "result1", "success": True, "metadata": {}},
                {"output": "result2", "success": True, "metadata": {}},
                {"output": "result3", "success": True, "metadata": {}}
            ]
        })

        inputs = ["input1", "input2", "input3"]
        responses = self.client.batch_execute("sentiment", inputs)

        self.assertEqual(len(responses), 3)
        for i, resp in enumerate(responses):
            self.assertTrue(resp.success)
            self.assertEqual(resp.output, f"result{i+1}")

    def test_batch_execute_error(self):
        """Test batch execution error handling"""
        self.mock_session.post.side_effect = Exception("Server error")

        inputs = ["input1", "input2"]
        responses = self.client.batch_execute("agent", inputs)

        # Should return error responses for all inputs
        self.assertEqual(len(responses), 2)
        for resp in responses:
            self.assertFalse(resp.success)
            self.assertIn("Server error", resp.error)

    def test_health_check_healthy(self):
        """Test health check when server is healthy"""
        self.mock_session.get.return_value = mock_response(
            {"status": "healthy"}, status_code=200
        )

        result = self.client.health_check()

        self.assertTrue(result)
        self.mock_session.get.assert_called_once()

    def test_health_check_unhealthy(self):
        """Test health check when server is down"""
        self.mock_session.get.side_effect = Exception("Connection refused")

        result = self.client.health_check()

        self.assertFalse(result)

    def test_list_agents(self):
        """Test listing agents"""
        self.mock_session.get.return_value = mock_response({
            "agents": [
                {"id": "echo", "name": "Echo Agent"},
                {"id": "summarizer", "name": "Summarizer Agent"}
            ]
        })

        agents = self.client.list_agents()

        self.assertEqual(len(agents), 2)
        self.assertEqual(agents[0]["id"], "echo")

    def test_list_agents_error(self):
        """Test list agents error handling"""
        self.mock_session.get.side_effect = Exception("Error")

        agents = self.client.list_agents()

        self.assertEqual(agents, [])

    def test_context_manager(self):
        """Test context manager protocol"""
        with AgentClient("http://test:8080") as client:
            self.assertIsNotNone(client)

    def test_server_url_from_env(self):
        """Test server URL from environment variable"""
        with patch.dict(os.environ, {"AGENT_SERVER_URL": "http://env-server:9090"}):
            client = AgentClient()
            self.assertEqual(client.server_url, "http://env-server:9090")

    def test_default_server_url(self):
        """Test default server URL"""
        os.environ.pop("AGENT_SERVER_URL", None)
        client = AgentClient()
        self.assertEqual(client.server_url, "http://localhost:8080")


class TestAgentClientPool(unittest.TestCase):
    """Tests for AgentClientPool class"""

    def test_pool_creation(self):
        """Test pool initialization"""
        pool = AgentClientPool(server_url="http://test:8080", pool_size=5)

        self.assertEqual(pool.pool_size, 5)
        self.assertEqual(len(pool.clients), 5)

        pool.close_all()

    def test_round_robin(self):
        """Test round-robin client selection"""
        pool = AgentClientPool(pool_size=3)

        client0 = pool.get_client()
        client1 = pool.get_client()
        client2 = pool.get_client()
        client3 = pool.get_client()  # Should wrap around

        self.assertEqual(client0, pool.clients[0])
        self.assertEqual(client1, pool.clients[1])
        self.assertEqual(client2, pool.clients[2])
        self.assertEqual(client3, pool.clients[0])  # Wrapped

        pool.close_all()

    def test_pool_execute(self):
        """Test execution through pool"""
        pool = AgentClientPool(pool_size=2)

        # Inject mock session into first client
        mock_session = create_mock_session()
        mock_session.post.return_value = mock_response({
            "output": "test", "success": True
        })
        pool.clients[0]._session = mock_session

        response = pool.execute("agent", "input")

        self.assertTrue(response.success)
        pool.close_all()

    def test_context_manager(self):
        """Test pool context manager"""
        with AgentClientPool(pool_size=2) as pool:
            self.assertIsNotNone(pool)


class TestAgent(unittest.TestCase):
    """Tests for Agent wrapper class"""

    def test_execute(self):
        """Test agent execution"""
        agent = Agent("echo", server_url="http://test:8080")

        # Inject mock session
        mock_session = create_mock_session()
        mock_session.post.return_value = mock_response({
            "output": "Echo: Hello",
            "success": True,
            "metadata": {}
        })
        agent._client = AgentClient("http://test:8080")
        agent._client._session = mock_session

        response = agent.execute("Hello")

        self.assertTrue(response.success)
        self.assertEqual(response.output, "Echo: Hello")

    def test_batch_execute(self):
        """Test agent batch execution"""
        agent = Agent("summarizer")

        mock_session = create_mock_session()
        mock_session.post.return_value = mock_response({
            "results": [
                {"output": "r1", "success": True, "metadata": {}},
                {"output": "r2", "success": True, "metadata": {}}
            ]
        })
        agent._client = AgentClient("http://test:8080")
        agent._client._session = mock_session

        responses = agent.batch_execute(["input1", "input2"])

        self.assertEqual(len(responses), 2)

    def test_callable(self):
        """Test agent as callable"""
        agent = Agent("test")

        mock_session = create_mock_session()
        mock_session.post.return_value = mock_response({
            "output": "Result",
            "success": True,
            "metadata": {}
        })
        agent._client = AgentClient("http://test:8080")
        agent._client._session = mock_session

        result = agent("input")  # Call directly

        self.assertEqual(result, "Result")

    def test_callable_error(self):
        """Test callable returns empty string on error"""
        agent = Agent("test")

        mock_session = create_mock_session()
        mock_session.post.side_effect = Exception("Error")
        agent._client = AgentClient("http://test:8080")
        agent._client._session = mock_session

        result = agent("input")

        self.assertEqual(result, "")

    def test_repr(self):
        """Test agent string representation"""
        agent = Agent("test-agent")
        repr_str = repr(agent)

        self.assertIn("Agent", repr_str)
        self.assertIn("test-agent", repr_str)

    def test_lazy_client_initialization(self):
        """Test client is lazily initialized"""
        agent = Agent("test")
        self.assertIsNone(agent._client)

        # Access client property triggers initialization
        # (but we won't actually make a request)


class TestAgentSubclasses(unittest.TestCase):
    """Tests for Agent subclasses"""

    def test_llm_agent(self):
        """Test LlmAgent initialization"""
        agent = LlmAgent(
            "summarizer",
            model="gpt-4",
            system_prompt="You are a summarizer"
        )

        self.assertEqual(agent.agent_id, "summarizer")
        self.assertEqual(agent.model, "gpt-4")
        self.assertEqual(agent.system_prompt, "You are a summarizer")

    def test_sequential_agent(self):
        """Test SequentialAgent initialization"""
        agent = SequentialAgent(
            "pipeline",
            sub_agents=["step1", "step2", "step3"]
        )

        self.assertEqual(agent.agent_id, "pipeline")
        self.assertEqual(agent.sub_agents, ["step1", "step2", "step3"])

    def test_parallel_agent(self):
        """Test ParallelAgent initialization"""
        agent = ParallelAgent(
            "multi-analysis",
            sub_agents=["sentiment", "topic", "entity"]
        )

        self.assertEqual(agent.agent_id, "multi-analysis")
        self.assertEqual(len(agent.sub_agents), 3)

    def test_loop_agent(self):
        """Test LoopAgent initialization"""
        agent = LoopAgent(
            "refiner",
            max_iterations=5
        )

        self.assertEqual(agent.agent_id, "refiner")
        self.assertEqual(agent.max_iterations, 5)


class TestCreateAgent(unittest.TestCase):
    """Tests for create_agent factory function"""

    def test_create_llm_agent(self):
        """Test creating LLM agent"""
        agent = create_agent("llm", "test", model="gpt-4")

        self.assertIsInstance(agent, LlmAgent)
        self.assertEqual(agent.model, "gpt-4")

    def test_create_sequential_agent(self):
        """Test creating sequential agent"""
        agent = create_agent("sequential", "pipeline", sub_agents=["a", "b"])

        self.assertIsInstance(agent, SequentialAgent)
        self.assertEqual(agent.sub_agents, ["a", "b"])

    def test_create_parallel_agent(self):
        """Test creating parallel agent"""
        agent = create_agent("parallel", "multi")

        self.assertIsInstance(agent, ParallelAgent)

    def test_create_loop_agent(self):
        """Test creating loop agent"""
        agent = create_agent("loop", "iter", max_iterations=3)

        self.assertIsInstance(agent, LoopAgent)
        self.assertEqual(agent.max_iterations, 3)

    def test_create_unknown_type(self):
        """Test creating unknown agent type falls back to base Agent"""
        agent = create_agent("unknown", "test")

        self.assertIsInstance(agent, Agent)
        self.assertNotIsInstance(agent, LlmAgent)

    def test_case_insensitive(self):
        """Test agent type is case insensitive"""
        agent1 = create_agent("LLM", "test")
        agent2 = create_agent("llm", "test")
        agent3 = create_agent("Llm", "test")

        self.assertIsInstance(agent1, LlmAgent)
        self.assertIsInstance(agent2, LlmAgent)
        self.assertIsInstance(agent3, LlmAgent)


class TestIntegration(unittest.TestCase):
    """Integration tests with mocked HTTP"""

    def test_full_workflow(self):
        """Test a complete workflow"""
        with AgentClient("http://test:8080") as client:
            # Inject mock session
            mock_session = create_mock_session()
            client._session = mock_session

            # Setup mock responses
            health_response = mock_response({"status": "healthy"}, 200)
            list_response = mock_response({"agents": [{"id": "sentiment"}]})
            execute_response = mock_response({
                "output": "positive",
                "success": True,
                "metadata": {"score": "0.9"}
            })

            mock_session.get.side_effect = [health_response, list_response]
            mock_session.post.return_value = execute_response

            # Check health
            self.assertTrue(client.health_check())

            # List agents
            agents = client.list_agents()
            self.assertEqual(len(agents), 1)

            # Execute
            response = client.execute("sentiment", "Great product!")
            self.assertTrue(response.success)
            self.assertEqual(response.output, "positive")


if __name__ == "__main__":
    unittest.main(verbosity=2)

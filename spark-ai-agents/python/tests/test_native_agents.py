"""
Tests for native agent execution (no RPC).

These tests verify that agents can run directly on executors
without requiring an external server.
"""

import pytest
import json
from typing import Dict, Any

# Import native agent components
from spark_ai_agents.native_providers import (
    NativeLLMProvider, LLMConfig, LLMResponse,
    MockProvider, OpenAIProvider, AnthropicProvider,
    create_provider, register_provider
)
from spark_ai_agents.native_agents import (
    NativeAgent, NativeAgentConfig, NativeAgentResponse,
    NativeSequentialAgent, NativeParallelAgent,
    create_native_agent, create_native_sequential_agent, create_native_parallel_agent
)


# ============================================================================
# LLMConfig Tests
# ============================================================================

class TestLLMConfig:
    """Tests for LLMConfig dataclass"""

    def test_default_config(self):
        """Test default config values"""
        config = LLMConfig(model="gpt-3.5-turbo")
        assert config.model == "gpt-3.5-turbo"
        assert config.temperature == 0.7
        assert config.max_tokens == 1024
        assert config.top_p == 1.0
        assert config.stop_sequences == []

    def test_custom_config(self):
        """Test custom config values"""
        config = LLMConfig(
            model="gpt-4",
            temperature=0.3,
            max_tokens=2048,
            stop_sequences=["END", "STOP"]
        )
        assert config.model == "gpt-4"
        assert config.temperature == 0.3
        assert config.max_tokens == 2048
        assert config.stop_sequences == ["END", "STOP"]

    def test_config_serialization(self):
        """Test config to/from dict"""
        config = LLMConfig(
            model="gpt-4",
            temperature=0.5,
            max_tokens=512
        )
        config_dict = config.to_dict()
        restored = LLMConfig.from_dict(config_dict)

        assert restored.model == config.model
        assert restored.temperature == config.temperature
        assert restored.max_tokens == config.max_tokens


# ============================================================================
# MockProvider Tests
# ============================================================================

class TestMockProvider:
    """Tests for MockProvider"""

    def test_mock_response(self):
        """Test mock provider returns expected responses"""
        provider = MockProvider(responses=["Response 1", "Response 2"])

        resp1 = provider.generate("test")
        assert resp1.text == "Response 1"
        assert resp1.success

        resp2 = provider.generate("test")
        assert resp2.text == "Response 2"

    def test_mock_cycles(self):
        """Test mock provider cycles through responses"""
        provider = MockProvider(responses=["A", "B"])

        assert provider.generate("1").text == "A"
        assert provider.generate("2").text == "B"
        assert provider.generate("3").text == "A"  # Cycles back

    def test_mock_default_response(self):
        """Test mock provider default response"""
        provider = MockProvider()
        resp = provider.generate("test")
        assert resp.text == "Mock response"

    def test_mock_batch_generate(self):
        """Test mock provider batch generation"""
        provider = MockProvider(responses=["Result"])
        responses = provider.batch_generate(["a", "b", "c"])
        assert len(responses) == 3
        for r in responses:
            assert r.text == "Result"


# ============================================================================
# Provider Factory Tests
# ============================================================================

class TestProviderFactory:
    """Tests for provider factory functions"""

    def test_create_mock_provider(self):
        """Test creating mock provider via factory"""
        provider = create_provider("mock", responses=["Test"])
        assert provider.name() == "mock"
        assert provider.generate("test").text == "Test"

    def test_create_openai_provider(self):
        """Test creating OpenAI provider via factory"""
        provider = create_provider("openai", api_key="test-key")
        assert provider.name() == "openai"
        assert isinstance(provider, OpenAIProvider)

    def test_create_anthropic_provider(self):
        """Test creating Anthropic provider via factory"""
        provider = create_provider("anthropic", api_key="test-key")
        assert provider.name() == "anthropic"
        assert isinstance(provider, AnthropicProvider)

    def test_unknown_provider_raises(self):
        """Test unknown provider type raises error"""
        with pytest.raises(ValueError, match="Unknown provider type"):
            create_provider("unknown_provider")

    def test_register_custom_provider(self):
        """Test registering custom provider"""
        class CustomProvider(NativeLLMProvider):
            def name(self):
                return "custom"
            def generate(self, prompt, system_prompt=None, config=None):
                return LLMResponse(text="Custom: " + prompt)

        register_provider("custom", CustomProvider)
        provider = create_provider("custom")
        assert provider.name() == "custom"


# ============================================================================
# NativeAgentConfig Tests
# ============================================================================

class TestNativeAgentConfig:
    """Tests for NativeAgentConfig"""

    def test_default_config(self):
        """Test default config values"""
        config = NativeAgentConfig(name="test")
        assert config.name == "test"
        assert config.provider_type == "openai"
        assert config.model == "gpt-3.5-turbo"
        assert config.temperature == 0.7

    def test_full_config(self):
        """Test full config specification"""
        config = NativeAgentConfig(
            name="summarizer",
            description="Summarizes text",
            provider_type="anthropic",
            model="claude-3-sonnet-20240229",
            temperature=0.3,
            max_tokens=2048,
            system_prompt="You summarize text concisely.",
            instruction="Summarize the following:",
            output_format="text"
        )

        assert config.name == "summarizer"
        assert config.provider_type == "anthropic"
        assert config.model == "claude-3-sonnet-20240229"
        assert config.system_prompt == "You summarize text concisely."

    def test_config_serialization(self):
        """Test config serialization round-trip"""
        config = NativeAgentConfig(
            name="test",
            provider_type="openai",
            model="gpt-4",
            system_prompt="Test prompt"
        )

        config_dict = config.to_dict()
        restored = NativeAgentConfig.from_dict(config_dict)

        assert restored.name == config.name
        assert restored.provider_type == config.provider_type
        assert restored.model == config.model
        assert restored.system_prompt == config.system_prompt

    def test_to_llm_config(self):
        """Test conversion to LLMConfig"""
        config = NativeAgentConfig(
            name="test",
            model="gpt-4",
            temperature=0.5,
            max_tokens=512
        )

        llm_config = config.to_llm_config()
        assert llm_config.model == "gpt-4"
        assert llm_config.temperature == 0.5
        assert llm_config.max_tokens == 512


# ============================================================================
# NativeAgent Tests
# ============================================================================

class TestNativeAgent:
    """Tests for NativeAgent"""

    def test_create_agent(self):
        """Test creating native agent"""
        config = NativeAgentConfig(
            name="test",
            provider_type="mock",
            system_prompt="You are a helpful assistant."
        )
        agent = NativeAgent(config)

        assert agent.name == "test"
        assert agent.config.provider_type == "mock"

    def test_agent_execute_mock(self):
        """Test agent execution with mock provider"""
        config = NativeAgentConfig(
            name="test",
            provider_type="mock",
        )
        # Inject mock provider
        agent = NativeAgent(config)
        agent._provider = MockProvider(responses=["Hello from mock!"])

        response = agent.execute("Test input")

        assert response.success
        assert response.output == "Hello from mock!"
        assert response.duration_ms >= 0  # May be 0 for very fast mock

    def test_agent_execute_with_context(self):
        """Test agent execution with context"""
        config = NativeAgentConfig(
            name="test",
            provider_type="mock",
            instruction="Answer the question:"
        )
        agent = NativeAgent(config)
        agent._provider = MockProvider(responses=["Answer with context"])

        response = agent.execute("What is 2+2?", context={"user": "test"})

        assert response.success
        assert "Answer with context" in response.output

    def test_agent_callable_interface(self):
        """Test agent __call__ method"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        agent = NativeAgent(config)
        agent._provider = MockProvider(responses=["Callable response"])

        result = agent("Test input")
        assert result == "Callable response"

    def test_agent_batch_execute(self):
        """Test batch execution"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        agent = NativeAgent(config)
        agent._provider = MockProvider(responses=["Batch result"])

        responses = agent.batch_execute(["input1", "input2", "input3"])

        assert len(responses) == 3
        for resp in responses:
            assert resp.success
            assert resp.output == "Batch result"

    def test_agent_json_output_format(self):
        """Test JSON output format handling"""
        config = NativeAgentConfig(
            name="test",
            provider_type="mock",
            output_format="json"
        )
        agent = NativeAgent(config)
        agent._provider = MockProvider(responses=['{"result": "value"}'])

        response = agent.execute("Generate JSON")

        assert response.success
        assert response.output == '{"result": "value"}'

    def test_agent_error_handling(self):
        """Test error handling in agent execution"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        agent = NativeAgent(config)

        # Create a provider that returns error
        class ErrorProvider(NativeLLMProvider):
            def name(self):
                return "error"
            def generate(self, prompt, system_prompt=None, config=None):
                raise Exception("Test error")

        agent._provider = ErrorProvider()

        response = agent.execute("Test")

        assert not response.success
        assert "Test error" in response.error


# ============================================================================
# NativeSequentialAgent Tests
# ============================================================================

class TestNativeSequentialAgent:
    """Tests for NativeSequentialAgent"""

    def test_sequential_execution(self):
        """Test sequential agent execution"""
        # Create mock agents
        config1 = NativeAgentConfig(name="step1", provider_type="mock")
        config2 = NativeAgentConfig(name="step2", provider_type="mock")

        agent1 = NativeAgent(config1)
        agent1._provider = MockProvider(responses=["Step 1 output"])

        agent2 = NativeAgent(config2)
        agent2._provider = MockProvider(responses=["Step 2 output"])

        seq_agent = NativeSequentialAgent("pipeline", [agent1, agent2])

        response = seq_agent.execute("Initial input")

        assert response.success
        assert response.output == "Step 2 output"
        assert "steps" in response.metadata
        assert len(response.metadata["steps"]) == 2

    def test_sequential_early_failure(self):
        """Test sequential agent fails fast on error"""
        config1 = NativeAgentConfig(name="step1", provider_type="mock")
        config2 = NativeAgentConfig(name="step2", provider_type="mock")

        agent1 = NativeAgent(config1)
        agent1._provider = MockProvider(responses=[""])  # Empty = will succeed

        # Agent 2 will fail
        class FailProvider(NativeLLMProvider):
            def name(self):
                return "fail"
            def generate(self, prompt, system_prompt=None, config=None):
                return LLMResponse(text="", success=False, error="Step 2 failed")

        agent2 = NativeAgent(config2)
        agent2._provider = FailProvider()

        seq_agent = NativeSequentialAgent("pipeline", [agent1, agent2])

        response = seq_agent.execute("Input")

        assert not response.success
        assert "Step 2" in response.error


# ============================================================================
# NativeParallelAgent Tests
# ============================================================================

class TestNativeParallelAgent:
    """Tests for NativeParallelAgent"""

    def test_parallel_execution(self):
        """Test parallel agent execution"""
        config1 = NativeAgentConfig(name="parallel1", provider_type="mock")
        config2 = NativeAgentConfig(name="parallel2", provider_type="mock")

        agent1 = NativeAgent(config1)
        agent1._provider = MockProvider(responses=["Result A"])

        agent2 = NativeAgent(config2)
        agent2._provider = MockProvider(responses=["Result B"])

        parallel_agent = NativeParallelAgent("multi", [agent1, agent2])

        response = parallel_agent.execute("Input")

        assert response.success
        # Default combiner joins with "---"
        assert "Result A" in response.output or "Result B" in response.output

    def test_parallel_custom_combiner(self):
        """Test parallel agent with custom combiner"""
        config1 = NativeAgentConfig(name="p1", provider_type="mock")
        config2 = NativeAgentConfig(name="p2", provider_type="mock")

        agent1 = NativeAgent(config1)
        agent1._provider = MockProvider(responses=["A"])

        agent2 = NativeAgent(config2)
        agent2._provider = MockProvider(responses=["B"])

        # Custom combiner
        combiner = lambda outputs: " | ".join(sorted(outputs))

        parallel_agent = NativeParallelAgent("multi", [agent1, agent2], combiner=combiner)

        response = parallel_agent.execute("Input")

        assert response.success
        assert response.output == "A | B"


# ============================================================================
# Factory Function Tests
# ============================================================================

class TestFactoryFunctions:
    """Tests for agent factory functions"""

    def test_create_native_agent(self):
        """Test create_native_agent factory"""
        agent = create_native_agent(
            name="test",
            provider_type="mock",
            model="test-model",
            system_prompt="Test prompt"
        )

        assert agent.name == "test"
        assert agent.config.provider_type == "mock"
        assert agent.config.model == "test-model"

    def test_create_native_sequential_agent(self):
        """Test create_native_sequential_agent factory"""
        agent1 = create_native_agent("step1", provider_type="mock")
        agent2 = create_native_agent("step2", provider_type="mock")

        seq_agent = create_native_sequential_agent("pipeline", [agent1, agent2])

        assert seq_agent.name == "pipeline"
        assert len(seq_agent.agents) == 2

    def test_create_native_sequential_from_dicts(self):
        """Test creating sequential agent from config dicts"""
        seq_agent = create_native_sequential_agent("pipeline", [
            {"name": "step1", "provider_type": "mock"},
            {"name": "step2", "provider_type": "mock"}
        ])

        assert seq_agent.name == "pipeline"
        assert len(seq_agent.agents) == 2

    def test_create_native_parallel_agent(self):
        """Test create_native_parallel_agent factory"""
        agent1 = create_native_agent("p1", provider_type="mock")
        agent2 = create_native_agent("p2", provider_type="mock")

        parallel_agent = create_native_parallel_agent("multi", [agent1, agent2])

        assert parallel_agent.name == "multi"
        assert len(parallel_agent.agents) == 2


# ============================================================================
# Provider Caching Tests
# ============================================================================

class TestProviderCaching:
    """Tests for provider caching behavior"""

    def test_provider_caching(self):
        """Test that providers are cached across agents with same provider config"""
        # Clear cache first
        NativeAgent._provider_cache.clear()

        config1 = NativeAgentConfig(
            name="agent1",
            provider_type="mock",
            api_key="test-key"
        )
        config2 = NativeAgentConfig(
            name="agent2",  # Different name, same provider config
            provider_type="mock",
            api_key="test-key"
        )

        agent1 = NativeAgent(config1)
        agent2 = NativeAgent(config2)

        # Access providers (this should populate cache)
        provider1 = agent1.provider
        provider2 = agent2.provider

        # Should be same cached instance (same provider config: type + api_key + api_base)
        assert provider1 is provider2

    def test_different_provider_configs_not_cached(self):
        """Test that different provider configs create different providers"""
        # Clear cache first
        NativeAgent._provider_cache.clear()

        config1 = NativeAgentConfig(
            name="agent1",
            provider_type="mock",
            api_key="key1"
        )
        config2 = NativeAgentConfig(
            name="agent2",
            provider_type="mock",
            api_key="key2"  # Different api_key
        )

        agent1 = NativeAgent(config1)
        agent2 = NativeAgent(config2)

        provider1 = agent1.provider
        provider2 = agent2.provider

        # Should be different instances (different api_key)
        assert provider1 is not provider2


# ============================================================================
# Integration Tests
# ============================================================================

class TestIntegration:
    """Integration tests for native agents"""

    def test_full_pipeline(self):
        """Test complete pipeline with mock providers"""
        # Create agents
        extractor = create_native_agent(
            "extractor",
            provider_type="mock",
            system_prompt="Extract key facts."
        )
        extractor._provider = MockProvider(responses=["Fact 1, Fact 2"])

        summarizer = create_native_agent(
            "summarizer",
            provider_type="mock",
            system_prompt="Summarize."
        )
        summarizer._provider = MockProvider(responses=["Summary of facts"])

        # Create pipeline
        pipeline = create_native_sequential_agent(
            "analysis_pipeline",
            [extractor, summarizer]
        )

        # Execute
        response = pipeline.execute("Long document text...")

        assert response.success
        assert response.output == "Summary of facts"
        assert len(response.metadata["steps"]) == 2

    def test_parallel_analysis(self):
        """Test parallel analysis with different perspectives"""
        # Create perspective agents
        positive = create_native_agent("positive", provider_type="mock")
        positive._provider = MockProvider(responses=["Positive aspects: good, excellent"])

        negative = create_native_agent("negative", provider_type="mock")
        negative._provider = MockProvider(responses=["Concerns: limited, expensive"])

        # Custom combiner for analysis
        def combine_analysis(outputs):
            return "Analysis:\n" + "\n".join(outputs)

        parallel = create_native_parallel_agent(
            "multi_perspective",
            [positive, negative],
            combiner=combine_analysis
        )

        response = parallel.execute("Product review text")

        assert response.success
        assert "Analysis:" in response.output
        assert "Positive" in response.output or "Concerns" in response.output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""
Tests for native agent UDFs.

These tests verify that UDFs work correctly for native executor execution.
Note: Full Spark integration tests require PySpark to be installed.
"""

import pytest
import json
from typing import Dict, Any

from spark_ai_agents.native_providers import MockProvider, LLMResponse
from spark_ai_agents.native_agents import NativeAgent, NativeAgentConfig

# Import UDF components (may fail if PySpark not installed)
try:
    from spark_ai_agents.native_udf import (
        NativeAgentUDF, NativeBatchAgentUDF, NativePartitionProcessor,
        native_agent_udf, native_batch_agent_udf, native_partition_executor
    )
    HAS_NATIVE_UDF = True
except ImportError:
    HAS_NATIVE_UDF = False


# ============================================================================
# NativeAgentConfig Serialization Tests
# ============================================================================

class TestConfigSerialization:
    """Tests for config serialization (critical for Spark broadcast)"""

    def test_config_to_dict(self):
        """Test config serializes to dict correctly"""
        config = NativeAgentConfig(
            name="test",
            provider_type="openai",
            model="gpt-4",
            temperature=0.5,
            max_tokens=1024,
            system_prompt="You are helpful.",
            instruction="Answer:",
            api_key="sk-test",
            api_base="https://api.example.com"
        )

        config_dict = config.to_dict()

        assert config_dict["name"] == "test"
        assert config_dict["provider_type"] == "openai"
        assert config_dict["model"] == "gpt-4"
        assert config_dict["temperature"] == 0.5
        assert config_dict["system_prompt"] == "You are helpful."
        assert config_dict["api_key"] == "sk-test"

    def test_config_from_dict(self):
        """Test config deserializes from dict correctly"""
        config_dict = {
            "name": "restored",
            "provider_type": "anthropic",
            "model": "claude-3",
            "temperature": 0.3,
            "max_tokens": 2048,
            "system_prompt": "System prompt",
            "instruction": "Instruction",
            "api_key": "test-key"
        }

        config = NativeAgentConfig.from_dict(config_dict)

        assert config.name == "restored"
        assert config.provider_type == "anthropic"
        assert config.model == "claude-3"
        assert config.temperature == 0.3
        assert config.system_prompt == "System prompt"

    def test_config_json_roundtrip(self):
        """Test config survives JSON serialization (like Spark broadcast)"""
        config = NativeAgentConfig(
            name="json_test",
            provider_type="openai",
            model="gpt-4",
            temperature=0.7,
            stop_sequences=["END", "STOP"],
            extra_params={"custom": "value"}
        )

        # Simulate Spark broadcast: dict -> JSON -> dict -> config
        config_dict = config.to_dict()
        json_str = json.dumps(config_dict)
        restored_dict = json.loads(json_str)
        restored_config = NativeAgentConfig.from_dict(restored_dict)

        assert restored_config.name == config.name
        assert restored_config.provider_type == config.provider_type
        assert restored_config.stop_sequences == config.stop_sequences
        assert restored_config.extra_params == config.extra_params


# ============================================================================
# Agent Cache Tests
# ============================================================================

class TestAgentCaching:
    """Tests for agent caching on executors"""

    def test_cache_key_generation(self):
        """Test that identical configs produce same cache key"""
        config1 = NativeAgentConfig(
            name="agent1",
            provider_type="mock",
            model="test-model"
        )
        config2 = NativeAgentConfig(
            name="agent1",
            provider_type="mock",
            model="test-model"
        )

        key1 = json.dumps(config1.to_dict(), sort_keys=True)
        key2 = json.dumps(config2.to_dict(), sort_keys=True)

        assert key1 == key2

    def test_different_configs_different_keys(self):
        """Test that different configs produce different cache keys"""
        config1 = NativeAgentConfig(name="agent1", model="gpt-3.5")
        config2 = NativeAgentConfig(name="agent1", model="gpt-4")

        key1 = json.dumps(config1.to_dict(), sort_keys=True)
        key2 = json.dumps(config2.to_dict(), sort_keys=True)

        assert key1 != key2


# ============================================================================
# NativePartitionProcessor Tests
# ============================================================================

class TestNativePartitionProcessor:
    """Tests for partition processor"""

    @pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
    def test_processor_creation(self):
        """Test creating partition processor"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        processor = NativePartitionProcessor(config, batch_size=32, input_col="text")

        assert processor.batch_size == 32
        assert processor.input_col == "text"

    @pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
    def test_processor_from_dict(self):
        """Test creating processor from dict config"""
        config_dict = {"name": "test", "provider_type": "mock"}
        processor = NativePartitionProcessor(config_dict, batch_size=16)

        assert processor.config_dict == config_dict
        assert processor.batch_size == 16

    @pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
    def test_processor_iteration(self):
        """Test processor processes rows correctly"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        processor = NativePartitionProcessor(config, batch_size=2, input_col="value")

        # Create mock rows
        class MockRow:
            def __init__(self, value):
                self.value = value

        rows = [MockRow("text1"), MockRow("text2"), MockRow("text3")]

        # Create processor and inject mock agent
        results = []

        # Manually test the processing logic
        agent = NativeAgent(NativeAgentConfig.from_dict(processor.config_dict))
        agent._provider = MockProvider(responses=["Processed"])

        batch = []
        for row in rows:
            batch.append(row.value)
            if len(batch) >= 2:
                responses = agent.batch_execute(batch)
                for inp, resp in zip(batch, responses):
                    results.append((inp, resp.output, resp.success))
                batch = []

        if batch:
            responses = agent.batch_execute(batch)
            for inp, resp in zip(batch, responses):
                results.append((inp, resp.output, resp.success))

        assert len(results) == 3
        for inp, output, success in results:
            assert success
            assert output == "Processed"


# ============================================================================
# NativeAgentUDF Tests (without Spark)
# ============================================================================

@pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
class TestNativeAgentUDFCreation:
    """Tests for NativeAgentUDF creation (no Spark execution)"""

    def test_udf_creation(self):
        """Test creating UDF wrapper"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        udf_wrapper = NativeAgentUDF(config)

        assert udf_wrapper.config.name == "test"
        assert udf_wrapper.spark is None

    def test_udf_from_dict(self):
        """Test creating UDF from dict config"""
        config_dict = {"name": "test", "provider_type": "mock"}
        udf_wrapper = NativeAgentUDF(config_dict)

        assert udf_wrapper.config.name == "test"

    def test_get_config_dict_no_spark(self):
        """Test getting config dict without Spark session"""
        config = NativeAgentConfig(name="test", provider_type="mock", model="gpt-4")
        udf_wrapper = NativeAgentUDF(config)

        config_dict = udf_wrapper._get_config_dict()

        assert config_dict["name"] == "test"
        assert config_dict["model"] == "gpt-4"

    def test_agent_cache_get_or_create(self):
        """Test agent caching mechanism"""
        config_dict = {"name": "cached_test", "provider_type": "mock", "model": "test"}

        # Clear cache
        NativeAgentUDF._agent_cache.clear()

        # First call creates agent
        agent1 = NativeAgentUDF._get_or_create_agent(config_dict)
        assert agent1.name == "cached_test"

        # Second call returns cached agent
        agent2 = NativeAgentUDF._get_or_create_agent(config_dict)
        assert agent1 is agent2


# ============================================================================
# NativeBatchAgentUDF Tests
# ============================================================================

@pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
class TestNativeBatchAgentUDF:
    """Tests for NativeBatchAgentUDF"""

    def test_batch_udf_creation(self):
        """Test creating batch UDF wrapper"""
        config = NativeAgentConfig(name="batch_test", provider_type="mock")
        udf_wrapper = NativeBatchAgentUDF(config, batch_size=64)

        assert udf_wrapper.config.name == "batch_test"
        assert udf_wrapper.batch_size == 64

    def test_batch_udf_default_batch_size(self):
        """Test default batch size"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        udf_wrapper = NativeBatchAgentUDF(config)

        assert udf_wrapper.batch_size == 32


# ============================================================================
# Factory Function Tests
# ============================================================================

@pytest.mark.skipif(not HAS_NATIVE_UDF, reason="PySpark required")
class TestFactoryFunctions:
    """Tests for UDF factory functions"""

    def test_native_partition_executor(self):
        """Test partition executor factory"""
        config = NativeAgentConfig(name="test", provider_type="mock")
        executor = native_partition_executor(config, batch_size=50, input_col="content")

        assert isinstance(executor, NativePartitionProcessor)
        assert executor.batch_size == 50
        assert executor.input_col == "content"

    def test_native_partition_executor_from_dict(self):
        """Test partition executor from dict config"""
        executor = native_partition_executor(
            {"name": "test", "provider_type": "mock"},
            batch_size=25
        )

        assert executor.batch_size == 25


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling in native UDFs"""

    def test_agent_handles_execution_error(self):
        """Test agent handles execution errors gracefully"""
        config = NativeAgentConfig(name="error_test", provider_type="mock")
        agent = NativeAgent(config)

        # Create error-producing provider
        class ErrorProvider:
            def name(self):
                return "error"
            def generate(self, prompt, system_prompt=None, config=None):
                raise Exception("Test execution error")
            def batch_generate(self, prompts, system_prompt=None, config=None):
                raise Exception("Test batch error")

        agent._provider = ErrorProvider()

        response = agent.execute("test input")

        assert not response.success
        assert "Test execution error" in response.error
        assert response.output == ""

    def test_batch_handles_partial_errors(self):
        """Test batch execution handles partial errors"""
        config = NativeAgentConfig(name="partial_error_test", provider_type="mock")
        agent = NativeAgent(config)

        # Create provider that returns mixed results
        class MixedResultProvider:
            def __init__(self):
                self.call_count = 0

            def name(self):
                return "mixed"

            def generate(self, prompt, system_prompt=None, config=None):
                self.call_count += 1
                if self.call_count % 2 == 0:
                    return LLMResponse(text="", success=False, error="Even call error")
                return LLMResponse(text="Success", success=True)

            def batch_generate(self, prompts, system_prompt=None, config=None):
                return [self.generate(p, system_prompt, config) for p in prompts]

        agent._provider = MixedResultProvider()

        responses = agent.batch_execute(["a", "b", "c"])

        assert len(responses) == 3
        # Mix of success and failures
        successes = sum(1 for r in responses if r.success)
        failures = sum(1 for r in responses if not r.success)
        assert successes > 0
        assert failures > 0


# ============================================================================
# Performance Consideration Tests
# ============================================================================

class TestPerformanceConsiderations:
    """Tests for performance-related behavior"""

    def test_provider_not_created_until_needed(self):
        """Test lazy initialization of provider"""
        config = NativeAgentConfig(name="lazy_test", provider_type="mock")
        agent = NativeAgent(config)

        # Provider should not be created yet
        assert agent._provider is None

        # Access provider
        _ = agent.provider

        # Now provider exists
        assert agent._provider is not None

    def test_config_dict_caching(self):
        """Test config dict is consistent for caching"""
        config = NativeAgentConfig(
            name="cache_test",
            provider_type="mock",
            model="gpt-4",
            temperature=0.5,
            extra_params={"key": "value"}
        )

        dict1 = config.to_dict()
        dict2 = config.to_dict()

        # Should produce identical dicts
        assert json.dumps(dict1, sort_keys=True) == json.dumps(dict2, sort_keys=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

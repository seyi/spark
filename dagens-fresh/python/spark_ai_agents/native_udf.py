"""
Native Agent UDFs for Spark executor execution.

This module provides UDFs that run agent logic directly on Spark executors
without RPC overhead, enabling true distributed AI processing.
"""

from typing import Dict, List, Optional, Any, Union, Iterator
from dataclasses import dataclass
import json
import logging

from .native_agents import NativeAgent, NativeAgentConfig, NativeAgentResponse
from .native_providers import LLMConfig

logger = logging.getLogger(__name__)


class NativeAgentUDF:
    """
    Spark UDF that executes agent logic natively on executors.

    This UDF uses broadcast variables to distribute agent configuration
    to executors, where the agent is initialized lazily and reused
    across partition processing.

    Example:
        from pyspark.sql import SparkSession
        from spark_ai_agents import NativeAgentUDF, NativeAgentConfig

        spark = SparkSession.builder.getOrCreate()

        # Create agent config
        config = NativeAgentConfig(
            name="summarizer",
            provider_type="openai",
            model="gpt-3.5-turbo",
            system_prompt="Summarize the text."
        )

        # Create UDF
        udf = NativeAgentUDF(config, spark)
        summarize = udf.create_udf()

        # Apply to DataFrame
        df.withColumn("summary", summarize(df.text))
    """

    # Class-level agent cache per executor (keyed by config hash)
    _agent_cache: Dict[str, NativeAgent] = {}

    def __init__(self, config: Union[NativeAgentConfig, Dict[str, Any]],
                 spark_session=None):
        """
        Initialize NativeAgentUDF.

        Args:
            config: Agent configuration (NativeAgentConfig or dict)
            spark_session: SparkSession for broadcasting (optional)
        """
        if isinstance(config, dict):
            config = NativeAgentConfig.from_dict(config)
        self.config = config
        self.spark = spark_session
        self._broadcast_config = None

    def _get_broadcast_config(self):
        """
        Get or create broadcast variable for config.

        Broadcast variables efficiently distribute the config to all executors.
        """
        if self._broadcast_config is None:
            if self.spark is None:
                # Try to get active session
                try:
                    from pyspark.sql import SparkSession
                    self.spark = SparkSession.getActiveSession()
                except:
                    pass

            if self.spark is not None:
                self._broadcast_config = self.spark.sparkContext.broadcast(
                    self.config.to_dict()
                )
            else:
                # No spark session - config will be serialized with closure
                return None

        return self._broadcast_config

    def _get_config_dict(self) -> Dict[str, Any]:
        """Get config as dict (from broadcast or direct)"""
        broadcast = self._get_broadcast_config()
        if broadcast is not None:
            return broadcast.value
        return self.config.to_dict()

    @classmethod
    def _get_or_create_agent(cls, config_dict: Dict[str, Any]) -> NativeAgent:
        """
        Get or create agent on executor (cached).

        This method is called on executors and maintains a cache of agents
        to avoid recreating them for each row.
        """
        # Create cache key from config
        cache_key = json.dumps(config_dict, sort_keys=True)

        if cache_key not in cls._agent_cache:
            config = NativeAgentConfig.from_dict(config_dict)
            cls._agent_cache[cache_key] = NativeAgent(config)
            logger.info(f"Created native agent '{config.name}' on executor")

        return cls._agent_cache[cache_key]

    def create_udf(self, return_type=None):
        """
        Create Spark UDF for native agent execution.

        Args:
            return_type: Spark data type (default: StringType())

        Returns:
            Spark UDF function
        """
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        if return_type is None:
            return_type = StringType()

        # Get config dict (will use broadcast if available)
        config_dict = self._get_config_dict()

        def agent_func(text: str) -> str:
            """Execute agent natively on executor"""
            if text is None:
                return None

            try:
                # Get or create agent (cached on executor)
                agent = NativeAgentUDF._get_or_create_agent(config_dict)

                # Execute
                response = agent.execute(text)
                return response.output if response.success else None

            except Exception as e:
                logger.error(f"Native agent execution failed: {e}")
                return None

        return udf(agent_func, return_type)

    def create_udf_with_metadata(self):
        """
        Create Spark UDF that returns output with metadata as struct.

        Returns:
            Spark UDF function returning struct
        """
        from pyspark.sql.functions import udf
        from pyspark.sql.types import (
            StructType, StructField, StringType, BooleanType,
            IntegerType, MapType
        )

        # Return schema
        return_type = StructType([
            StructField("output", StringType(), True),
            StructField("success", BooleanType(), False),
            StructField("error", StringType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("tokens_used", IntegerType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])

        config_dict = self._get_config_dict()

        def agent_func(text: str):
            """Execute agent and return full response"""
            if text is None:
                return None

            try:
                agent = NativeAgentUDF._get_or_create_agent(config_dict)
                response = agent.execute(text)

                # Convert metadata to string values
                str_metadata = {
                    k: str(v) for k, v in response.metadata.items()
                }

                return (
                    response.output,
                    response.success,
                    response.error,
                    response.duration_ms,
                    response.tokens_used,
                    str_metadata
                )

            except Exception as e:
                return (None, False, str(e), 0, 0, {})

        return udf(agent_func, return_type)

    def __call__(self, return_type=None):
        """Create UDF (convenience method)"""
        return self.create_udf(return_type)


class NativeBatchAgentUDF:
    """
    Batch-enabled native UDF using pandas_udf for better performance.

    This uses vectorized execution to process multiple rows efficiently.
    """

    _agent_cache: Dict[str, NativeAgent] = {}

    def __init__(self, config: Union[NativeAgentConfig, Dict[str, Any]],
                 spark_session=None,
                 batch_size: int = 32):
        """
        Initialize NativeBatchAgentUDF.

        Args:
            config: Agent configuration
            spark_session: SparkSession for broadcasting
            batch_size: Rows per batch for LLM calls
        """
        if isinstance(config, dict):
            config = NativeAgentConfig.from_dict(config)
        self.config = config
        self.spark = spark_session
        self.batch_size = batch_size
        self._broadcast_config = None

    def _get_broadcast_config(self):
        """Get or create broadcast variable"""
        if self._broadcast_config is None:
            if self.spark is None:
                try:
                    from pyspark.sql import SparkSession
                    self.spark = SparkSession.getActiveSession()
                except:
                    pass

            if self.spark is not None:
                self._broadcast_config = self.spark.sparkContext.broadcast(
                    self.config.to_dict()
                )

        return self._broadcast_config

    def _get_config_dict(self) -> Dict[str, Any]:
        broadcast = self._get_broadcast_config()
        if broadcast is not None:
            return broadcast.value
        return self.config.to_dict()

    @classmethod
    def _get_or_create_agent(cls, config_dict: Dict[str, Any]) -> NativeAgent:
        """Get or create agent on executor"""
        cache_key = json.dumps(config_dict, sort_keys=True)

        if cache_key not in cls._agent_cache:
            config = NativeAgentConfig.from_dict(config_dict)
            cls._agent_cache[cache_key] = NativeAgent(config)

        return cls._agent_cache[cache_key]

    def create_udf(self, return_type=None):
        """
        Create pandas UDF for batch processing.

        Args:
            return_type: Spark data type (default: StringType())

        Returns:
            Pandas UDF function
        """
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import StringType
        import pandas as pd

        if return_type is None:
            return_type = StringType()

        config_dict = self._get_config_dict()
        batch_size = self.batch_size

        @pandas_udf(return_type)
        def batch_agent_func(texts: pd.Series) -> pd.Series:
            """Batch process using native agent"""
            # Get agent on executor
            agent = NativeBatchAgentUDF._get_or_create_agent(config_dict)

            # Convert to list
            inputs = texts.tolist()

            # Process in batches for LLM calls
            outputs = []
            for i in range(0, len(inputs), batch_size):
                batch = inputs[i:i + batch_size]

                # Filter None values
                valid_indices = [j for j, x in enumerate(batch) if x is not None]
                valid_inputs = [batch[j] for j in valid_indices]

                if valid_inputs:
                    responses = agent.batch_execute(valid_inputs)
                    batch_outputs = [None] * len(batch)
                    for idx, resp in zip(valid_indices, responses):
                        batch_outputs[idx] = resp.output if resp.success else None
                else:
                    batch_outputs = [None] * len(batch)

                outputs.extend(batch_outputs)

            return pd.Series(outputs)

        return batch_agent_func

    def __call__(self, return_type=None):
        """Create UDF (convenience method)"""
        return self.create_udf(return_type)


class NativePartitionProcessor:
    """
    Partition processor for mapPartitions-style execution.

    This provides the most efficient processing by minimizing
    serialization overhead and maximizing batch efficiency.
    """

    def __init__(self, config: Union[NativeAgentConfig, Dict[str, Any]],
                 batch_size: int = 32,
                 input_col: str = "value"):
        """
        Initialize partition processor.

        Args:
            config: Agent configuration
            batch_size: Batch size for LLM calls
            input_col: Column name containing input text
        """
        if isinstance(config, dict):
            self.config_dict = config
        else:
            self.config_dict = config.to_dict()
        self.batch_size = batch_size
        self.input_col = input_col

    def __call__(self, partition: Iterator) -> Iterator:
        """
        Process partition.

        Args:
            partition: Iterator over partition rows

        Yields:
            Tuples of (input, output, success, duration_ms)
        """
        # Create agent on this executor
        config = NativeAgentConfig.from_dict(self.config_dict)
        agent = NativeAgent(config)

        # Collect batch
        batch = []
        batch_rows = []

        for row in partition:
            # Extract input
            if hasattr(row, self.input_col):
                input_text = getattr(row, self.input_col)
            elif isinstance(row, dict):
                input_text = row.get(self.input_col)
            else:
                input_text = str(row)

            batch.append(input_text)
            batch_rows.append(row)

            # Process when batch is full
            if len(batch) >= self.batch_size:
                yield from self._process_batch(agent, batch, batch_rows)
                batch = []
                batch_rows = []

        # Process remaining
        if batch:
            yield from self._process_batch(agent, batch, batch_rows)

    def _process_batch(self, agent: NativeAgent,
                       inputs: List[str],
                       rows: List[Any]) -> Iterator:
        """Process batch of inputs"""
        responses = agent.batch_execute(inputs)

        for inp, resp, row in zip(inputs, responses, rows):
            yield (inp, resp.output, resp.success, resp.duration_ms)


# Convenience functions

def native_agent_udf(config: Union[NativeAgentConfig, Dict[str, Any]],
                     spark_session=None,
                     return_type=None,
                     with_metadata: bool = False):
    """
    Create native agent UDF (convenience function).

    Args:
        config: Agent configuration
        spark_session: SparkSession for broadcasting
        return_type: Spark return type
        with_metadata: Return struct with metadata

    Returns:
        Spark UDF function

    Example:
        from spark_ai_agents import native_agent_udf, NativeAgentConfig

        config = NativeAgentConfig(
            name="classifier",
            provider_type="openai",
            model="gpt-4",
            system_prompt="Classify the text as positive or negative."
        )

        classify = native_agent_udf(config)
        df.withColumn("sentiment", classify(df.review))
    """
    udf_wrapper = NativeAgentUDF(config, spark_session)

    if with_metadata:
        return udf_wrapper.create_udf_with_metadata()
    return udf_wrapper.create_udf(return_type)


def native_batch_agent_udf(config: Union[NativeAgentConfig, Dict[str, Any]],
                           spark_session=None,
                           batch_size: int = 32,
                           return_type=None):
    """
    Create batch-enabled native agent UDF (convenience function).

    Args:
        config: Agent configuration
        spark_session: SparkSession
        batch_size: Batch size for processing
        return_type: Spark return type

    Returns:
        Pandas UDF function

    Example:
        summarize = native_batch_agent_udf(config, batch_size=32)
        df.withColumn("summary", summarize(df.text))
    """
    udf_wrapper = NativeBatchAgentUDF(config, spark_session, batch_size)
    return udf_wrapper.create_udf(return_type)


def native_partition_executor(config: Union[NativeAgentConfig, Dict[str, Any]],
                              batch_size: int = 32,
                              input_col: str = "value"):
    """
    Create partition executor for mapPartitions.

    Args:
        config: Agent configuration
        batch_size: Batch size
        input_col: Input column name

    Returns:
        Callable for mapPartitions

    Example:
        executor = native_partition_executor(config, batch_size=64)
        result_rdd = df.rdd.mapPartitions(executor)
    """
    return NativePartitionProcessor(config, batch_size, input_col)

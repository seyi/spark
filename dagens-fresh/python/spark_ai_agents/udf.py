"""
Spark UDF (User Defined Function) integration for agents.

This module provides utilities to create Spark UDFs from agents,
enabling seamless integration with DataFrame operations.
"""

from typing import Callable, Optional
from pyspark.sql.types import StringType, StructType, StructField, MapType, BooleanType, IntegerType

from .agents import Agent
from .client import AgentClient


class AgentUDF:
    """
    Wrapper to create Spark UDFs from agents.

    Example:
        from spark_ai_agents import AgentUDF, LlmAgent

        # Create agent
        summarizer = Llm Agent("summarizer")

        # Create UDF
        summarize_udf = AgentUDF(summarizer)()

        # Use in DataFrame
        df.withColumn("summary", summarize_udf(df.text))
    """

    def __init__(self, agent: Agent, batch_size: int = 1):
        """
        Initialize AgentUDF.

        Args:
            agent: Agent instance to wrap
            batch_size: Number of rows to batch together (default: 1, no batching)
        """
        self.agent = agent
        self.batch_size = batch_size

    def __call__(self, return_type=None):
        """
        Create Spark UDF from agent.

        Args:
            return_type: Spark data type for return value (default: StringType())

        Returns:
            Spark UDF function
        """
        from pyspark.sql.functions import udf

        if return_type is None:
            return_type = StringType()

        def agent_func(text: str) -> str:
            """Inner function that executes agent"""
            if text is None:
                return None

            try:
                response = self.agent.execute(text)
                return response.output if response.success else None
            except Exception as e:
                # Return None on error (can be configured to return error message)
                return None

        return udf(agent_func, return_type)


class AgentUDFWithMetadata:
    """
    AgentUDF that returns both output and metadata as a struct.

    Example:
        udf = AgentUDFWithMetadata(agent)()
        df.withColumn("result", udf(df.text)) \
          .select("result.output", "result.success", "result.duration_ms")
    """

    def __init__(self, agent: Agent):
        """
        Initialize AgentUDFWithMetadata.

        Args:
            agent: Agent instance to wrap
        """
        self.agent = agent

    def __call__(self):
        """
        Create Spark UDF that returns struct with output and metadata.

        Returns:
            Spark UDF function
        """
        from pyspark.sql.functions import udf

        # Define return schema
        return_type = StructType([
            StructField("output", StringType(), True),
            StructField("success", BooleanType(), False),
            StructField("error", StringType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True)
        ])

        def agent_func(text: str):
            """Inner function that executes agent"""
            if text is None:
                return None

            try:
                response = self.agent.execute(text)
                return (
                    response.output,
                    response.success,
                    response.error,
                    response.duration_ms,
                    response.metadata
                )
            except Exception as e:
                return (None, False, str(e), 0, {})

        return udf(agent_func, return_type)


# Convenience function for creating agent UDFs

def agent_udf(agent: Agent, return_type=None, with_metadata: bool = False):
    """
    Create Spark UDF from agent (convenience function).

    Args:
        agent: Agent instance or agent ID string
        return_type: Spark data type for return value (default: StringType())
        with_metadata: Return struct with metadata (default: False)

    Returns:
        Spark UDF function

    Example:
        # Simple usage
        summarize = agent_udf(LlmAgent("summarizer"))
        df.withColumn("summary", summarize(df.text))

        # With metadata
        analyze = agent_udf(LlmAgent("analyzer"), with_metadata=True)
        df.withColumn("result", analyze(df.text))
    """
    # Handle string agent ID
    if isinstance(agent, str):
        agent = Agent(agent)

    if with_metadata:
        return AgentUDFWithMetadata(agent)()
    else:
        return AgentUDF(agent)(return_type)


# Batch UDF for improved performance

class BatchAgentUDF:
    """
    Batch-enabled UDF that processes multiple rows at once for better performance.

    This uses pandas_udf for vectorized execution.

    Example:
        from spark_ai_agents import BatchAgentUDF, LlmAgent

        udf = BatchAgentUDF(LlmAgent("summarizer"), batch_size=32)()
        df.withColumn("summary", udf(df.text))
    """

    def __init__(self, agent: Agent, batch_size: int = 32):
        """
        Initialize BatchAgentUDF.

        Args:
            agent: Agent instance
            batch_size: Number of rows to process per batch
        """
        self.agent = agent
        self.batch_size = batch_size

    def __call__(self, return_type=None):
        """
        Create pandas UDF for batch processing.

        Args:
            return_type: Spark data type (default: StringType())

        Returns:
            Pandas UDF function
        """
        from pyspark.sql.functions import pandas_udf
        import pandas as pd

        if return_type is None:
            return_type = StringType()

        @pandas_udf(return_type)
        def batch_agent_func(texts: pd.Series) -> pd.Series:
            """Batch process using pandas UDF"""
            # Convert to list
            inputs = texts.tolist()

            # Execute batch
            responses = self.agent.batch_execute(inputs)

            # Extract outputs
            outputs = [
                r.output if r.success else None
                for r in responses
            ]

            return pd.Series(outputs)

        return batch_agent_func


def batch_agent_udf(agent: Agent, batch_size: int = 32, return_type=None):
    """
    Create batch-enabled Spark UDF from agent (convenience function).

    This uses pandas_udf for better performance on large datasets.

    Args:
        agent: Agent instance or agent ID string
        batch_size: Number of rows to process per batch
        return_type: Spark data type (default: StringType())

    Returns:
        Pandas UDF function

    Example:
        summarize = batch_agent_udf(LlmAgent("summarizer"), batch_size=32)
        df.withColumn("summary", summarize(df.text))
    """
    # Handle string agent ID
    if isinstance(agent, str):
        agent = Agent(agent)

    return BatchAgentUDF(agent, batch_size)(return_type)

"""
DataFrame extensions for agent integration.

This module adds agent-specific methods to Spark DataFrames via monkey-patching.
Supports both RPC-based agents (via external server) and native agents
(direct execution on Spark executors).
"""

from typing import Union, Optional, Dict, Any
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DataType, StringType

from .agents import Agent
from .udf import agent_udf, batch_agent_udf
from .native_agents import NativeAgent, NativeAgentConfig
from .native_udf import NativeAgentUDF, NativeBatchAgentUDF, native_agent_udf, native_batch_agent_udf


def with_agent(self,
               agent: Union[Agent, str],
               input_col: Union[str, Column],
               output_col: str = None,
               batch_size: int = 1,
               return_type: DataType = None) -> DataFrame:
    """
    Apply agent to DataFrame column.

    Args:
        agent: Agent instance or agent ID string
        input_col: Input column name or Column object
        output_col: Output column name (default: f"{input_col}_agent_output")
        batch_size: Batch size for processing (default: 1, no batching)
        return_type: Return data type (default: StringType())

    Returns:
        DataFrame with new column

    Example:
        # Simple usage
        df.with_agent("summarizer", "text", "summary")

        # With agent instance
        from spark_ai_agents import LlmAgent
        agent = LlmAgent("classifier")
        df.with_agent(agent, "text", "category")

        # With batching for better performance
        df.with_agent("summarizer", "text", "summary", batch_size=32)
    """
    # Handle string agent ID
    if isinstance(agent, str):
        agent = Agent(agent)

    # Handle output column name
    if output_col is None:
        if isinstance(input_col, str):
            output_col = f"{input_col}_agent_output"
        else:
            output_col = "agent_output"

    # Handle return type
    if return_type is None:
        return_type = StringType()

    # Get input column
    if isinstance(input_col, str):
        input_column = self[input_col]
    else:
        input_column = input_col

    # Create UDF
    if batch_size > 1:
        udf_func = batch_agent_udf(agent, batch_size=batch_size, return_type=return_type)
    else:
        udf_func = agent_udf(agent, return_type=return_type)

    # Apply UDF
    return self.withColumn(output_col, udf_func(input_column))


def map_with_agent(self,
                   agent: Union[Agent, str],
                   input_col: str = "value",
                   batch_size: int = 32) -> DataFrame:
    """
    Map agent over DataFrame rows (RDD-style).

    This is useful for more complex transformations where you need
    full control over the mapping process.

    Args:
        agent: Agent instance or agent ID string
        input_col: Column containing input text (default: "value")
        batch_size: Batch size for processing

    Returns:
        DataFrame with agent results

    Example:
        # Process text column with agent
        df.map_with_agent("summarizer", "text")

        # Process with batching
        df.map_with_agent("classifier", "text", batch_size=64)
    """
    from .utils import AgentMapper

    # Handle string agent ID
    if isinstance(agent, str):
        agent = Agent(agent)

    # Create mapper
    mapper = AgentMapper(agent, batch_size=batch_size)

    # Map partitions
    result_rdd = self.rdd.mapPartitions(
        lambda partition: mapper.map_partition(partition, input_col)
    )

    # Convert back to DataFrame
    return self.sql_ctx.createDataFrame(result_rdd, ["input", "output", "success"])


def filter_with_agent(self,
                      agent: Union[Agent, str],
                      input_col: Union[str, Column],
                      threshold: float = 0.5) -> DataFrame:
    """
    Filter DataFrame based on agent decision.

    This is useful for classification/filtering use cases where the agent
    returns a score or decision.

    Args:
        agent: Agent instance or agent ID string
        input_col: Input column name or Column object
        threshold: Threshold for keeping rows (default: 0.5)

    Returns:
        Filtered DataFrame

    Example:
        # Filter spam messages
        df.filter_with_agent("spam-detector", "message", threshold=0.8)

        # Keep only positive sentiment
        df.filter_with_agent("sentiment-analyzer", "review", threshold=0.6)
    """
    from pyspark.sql.functions import col

    # Add agent column
    df_with_agent = with_agent(self, agent, input_col, "_agent_score")

    # Filter based on threshold (assumes agent returns numeric score)
    # Note: This is simplified; in practice, you might need to parse agent output
    return df_with_agent.filter(col("_agent_score").cast("float") >= threshold)


def transform_with_agent(self,
                         agent: Union[Agent, str],
                         transformations: dict) -> DataFrame:
    """
    Apply agent to multiple columns with different transformations.

    Args:
        agent: Agent instance or agent ID string
        transformations: Dict mapping input columns to output columns

    Returns:
        DataFrame with transformed columns

    Example:
        df.transform_with_agent("processor", {
            "title": "processed_title",
            "description": "processed_description",
            "content": "processed_content"
        })
    """
    result_df = self

    for input_col, output_col in transformations.items():
        result_df = with_agent(result_df, agent, input_col, output_col)

    return result_df


def with_native_agent(self,
                      config: Union[NativeAgentConfig, Dict[str, Any]],
                      input_col: Union[str, Column],
                      output_col: str = None,
                      batch_size: int = 1,
                      return_type: DataType = None,
                      with_metadata: bool = False) -> DataFrame:
    """
    Apply native agent to DataFrame column (NO RPC - direct executor execution).

    This method runs agent logic directly on Spark executors without
    requiring RPC to an external server, providing significantly better
    performance for distributed processing.

    Args:
        config: NativeAgentConfig or dict with agent configuration
        input_col: Input column name or Column object
        output_col: Output column name (default: f"{input_col}_native_output")
        batch_size: Batch size for processing (default: 1)
        return_type: Return data type (default: StringType())
        with_metadata: Return struct with metadata (default: False)

    Returns:
        DataFrame with new column

    Example:
        from spark_ai_agents import NativeAgentConfig

        # Create config
        config = NativeAgentConfig(
            name="summarizer",
            provider_type="openai",
            model="gpt-3.5-turbo",
            system_prompt="Summarize the text concisely."
        )

        # Apply to DataFrame (native execution on executors!)
        df.with_native_agent(config, "text", "summary")

        # With batching for better performance
        df.with_native_agent(config, "text", "summary", batch_size=32)

        # With metadata
        df.with_native_agent(config, "text", "result", with_metadata=True)
    """
    # Handle dict config
    if isinstance(config, dict):
        config = NativeAgentConfig.from_dict(config)

    # Handle output column name
    if output_col is None:
        if isinstance(input_col, str):
            output_col = f"{input_col}_native_output"
        else:
            output_col = "native_agent_output"

    # Handle return type
    if return_type is None:
        return_type = StringType()

    # Get input column
    if isinstance(input_col, str):
        input_column = self[input_col]
    else:
        input_column = input_col

    # Get SparkSession
    try:
        spark_session = self.sparkSession
    except:
        spark_session = None

    # Create UDF
    if with_metadata:
        udf_func = NativeAgentUDF(config, spark_session).create_udf_with_metadata()
    elif batch_size > 1:
        udf_func = native_batch_agent_udf(config, spark_session, batch_size, return_type)
    else:
        udf_func = native_agent_udf(config, spark_session, return_type)

    # Apply UDF
    return self.withColumn(output_col, udf_func(input_column))


def map_with_native_agent(self,
                          config: Union[NativeAgentConfig, Dict[str, Any]],
                          input_col: str = "value",
                          batch_size: int = 32) -> DataFrame:
    """
    Map native agent over DataFrame rows using mapPartitions.

    This provides maximum efficiency by processing entire partitions
    without per-row serialization overhead.

    Args:
        config: Agent configuration
        input_col: Column containing input text (default: "value")
        batch_size: Batch size for LLM calls

    Returns:
        DataFrame with columns: input, output, success, duration_ms

    Example:
        config = NativeAgentConfig(
            name="classifier",
            provider_type="openai",
            model="gpt-4",
            system_prompt="Classify as positive or negative."
        )

        result = df.map_with_native_agent(config, "text", batch_size=64)
    """
    from .native_udf import NativePartitionProcessor

    # Handle dict config
    if isinstance(config, dict):
        config_dict = config
    else:
        config_dict = config.to_dict()

    # Create processor
    processor = NativePartitionProcessor(config_dict, batch_size, input_col)

    # Map partitions
    result_rdd = self.rdd.mapPartitions(processor)

    # Convert back to DataFrame
    return self.sql_ctx.createDataFrame(
        result_rdd,
        ["input", "output", "success", "duration_ms"]
    )


def transform_with_native_agent(self,
                                config: Union[NativeAgentConfig, Dict[str, Any]],
                                transformations: dict) -> DataFrame:
    """
    Apply native agent to multiple columns with different transformations.

    Args:
        config: Agent configuration
        transformations: Dict mapping input columns to output columns

    Returns:
        DataFrame with transformed columns

    Example:
        df.transform_with_native_agent(config, {
            "title": "processed_title",
            "description": "processed_description",
            "content": "processed_content"
        })
    """
    result_df = self

    for input_col, output_col in transformations.items():
        result_df = with_native_agent(result_df, config, input_col, output_col)

    return result_df


def register_dataframe_extensions():
    """
    Register DataFrame extensions (monkey-patch DataFrame class).

    This function is called automatically when the package is imported.
    """
    # RPC-based agent methods (original)
    DataFrame.with_agent = with_agent
    DataFrame.map_with_agent = map_with_agent
    DataFrame.filter_with_agent = filter_with_agent
    DataFrame.transform_with_agent = transform_with_agent

    # Native agent methods (NEW - no RPC!)
    DataFrame.with_native_agent = with_native_agent
    DataFrame.map_with_native_agent = map_with_native_agent
    DataFrame.transform_with_native_agent = transform_with_native_agent


# Utility class for DataFrame operations

class AgentDataFrame:
    """
    Wrapper class that provides agent-specific DataFrame operations.

    This is an alternative to monkey-patching for users who prefer
    explicit wrapper classes.

    Example:
        from spark_ai_agents import AgentDataFrame, LlmAgent

        adf = AgentDataFrame(df)
        result = adf.with_agent(LlmAgent("summarizer"), "text", "summary")
    """

    def __init__(self, df: DataFrame):
        """
        Initialize AgentDataFrame wrapper.

        Args:
            df: Spark DataFrame to wrap
        """
        self.df = df

    def with_agent(self, *args, **kwargs) -> 'AgentDataFrame':
        """Apply agent (returns wrapped DataFrame)"""
        result_df = with_agent(self.df, *args, **kwargs)
        return AgentDataFrame(result_df)

    def map_with_agent(self, *args, **kwargs) -> 'AgentDataFrame':
        """Map agent (returns wrapped DataFrame)"""
        result_df = map_with_agent(self.df, *args, **kwargs)
        return AgentDataFrame(result_df)

    def filter_with_agent(self, *args, **kwargs) -> 'AgentDataFrame':
        """Filter with agent (returns wrapped DataFrame)"""
        result_df = filter_with_agent(self.df, *args, **kwargs)
        return AgentDataFrame(result_df)

    def transform_with_agent(self, *args, **kwargs) -> 'AgentDataFrame':
        """Transform with agent (returns wrapped DataFrame)"""
        result_df = transform_with_agent(self.df, *args, **kwargs)
        return AgentDataFrame(result_df)

    def unwrap(self) -> DataFrame:
        """Get underlying DataFrame"""
        return self.df

    def __getattr__(self, name):
        """Forward other calls to underlying DataFrame"""
        return getattr(self.df, name)

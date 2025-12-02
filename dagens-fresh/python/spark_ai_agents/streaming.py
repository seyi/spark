"""
Structured Streaming integration for agents.

This module provides utilities for processing streaming data with agents.
"""

from typing import Union, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from .agents import Agent
from .udf import batch_agent_udf


class AgentStreamProcessor:
    """
    Processor for Structured Streaming with agents.

    Example:
        from spark_ai_agents import AgentStreamProcessor, LlmAgent

        stream = spark.readStream.format("kafka").load()

        processor = AgentStreamProcessor(LlmAgent("sentiment-analyzer"))
        processed = processor.process(stream, input_col="value")

        query = processed.writeStream.format("console").start()
    """

    def __init__(self, agent: Union[Agent, str], batch_size: int = 32):
        """
        Initialize stream processor.

        Args:
            agent: Agent instance or agent ID string
            batch_size: Batch size for processing
        """
        if isinstance(agent, str):
            self.agent = Agent(agent)
        else:
            self.agent = agent

        self.batch_size = batch_size

    def process(self,
                stream: DataFrame,
                input_col: str = "value",
                output_col: str = "agent_output") -> DataFrame:
        """
        Process streaming DataFrame with agent.

        Args:
            stream: Streaming DataFrame
            input_col: Input column name
            output_col: Output column name

        Returns:
            Processed streaming DataFrame

        Example:
            processor = AgentStreamProcessor("summarizer")
            processed_stream = processor.process(stream, "text", "summary")
        """
        from .dataframe import with_agent

        return with_agent(
            stream,
            self.agent,
            input_col=input_col,
            output_col=output_col,
            batch_size=self.batch_size
        )

    def foreachBatch(self, func: Optional[Callable] = None):
        """
        Create foreachBatch function for streaming.

        Args:
            func: Optional custom processing function

        Returns:
            Function for use with writeStream.foreachBatch()

        Example:
            processor = AgentStreamProcessor("analyzer")

            stream.writeStream \
                .foreachBatch(processor.foreachBatch()) \
                .start()
        """
        def process_batch(batch_df: DataFrame, batch_id: int):
            """Process micro-batch"""
            if func is not None:
                # Apply custom function first
                batch_df = func(batch_df, batch_id)

            # Process with agent
            processed = self.process(batch_df)

            # Write results (you can customize this)
            processed.write.format("parquet").mode("append").save("/path/to/output")

        return process_batch


class StatefulAgentProcessor:
    """
    Stateful processor for maintaining conversation state across batches.

    This is useful for conversational agents that need to remember
    previous interactions.

    Example:
        processor = StatefulAgentProcessor("conversational-agent")
        stream.groupByKey(lambda row: row.user_id) \
              .mapGroupsWithState(processor.process_with_state)
    """

    def __init__(self, agent: Union[Agent, str]):
        """
        Initialize stateful processor.

        Args:
            agent: Agent instance or agent ID string
        """
        if isinstance(agent, str):
            self.agent = Agent(agent)
        else:
            self.agent = agent

    def process_with_state(self, key, values, state):
        """
        Process group with state.

        Args:
            key: Group key (e.g., user_id)
            values: Iterator of values in group
            state: GroupState for maintaining conversation

        Yields:
            Processed results

        Example:
            from pyspark.sql.streaming import GroupState, GroupStateTimeout

            stream.groupByKey(lambda msg: msg.user_id) \
                  .mapGroupsWithState(
                      processor.process_with_state,
                      state Type=ConversationState,
                      timeout=GroupStateTimeout.ProcessingTimeTimeout
                  )
        """
        # Get existing state
        if state.exists:
            conversation_history = state.get()
        else:
            conversation_history = []

        # Process new messages
        for value in values:
            # Execute agent with history context
            response = self.agent.execute(
                value.text,
                context={"history": str(conversation_history)}
            )

            # Update conversation history
            conversation_history.append({
                "user": value.text,
                "agent": response.output
            })

            # Yield result
            yield (key, response.output)

        # Update state
        state.update(conversation_history)


def process_stream_with_agent(stream: DataFrame,
                               agent: Union[Agent, str],
                               input_col: str = "value",
                               output_col: str = "agent_output",
                               batch_size: int = 32) -> DataFrame:
    """
    Process streaming DataFrame with agent (convenience function).

    Args:
        stream: Streaming DataFrame
        agent: Agent instance or agent ID string
        input_col: Input column name
        output_col: Output column name
        batch_size: Batch size for processing

    Returns:
        Processed streaming DataFrame

    Example:
        from spark_ai_agents import process_stream_with_agent

        stream = spark.readStream.format("kafka").load()

        processed = process_stream_with_agent(
            stream,
            "sentiment-analyzer",
            input_col="value",
            output_col="sentiment"
        )

        query = processed.writeStream.format("console").start()
    """
    processor = AgentStreamProcessor(agent, batch_size=batch_size)
    return processor.process(stream, input_col, output_col)


class StreamingAgentWriter:
    """
    Wrapper for DataStreamWriter that adds agent-specific options.

    Example:
        writer = StreamingAgentWriter(processed_stream)
        writer.to_kafka("output-topic").start()
    """

    def __init__(self, stream: DataFrame):
        """
        Initialize streaming writer.

        Args:
            stream: Streaming DataFrame
        """
        self.stream = stream

    def to_kafka(self, topic: str, bootstrap_servers: str = "localhost:9092"):
        """
        Write to Kafka topic.

        Args:
            topic: Kafka topic name
            bootstrap_servers: Kafka bootstrap servers

        Returns:
            DataStreamWriter
        """
        from pyspark.sql.functions import to_json, struct

        # Convert to Kafka format
        kafka_df = self.stream.select(
            to_json(struct(*self.stream.columns)).alias("value")
        )

        return kafka_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", topic)

    def to_delta(self, path: str, checkpoint_location: str = None):
        """
        Write to Delta Lake.

        Args:
            path: Delta table path
            checkpoint_location: Checkpoint location

        Returns:
            DataStreamWriter
        """
        writer = self.stream.writeStream \
            .format("delta") \
            .option("path", path)

        if checkpoint_location:
            writer = writer.option("checkpointLocation", checkpoint_location)

        return writer

    def to_console(self, truncate: bool = True, num_rows: int = 20):
        """
        Write to console (for debugging).

        Args:
            truncate: Truncate output
            num_rows: Number of rows to display

        Returns:
            DataStreamWriter
        """
        return self.stream.writeStream \
            .format("console") \
            .option("truncate", truncate) \
            .option("numRows", num_rows)

    def foreachBatch(self, func: Callable):
        """
        Process each batch with custom function.

        Args:
            func: Function to process each batch

        Returns:
            DataStreamWriter
        """
        return self.stream.writeStream.foreachBatch(func)

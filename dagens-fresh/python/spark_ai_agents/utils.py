"""
Utility functions and classes for Spark AI Agents.
"""

from typing import Iterator, Any, List
from .agents import Agent


class AgentMapper:
    """
    Maps agent over DataFrame partitions with batching.
    """

    def __init__(self, agent: Agent, batch_size: int = 32):
        """
        Initialize AgentMapper.

        Args:
            agent: Agent instance
            batch_size: Number of rows to batch together
        """
        self.agent = agent
        self.batch_size = batch_size

    def map_partition(self, partition: Iterator, input_col: str = "value") -> Iterator:
        """
        Map agent over partition with batching.

        Args:
            partition: Iterator over partition rows
            input_col: Column containing input text

        Yields:
            Tuples of (input, output, success)
        """
        batch = []

        for row in partition:
            # Extract input text
            if hasattr(row, input_col):
                input_text = getattr(row, input_col)
            elif isinstance(row, dict):
                input_text = row.get(input_col)
            else:
                input_text = str(row)

            batch.append(input_text)

            # Process batch when full
            if len(batch) >= self.batch_size:
                yield from self._process_batch(batch)
                batch = []

        # Process remaining
        if batch:
            yield from self._process_batch(batch)

    def _process_batch(self, inputs: List[str]) -> Iterator:
        """
        Process batch of inputs.

        Args:
            inputs: List of input texts

        Yields:
            Tuples of (input, output, success)
        """
        responses = self.agent.batch_execute(inputs)

        for inp, resp in zip(inputs, responses):
            yield (inp, resp.output, resp.success)


class PartitionProcessor:
    """
    Processes DataFrame partitions with custom logic.
    """

    def __init__(self, agent: Agent):
        """
        Initialize PartitionProcessor.

        Args:
            agent: Agent instance
        """
        self.agent = agent

    def process(self, partition: Iterator) -> Iterator:
        """
        Process partition.

        Args:
            partition: Iterator over partition rows

        Yields:
            Processed rows
        """
        for row in partition:
            # Process each row
            response = self.agent.execute(str(row))

            # Yield result
            yield {
                "input": str(row),
                "output": response.output,
                "success": response.success,
                "duration_ms": response.duration_ms
            }


def partition_agent_executor(agent_id: str, batch_size: int = 32):
    """
    Create a partition executor function for use with mapPartitions.

    Args:
        agent_id: ID of the agent to execute
        batch_size: Batch size for processing

    Returns:
        Function that can be used with DataFrame.rdd.mapPartitions()

    Example:
        from spark_ai_agents import partition_agent_executor

        executor = partition_agent_executor("summarizer", batch_size=32)
        results = df.rdd.mapPartitions(executor)
    """
    def executor(partition: Iterator) -> Iterator:
        """Execute agent on partition"""
        agent = Agent(agent_id)
        mapper = AgentMapper(agent, batch_size=batch_size)
        return mapper.map_partition(partition)

    return executor


def create_agent_pipeline(*agents):
    """
    Create a pipeline of agents for sequential processing.

    Args:
        *agents: Variable number of Agent instances or agent IDs

    Returns:
        Function that applies agents sequentially

    Example:
        from spark_ai_agents import create_agent_pipeline, Agent

        pipeline = create_agent_pipeline(
            Agent("extractor"),
            Agent("summarizer"),
            Agent("classifier")
        )

        df.rdd.map(pipeline)
    """
    # Convert agent IDs to Agent instances
    agent_instances = []
    for ag in agents:
        if isinstance(ag, str):
            agent_instances.append(Agent(ag))
        else:
            agent_instances.append(ag)

    def pipeline(text: str) -> str:
        """Apply agents sequentially"""
        result = text
        for agent in agent_instances:
            response = agent.execute(result)
            if response.success:
                result = response.output
            else:
                break
        return result

    return pipeline

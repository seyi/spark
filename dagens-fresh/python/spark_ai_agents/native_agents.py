"""
Native Agent implementation for direct execution on Spark executors.

This module provides agents that run directly on Spark executors without
requiring RPC to an external server, enabling true distributed AI processing.
"""

import os
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Union
import logging

from .native_providers import (
    NativeLLMProvider, LLMConfig, LLMResponse,
    create_provider, OpenAIProvider
)

logger = logging.getLogger(__name__)


@dataclass
class NativeAgentResponse:
    """Response from native agent execution"""
    output: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    error: Optional[str] = None
    duration_ms: int = 0
    tokens_used: int = 0

    def __repr__(self):
        status = "success" if self.success else "error"
        output_preview = self.output[:50] + "..." if len(self.output) > 50 else self.output
        return f"NativeAgentResponse({status}, output='{output_preview}')"


@dataclass
class NativeAgentConfig:
    """
    Configuration for native agents.

    This configuration is designed to be serializable for Spark broadcast.
    """
    # Agent identification
    name: str
    description: str = ""

    # LLM configuration
    provider_type: str = "openai"
    model: str = "gpt-3.5-turbo"
    temperature: float = 0.7
    max_tokens: int = 1024
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop_sequences: List[str] = field(default_factory=list)

    # Agent behavior
    system_prompt: str = ""
    instruction: str = ""
    output_format: Optional[str] = None  # 'json', 'text', etc.

    # Provider credentials (read from env if not set)
    api_key: Optional[str] = None
    api_base: Optional[str] = None

    # Extra provider-specific params
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "name": self.name,
            "description": self.description,
            "provider_type": self.provider_type,
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "top_p": self.top_p,
            "frequency_penalty": self.frequency_penalty,
            "presence_penalty": self.presence_penalty,
            "stop_sequences": self.stop_sequences,
            "system_prompt": self.system_prompt,
            "instruction": self.instruction,
            "output_format": self.output_format,
            "api_key": self.api_key,
            "api_base": self.api_base,
            "extra_params": self.extra_params,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NativeAgentConfig':
        """Create from dictionary"""
        return cls(
            name=data.get("name", "native_agent"),
            description=data.get("description", ""),
            provider_type=data.get("provider_type", "openai"),
            model=data.get("model", "gpt-3.5-turbo"),
            temperature=data.get("temperature", 0.7),
            max_tokens=data.get("max_tokens", 1024),
            top_p=data.get("top_p", 1.0),
            frequency_penalty=data.get("frequency_penalty", 0.0),
            presence_penalty=data.get("presence_penalty", 0.0),
            stop_sequences=data.get("stop_sequences", []),
            system_prompt=data.get("system_prompt", ""),
            instruction=data.get("instruction", ""),
            output_format=data.get("output_format"),
            api_key=data.get("api_key"),
            api_base=data.get("api_base"),
            extra_params=data.get("extra_params", {}),
        )

    def to_llm_config(self) -> LLMConfig:
        """Convert to LLMConfig for provider"""
        return LLMConfig(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            top_p=self.top_p,
            frequency_penalty=self.frequency_penalty,
            presence_penalty=self.presence_penalty,
            stop_sequences=self.stop_sequences,
            api_key=self.api_key,
            api_base=self.api_base,
            extra_params=self.extra_params,
        )


class NativeAgent:
    """
    Native agent that runs directly on Spark executors.

    This agent executes LLM calls directly without RPC, providing
    significantly better performance for distributed processing.

    Example:
        # Create agent
        agent = NativeAgent(
            NativeAgentConfig(
                name="summarizer",
                provider_type="openai",
                model="gpt-3.5-turbo",
                system_prompt="You are a helpful assistant that summarizes text."
            )
        )

        # Execute
        response = agent.execute("Long text to summarize...")
        print(response.output)
    """

    # Class-level provider cache for reuse across calls
    _provider_cache: Dict[str, NativeLLMProvider] = {}

    def __init__(self, config: NativeAgentConfig):
        """
        Initialize native agent.

        Args:
            config: Agent configuration
        """
        self.config = config
        self._provider: Optional[NativeLLMProvider] = None

    @property
    def name(self) -> str:
        """Get agent name"""
        return self.config.name

    @property
    def provider(self) -> NativeLLMProvider:
        """
        Get or create LLM provider (lazy initialization).

        Uses class-level cache to reuse providers across agents with
        same configuration.
        """
        if self._provider is not None:
            return self._provider

        # Create cache key from provider config
        cache_key = f"{self.config.provider_type}:{self.config.api_key}:{self.config.api_base}"

        if cache_key not in self._provider_cache:
            provider_kwargs = {}
            if self.config.api_key:
                provider_kwargs["api_key"] = self.config.api_key
            if self.config.api_base:
                provider_kwargs["api_base"] = self.config.api_base

            self._provider_cache[cache_key] = create_provider(
                self.config.provider_type,
                **provider_kwargs
            )

        self._provider = self._provider_cache[cache_key]
        return self._provider

    def execute(self, input_text: str,
                context: Optional[Dict[str, Any]] = None) -> NativeAgentResponse:
        """
        Execute agent with given input.

        Args:
            input_text: Input text for the agent
            context: Additional context (optional)

        Returns:
            NativeAgentResponse with agent output
        """
        start_time = time.time()

        try:
            # Build system prompt
            system_prompt = self._build_system_prompt(context)

            # Build user prompt
            user_prompt = self._build_user_prompt(input_text, context)

            # Execute LLM call
            llm_response = self.provider.generate(
                prompt=user_prompt,
                system_prompt=system_prompt,
                config=self.config.to_llm_config()
            )

            duration_ms = int((time.time() - start_time) * 1000)

            if not llm_response.success:
                return NativeAgentResponse(
                    output="",
                    success=False,
                    error=llm_response.error,
                    duration_ms=duration_ms
                )

            # Post-process output if needed
            output = self._post_process_output(llm_response.text)

            return NativeAgentResponse(
                output=output,
                success=True,
                duration_ms=duration_ms,
                tokens_used=llm_response.tokens_used,
                metadata={
                    **llm_response.metadata,
                    "agent_name": self.name,
                    "provider": self.config.provider_type,
                    "model": self.config.model,
                }
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Agent execution failed: {e}")
            return NativeAgentResponse(
                output="",
                success=False,
                error=str(e),
                duration_ms=duration_ms
            )

    def batch_execute(self, inputs: List[str],
                      context: Optional[Dict[str, Any]] = None) -> List[NativeAgentResponse]:
        """
        Execute agent on batch of inputs.

        Args:
            inputs: List of input texts
            context: Shared context for all inputs

        Returns:
            List of NativeAgentResponse objects
        """
        # Build system prompt once for batch
        system_prompt = self._build_system_prompt(context)

        # Build user prompts
        user_prompts = [self._build_user_prompt(inp, context) for inp in inputs]

        # Execute batch
        llm_responses = self.provider.batch_generate(
            prompts=user_prompts,
            system_prompt=system_prompt,
            config=self.config.to_llm_config()
        )

        # Convert to agent responses
        results = []
        for llm_response in llm_responses:
            if llm_response.success:
                output = self._post_process_output(llm_response.text)
                results.append(NativeAgentResponse(
                    output=output,
                    success=True,
                    duration_ms=llm_response.duration_ms,
                    tokens_used=llm_response.tokens_used,
                    metadata={
                        **llm_response.metadata,
                        "agent_name": self.name,
                    }
                ))
            else:
                results.append(NativeAgentResponse(
                    output="",
                    success=False,
                    error=llm_response.error,
                    duration_ms=llm_response.duration_ms
                ))

        return results

    def _build_system_prompt(self, context: Optional[Dict[str, Any]] = None) -> str:
        """Build system prompt from config and context"""
        parts = []

        if self.config.system_prompt:
            parts.append(self.config.system_prompt)

        if self.config.output_format == "json":
            parts.append("Always respond with valid JSON.")

        # Add context to system prompt if provided
        if context and "system_context" in context:
            parts.append(str(context["system_context"]))

        return "\n\n".join(parts) if parts else None

    def _build_user_prompt(self, input_text: str,
                          context: Optional[Dict[str, Any]] = None) -> str:
        """Build user prompt from input and config"""
        parts = []

        if self.config.instruction:
            parts.append(self.config.instruction)

        if context and "instruction" in context:
            parts.append(str(context["instruction"]))

        parts.append(input_text)

        return "\n\n".join(parts)

    def _post_process_output(self, output: str) -> str:
        """Post-process LLM output"""
        output = output.strip()

        # Parse JSON if expected
        if self.config.output_format == "json":
            try:
                # Validate JSON
                json.loads(output)
            except json.JSONDecodeError:
                # Try to extract JSON from response
                if "{" in output and "}" in output:
                    start = output.index("{")
                    end = output.rindex("}") + 1
                    output = output[start:end]

        return output

    def __call__(self, input_text: str) -> str:
        """
        Execute agent (simplified interface).

        Args:
            input_text: Input text

        Returns:
            Output text (empty string on error)
        """
        response = self.execute(input_text)
        return response.output if response.success else ""

    def __repr__(self):
        return f"NativeAgent(name='{self.name}', provider='{self.config.provider_type}')"


class NativeSequentialAgent:
    """
    Sequential agent that chains multiple native agents.

    Executes agents in sequence, passing output to next input.
    """

    def __init__(self, name: str, agents: List[NativeAgent]):
        """
        Initialize sequential agent.

        Args:
            name: Agent name
            agents: List of agents to execute in sequence
        """
        self.name = name
        self.agents = agents

    def execute(self, input_text: str,
                context: Optional[Dict[str, Any]] = None) -> NativeAgentResponse:
        """
        Execute agents in sequence.

        Args:
            input_text: Initial input
            context: Shared context

        Returns:
            Final agent response
        """
        start_time = time.time()
        current_input = input_text
        all_metadata = {"agent_name": self.name, "steps": []}

        for i, agent in enumerate(self.agents):
            response = agent.execute(current_input, context)

            all_metadata["steps"].append({
                "step": i + 1,
                "agent": agent.name,
                "success": response.success,
                "duration_ms": response.duration_ms,
            })

            if not response.success:
                return NativeAgentResponse(
                    output="",
                    success=False,
                    error=f"Step {i + 1} ({agent.name}) failed: {response.error}",
                    duration_ms=int((time.time() - start_time) * 1000),
                    metadata=all_metadata
                )

            current_input = response.output

        return NativeAgentResponse(
            output=current_input,
            success=True,
            duration_ms=int((time.time() - start_time) * 1000),
            metadata=all_metadata
        )

    def __call__(self, input_text: str) -> str:
        response = self.execute(input_text)
        return response.output if response.success else ""


class NativeParallelAgent:
    """
    Parallel agent that executes multiple agents concurrently.

    Uses ThreadPoolExecutor for parallel execution on a single executor.
    """

    def __init__(self, name: str, agents: List[NativeAgent],
                 combiner: Optional[Callable[[List[str]], str]] = None,
                 max_workers: int = 4):
        """
        Initialize parallel agent.

        Args:
            name: Agent name
            agents: List of agents to execute in parallel
            combiner: Function to combine outputs (default: newline join)
            max_workers: Maximum concurrent workers
        """
        self.name = name
        self.agents = agents
        self.combiner = combiner or (lambda outputs: "\n---\n".join(outputs))
        self.max_workers = max_workers

    def execute(self, input_text: str,
                context: Optional[Dict[str, Any]] = None) -> NativeAgentResponse:
        """
        Execute agents in parallel.

        Args:
            input_text: Input for all agents
            context: Shared context

        Returns:
            Combined agent response
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        start_time = time.time()
        all_metadata = {"agent_name": self.name, "parallel_results": []}

        outputs = []
        errors = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(agent.execute, input_text, context): agent
                for agent in self.agents
            }

            for future in as_completed(futures):
                agent = futures[future]
                try:
                    response = future.result()
                    all_metadata["parallel_results"].append({
                        "agent": agent.name,
                        "success": response.success,
                        "duration_ms": response.duration_ms,
                    })

                    if response.success:
                        outputs.append(response.output)
                    else:
                        errors.append(f"{agent.name}: {response.error}")

                except Exception as e:
                    errors.append(f"{agent.name}: {str(e)}")

        duration_ms = int((time.time() - start_time) * 1000)

        if errors:
            # Partial success - return what we have with error info
            return NativeAgentResponse(
                output=self.combiner(outputs) if outputs else "",
                success=len(outputs) > 0,
                error="; ".join(errors) if len(outputs) == 0 else None,
                duration_ms=duration_ms,
                metadata=all_metadata
            )

        return NativeAgentResponse(
            output=self.combiner(outputs),
            success=True,
            duration_ms=duration_ms,
            metadata=all_metadata
        )

    def __call__(self, input_text: str) -> str:
        response = self.execute(input_text)
        return response.output if response.success else ""


# Convenience factory functions

def create_native_agent(
    name: str,
    provider_type: str = "openai",
    model: str = "gpt-3.5-turbo",
    system_prompt: str = "",
    instruction: str = "",
    temperature: float = 0.7,
    max_tokens: int = 1024,
    **kwargs
) -> NativeAgent:
    """
    Factory function to create native agents.

    Args:
        name: Agent name
        provider_type: LLM provider ('openai', 'anthropic', 'azure', etc.)
        model: Model name
        system_prompt: System prompt
        instruction: Instruction prefix for user prompts
        temperature: Temperature for generation
        max_tokens: Maximum tokens in response
        **kwargs: Additional config parameters

    Returns:
        NativeAgent instance

    Example:
        agent = create_native_agent(
            "summarizer",
            provider_type="openai",
            model="gpt-4",
            system_prompt="Summarize the following text concisely."
        )
    """
    config = NativeAgentConfig(
        name=name,
        provider_type=provider_type,
        model=model,
        system_prompt=system_prompt,
        instruction=instruction,
        temperature=temperature,
        max_tokens=max_tokens,
        **kwargs
    )
    return NativeAgent(config)


def create_native_sequential_agent(
    name: str,
    agents: List[Union[NativeAgent, Dict[str, Any]]]
) -> NativeSequentialAgent:
    """
    Factory function to create sequential native agents.

    Args:
        name: Agent name
        agents: List of NativeAgent instances or config dicts

    Returns:
        NativeSequentialAgent instance

    Example:
        agent = create_native_sequential_agent("pipeline", [
            create_native_agent("extractor", system_prompt="Extract key facts."),
            create_native_agent("summarizer", system_prompt="Summarize facts.")
        ])
    """
    native_agents = []
    for i, agent in enumerate(agents):
        if isinstance(agent, NativeAgent):
            native_agents.append(agent)
        elif isinstance(agent, dict):
            # Make a copy to avoid modifying the original
            agent_dict = dict(agent)
            agent_name = agent_dict.pop("name", f"step_{i + 1}")
            native_agents.append(create_native_agent(agent_name, **agent_dict))
        else:
            raise ValueError(f"Invalid agent specification: {agent}")

    return NativeSequentialAgent(name, native_agents)


def create_native_parallel_agent(
    name: str,
    agents: List[Union[NativeAgent, Dict[str, Any]]],
    combiner: Optional[Callable[[List[str]], str]] = None
) -> NativeParallelAgent:
    """
    Factory function to create parallel native agents.

    Args:
        name: Agent name
        agents: List of NativeAgent instances or config dicts
        combiner: Function to combine outputs

    Returns:
        NativeParallelAgent instance
    """
    native_agents = []
    for i, agent in enumerate(agents):
        if isinstance(agent, NativeAgent):
            native_agents.append(agent)
        elif isinstance(agent, dict):
            # Make a copy to avoid modifying the original
            agent_dict = dict(agent)
            agent_name = agent_dict.pop("name", f"parallel_{i + 1}")
            native_agents.append(create_native_agent(agent_name, **agent_dict))
        else:
            raise ValueError(f"Invalid agent specification: {agent}")

    return NativeParallelAgent(name, native_agents, combiner)

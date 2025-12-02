"""
Native LLM Provider abstraction for direct API calls on Spark executors.

This module provides a lightweight LLM provider interface that runs directly
on Spark executors without requiring RPC to an external server.
"""

import os
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Iterator
import logging

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Response from LLM provider"""
    text: str
    tokens_used: int = 0
    finish_reason: str = "stop"
    metadata: Dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    success: bool = True
    error: Optional[str] = None


@dataclass
class LLMConfig:
    """Configuration for LLM providers"""
    model: str
    temperature: float = 0.7
    max_tokens: int = 1024
    top_p: float = 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop_sequences: List[str] = field(default_factory=list)
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "top_p": self.top_p,
            "frequency_penalty": self.frequency_penalty,
            "presence_penalty": self.presence_penalty,
            "stop_sequences": self.stop_sequences,
            "api_key": self.api_key,
            "api_base": self.api_base,
            "extra_params": self.extra_params,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LLMConfig':
        """Create from dictionary"""
        return cls(
            model=data.get("model", "gpt-3.5-turbo"),
            temperature=data.get("temperature", 0.7),
            max_tokens=data.get("max_tokens", 1024),
            top_p=data.get("top_p", 1.0),
            frequency_penalty=data.get("frequency_penalty", 0.0),
            presence_penalty=data.get("presence_penalty", 0.0),
            stop_sequences=data.get("stop_sequences", []),
            api_key=data.get("api_key"),
            api_base=data.get("api_base"),
            extra_params=data.get("extra_params", {}),
        )


class NativeLLMProvider(ABC):
    """
    Abstract base class for native LLM providers.

    These providers run directly on Spark executors without requiring
    RPC to an external server.
    """

    @abstractmethod
    def name(self) -> str:
        """Return provider name"""
        pass

    @abstractmethod
    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """
        Generate text from prompt.

        Args:
            prompt: User prompt/input
            system_prompt: System prompt for context
            config: LLM configuration

        Returns:
            LLMResponse with generated text
        """
        pass

    def batch_generate(self, prompts: List[str], system_prompt: Optional[str] = None,
                       config: Optional[LLMConfig] = None) -> List[LLMResponse]:
        """
        Generate text for batch of prompts.

        Default implementation processes sequentially. Override for
        providers that support batch APIs.

        Args:
            prompts: List of user prompts
            system_prompt: Shared system prompt
            config: LLM configuration

        Returns:
            List of LLMResponse objects
        """
        return [self.generate(p, system_prompt, config) for p in prompts]


class OpenAIProvider(NativeLLMProvider):
    """
    OpenAI API provider for native executor execution.

    Supports GPT-3.5-turbo, GPT-4, and other OpenAI models.
    """

    def __init__(self, api_key: Optional[str] = None, api_base: Optional[str] = None):
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            api_base: Optional custom API base URL
        """
        self._api_key = api_key or os.getenv("OPENAI_API_KEY")
        self._api_base = api_base or os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
        self._client = None

    def name(self) -> str:
        return "openai"

    def _get_client(self):
        """Lazy initialize OpenAI client"""
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    api_key=self._api_key,
                    base_url=self._api_base
                )
            except ImportError:
                raise ImportError(
                    "openai library required for OpenAIProvider. "
                    "Install with: pip install openai"
                )
        return self._client

    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """Generate using OpenAI API"""
        start_time = time.time()
        config = config or LLMConfig(model="gpt-3.5-turbo")

        try:
            client = self._get_client()

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            response = client.chat.completions.create(
                model=config.model,
                messages=messages,
                temperature=config.temperature,
                max_tokens=config.max_tokens,
                top_p=config.top_p,
                frequency_penalty=config.frequency_penalty,
                presence_penalty=config.presence_penalty,
                stop=config.stop_sequences or None,
                **config.extra_params
            )

            duration_ms = int((time.time() - start_time) * 1000)
            choice = response.choices[0]

            return LLMResponse(
                text=choice.message.content or "",
                tokens_used=response.usage.total_tokens if response.usage else 0,
                finish_reason=choice.finish_reason or "stop",
                duration_ms=duration_ms,
                metadata={
                    "model": response.model,
                    "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
                    "completion_tokens": response.usage.completion_tokens if response.usage else 0,
                }
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return LLMResponse(
                text="",
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )


class AnthropicProvider(NativeLLMProvider):
    """
    Anthropic API provider for native executor execution.

    Supports Claude models.
    """

    def __init__(self, api_key: Optional[str] = None, api_base: Optional[str] = None):
        """
        Initialize Anthropic provider.

        Args:
            api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
            api_base: Optional custom API base URL
        """
        self._api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self._api_base = api_base
        self._client = None

    def name(self) -> str:
        return "anthropic"

    def _get_client(self):
        """Lazy initialize Anthropic client"""
        if self._client is None:
            try:
                import anthropic
                kwargs = {"api_key": self._api_key}
                if self._api_base:
                    kwargs["base_url"] = self._api_base
                self._client = anthropic.Anthropic(**kwargs)
            except ImportError:
                raise ImportError(
                    "anthropic library required for AnthropicProvider. "
                    "Install with: pip install anthropic"
                )
        return self._client

    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """Generate using Anthropic API"""
        start_time = time.time()
        config = config or LLMConfig(model="claude-3-sonnet-20240229")

        try:
            client = self._get_client()

            kwargs = {
                "model": config.model,
                "max_tokens": config.max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            }

            if system_prompt:
                kwargs["system"] = system_prompt

            if config.temperature != 0.7:  # Non-default
                kwargs["temperature"] = config.temperature

            if config.stop_sequences:
                kwargs["stop_sequences"] = config.stop_sequences

            response = client.messages.create(**kwargs)

            duration_ms = int((time.time() - start_time) * 1000)

            return LLMResponse(
                text=response.content[0].text if response.content else "",
                tokens_used=response.usage.input_tokens + response.usage.output_tokens,
                finish_reason=response.stop_reason or "stop",
                duration_ms=duration_ms,
                metadata={
                    "model": response.model,
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens,
                }
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return LLMResponse(
                text="",
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )


class AzureOpenAIProvider(NativeLLMProvider):
    """
    Azure OpenAI API provider for native executor execution.
    """

    def __init__(self, api_key: Optional[str] = None,
                 api_base: Optional[str] = None,
                 api_version: Optional[str] = None,
                 deployment_name: Optional[str] = None):
        """
        Initialize Azure OpenAI provider.

        Args:
            api_key: Azure OpenAI API key
            api_base: Azure OpenAI endpoint
            api_version: API version
            deployment_name: Deployment name
        """
        self._api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY")
        self._api_base = api_base or os.getenv("AZURE_OPENAI_ENDPOINT")
        self._api_version = api_version or os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        self._deployment_name = deployment_name or os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self._client = None

    def name(self) -> str:
        return "azure_openai"

    def _get_client(self):
        """Lazy initialize Azure OpenAI client"""
        if self._client is None:
            try:
                from openai import AzureOpenAI
                self._client = AzureOpenAI(
                    api_key=self._api_key,
                    api_version=self._api_version,
                    azure_endpoint=self._api_base
                )
            except ImportError:
                raise ImportError(
                    "openai library required for AzureOpenAIProvider. "
                    "Install with: pip install openai"
                )
        return self._client

    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """Generate using Azure OpenAI API"""
        start_time = time.time()
        config = config or LLMConfig(model=self._deployment_name or "gpt-35-turbo")

        try:
            client = self._get_client()

            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            response = client.chat.completions.create(
                model=config.model,
                messages=messages,
                temperature=config.temperature,
                max_tokens=config.max_tokens,
                top_p=config.top_p,
                frequency_penalty=config.frequency_penalty,
                presence_penalty=config.presence_penalty,
                stop=config.stop_sequences or None,
            )

            duration_ms = int((time.time() - start_time) * 1000)
            choice = response.choices[0]

            return LLMResponse(
                text=choice.message.content or "",
                tokens_used=response.usage.total_tokens if response.usage else 0,
                finish_reason=choice.finish_reason or "stop",
                duration_ms=duration_ms,
                metadata={
                    "model": response.model,
                    "prompt_tokens": response.usage.prompt_tokens if response.usage else 0,
                    "completion_tokens": response.usage.completion_tokens if response.usage else 0,
                }
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return LLMResponse(
                text="",
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )


class GoogleVertexProvider(NativeLLMProvider):
    """
    Google Vertex AI provider for native executor execution.

    Supports Gemini and PaLM models.
    """

    def __init__(self, project_id: Optional[str] = None,
                 location: Optional[str] = None):
        """
        Initialize Google Vertex AI provider.

        Args:
            project_id: GCP project ID
            location: GCP region (defaults to us-central1)
        """
        self._project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self._location = location or os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
        self._model = None

    def name(self) -> str:
        return "vertex_ai"

    def _get_model(self, model_name: str):
        """Lazy initialize Vertex AI model"""
        try:
            import vertexai
            from vertexai.generative_models import GenerativeModel

            vertexai.init(project=self._project_id, location=self._location)
            return GenerativeModel(model_name)
        except ImportError:
            raise ImportError(
                "google-cloud-aiplatform library required for GoogleVertexProvider. "
                "Install with: pip install google-cloud-aiplatform"
            )

    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """Generate using Vertex AI API"""
        start_time = time.time()
        config = config or LLMConfig(model="gemini-1.5-flash")

        try:
            model = self._get_model(config.model)

            full_prompt = prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"

            generation_config = {
                "temperature": config.temperature,
                "max_output_tokens": config.max_tokens,
                "top_p": config.top_p,
            }

            if config.stop_sequences:
                generation_config["stop_sequences"] = config.stop_sequences

            response = model.generate_content(
                full_prompt,
                generation_config=generation_config
            )

            duration_ms = int((time.time() - start_time) * 1000)

            return LLMResponse(
                text=response.text if response.text else "",
                tokens_used=0,  # Vertex AI doesn't always return token count
                finish_reason="stop",
                duration_ms=duration_ms,
                metadata={"model": config.model}
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            return LLMResponse(
                text="",
                duration_ms=duration_ms,
                success=False,
                error=str(e)
            )


class MockProvider(NativeLLMProvider):
    """
    Mock provider for testing without making actual API calls.
    """

    def __init__(self, responses: Optional[List[str]] = None,
                 api_key: Optional[str] = None,
                 api_base: Optional[str] = None):
        """
        Initialize mock provider.

        Args:
            responses: List of responses to return (cycles through)
            api_key: Ignored (for compatibility with factory)
            api_base: Ignored (for compatibility with factory)
        """
        self._responses = responses or ["Mock response"]
        self._call_count = 0
        # api_key and api_base are ignored but accepted for compatibility

    def name(self) -> str:
        return "mock"

    def generate(self, prompt: str, system_prompt: Optional[str] = None,
                 config: Optional[LLMConfig] = None) -> LLMResponse:
        """Return mock response"""
        response = self._responses[self._call_count % len(self._responses)]
        self._call_count += 1

        return LLMResponse(
            text=response,
            tokens_used=10,
            finish_reason="stop",
            duration_ms=1,
            metadata={"call_count": self._call_count}
        )


# Provider registry for easy creation
_PROVIDER_REGISTRY: Dict[str, type] = {
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "azure_openai": AzureOpenAIProvider,
    "azure": AzureOpenAIProvider,
    "vertex_ai": GoogleVertexProvider,
    "google": GoogleVertexProvider,
    "mock": MockProvider,
}


def create_provider(provider_type: str, **kwargs) -> NativeLLMProvider:
    """
    Factory function to create LLM providers.

    Args:
        provider_type: Type of provider ('openai', 'anthropic', 'azure', 'vertex_ai', 'mock')
        **kwargs: Provider-specific arguments

    Returns:
        NativeLLMProvider instance

    Example:
        provider = create_provider("openai", api_key="sk-...")
        provider = create_provider("anthropic")
        provider = create_provider("mock", responses=["Test response"])
    """
    provider_class = _PROVIDER_REGISTRY.get(provider_type.lower())
    if provider_class is None:
        raise ValueError(f"Unknown provider type: {provider_type}. "
                        f"Available: {list(_PROVIDER_REGISTRY.keys())}")
    return provider_class(**kwargs)


def register_provider(name: str, provider_class: type):
    """
    Register a custom provider class.

    Args:
        name: Provider name for the registry
        provider_class: Provider class (must inherit from NativeLLMProvider)
    """
    if not issubclass(provider_class, NativeLLMProvider):
        raise ValueError("Provider class must inherit from NativeLLMProvider")
    _PROVIDER_REGISTRY[name.lower()] = provider_class

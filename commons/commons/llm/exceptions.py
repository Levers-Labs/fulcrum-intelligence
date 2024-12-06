from typing import Any


class LLMError(Exception):
    """Base class for LLM-related errors."""

    pass


class LLMProviderError(LLMError):
    """Error from LLM provider."""

    def __init__(self, message: str, provider: str, details: Any = None):
        super().__init__(message)
        self.provider = provider
        self.details = details


class LLMParsingError(LLMError):
    """Error parsing LLM response."""

    pass


class LLMValidationError(LLMError):
    """Error validating LLM input/output."""

    pass


class LLMPromptNotFoundError(LLMError):
    """Error when prompt is not found in registry."""

    def __init__(self, prompt_name: str):
        super().__init__(f"Prompt not found in registry: {prompt_name}")
        self.prompt_name = prompt_name

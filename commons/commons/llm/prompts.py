from typing import Any, Generic, TypeVar

from jinja2 import Template
from pydantic import BaseModel

from commons.llm.exceptions import LLMPromptNotFoundError

T = TypeVar("T", bound=BaseModel)


class BasePrompt(BaseModel, Generic[T]):
    template: str
    output_schema: type[T] | None = None
    system_message: str | None = None

    def format(self, **kwargs: Any) -> str:
        """Format the prompt template with given variables."""
        template = Template(self.template)
        return template.render(**kwargs)

    def get_messages(self, **kwargs: Any) -> list[dict[str, str]]:
        """Get formatted messages for the LLM."""
        messages = []
        if self.system_message:
            messages.append({"role": "system", "content": self.system_message})
        messages.append({"role": "user", "content": self.format(**kwargs)})
        return messages


class PromptRegistry:
    """Registry for storing and retrieving prompts."""

    _prompts: dict[str, BasePrompt] = {}

    @classmethod
    def register(cls, name: str, prompt: BasePrompt) -> None:
        cls._prompts[name] = prompt

    @classmethod
    def get(cls, name: str) -> BasePrompt:
        if name not in cls._prompts:
            raise LLMPromptNotFoundError(f"Prompt '{name}' not found in registry")
        return cls._prompts[name]

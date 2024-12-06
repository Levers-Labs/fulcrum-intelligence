from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

from commons.llm.constants import LLMProviderType, OpenAIModels
from commons.llm.exceptions import LLMValidationError
from commons.llm.middleware import log_llm_request
from commons.llm.settings import LLMSettings
from commons.utilities.retries import retry


class LLMResponse(BaseModel):
    """Standardized response format."""

    content: str
    model: str
    usage: dict[str, Any]
    raw_response: Any | None = None


class LLMProvider:
    """Unified LLM provider using LangChain models."""

    def __init__(
        self,
        model: BaseChatModel | None = None,
        model_name: str = OpenAIModels.GPT4,
        api_key: str | None = None,
        temperature: float = 0,
    ):
        self.model = model or ChatOpenAI(openai_api_key=api_key, model_name=model_name, temperature=temperature)  # type: ignore  # noqa

    @log_llm_request
    @retry(retries=3, delay=1.0)
    async def generate(self, messages: list[BaseMessage], **kwargs: Any) -> LLMResponse:
        """Generate completion using LangChain with our custom monitoring."""
        result = await self.model.agenerate([messages], **kwargs)
        generation = result.generations[0][0]

        model_name = self.model.model_name if hasattr(self.model, "model_name") else self.model.model  # type: ignore

        return LLMResponse(
            content=generation.text, model=model_name, usage=result.llm_output or {}, raw_response=result
        )

    @classmethod
    def from_settings(cls, settings: LLMSettings) -> "LLMProvider":
        """Create provider from settings."""
        match settings.provider:
            case LLMProviderType.OPENAI:
                if not settings.openai_api_key:
                    raise LLMValidationError("OpenAI API key not configured")
                return cls(api_key=settings.openai_api_key, model_name=settings.default_model)

            case LLMProviderType.ANTHROPIC:
                from langchain_anthropic import ChatAnthropic  # noqa: E402

                if not settings.anthropic_api_key:
                    raise LLMValidationError("Anthropic API key not configured")
                model = ChatAnthropic(anthropic_api_key=settings.anthropic_api_key, model_name=settings.default_model)  # type: ignore
                return cls(model=model)

            case LLMProviderType.AZURE_OPENAI:
                from langchain_openai import AzureChatOpenAI  # noqa: E402

                if not settings.azure_openai_api_key or not settings.azure_openai_endpoint:
                    raise LLMValidationError("Azure OpenAI credentials not configured")
                model = AzureChatOpenAI(  # type: ignore
                    azure_api_key=settings.azure_openai_api_key,
                    azure_endpoint=settings.azure_openai_endpoint,
                    deployment_name=settings.default_model,
                )
                return cls(model=model)

            case _:
                raise LLMValidationError(f"Unsupported LLM provider: {settings.provider}")

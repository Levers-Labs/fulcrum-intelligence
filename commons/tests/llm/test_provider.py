from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import HumanMessage
from pydantic import ValidationError

from commons.llm.constants import LLMProviderType, OpenAIModels
from commons.llm.exceptions import LLMValidationError
from commons.llm.provider import LLMProvider, LLMResponse
from commons.llm.settings import LLMSettings


@pytest.mark.asyncio
async def test_llm_provider_generate(mock_llm_provider: LLMProvider):
    """Test basic generation functionality."""
    messages = [HumanMessage(content="test prompt")]
    response = await mock_llm_provider.generate(messages)

    assert isinstance(response, LLMResponse)
    assert response.content == "mock response"
    assert response.model == OpenAIModels.GPT4
    assert response.usage == {"token_usage": {"total_tokens": 100}}


@pytest.mark.asyncio
async def test_llm_provider_retry(mock_chat_result):
    """Test retry mechanism."""
    model = AsyncMock()
    model.agenerate.side_effect = [
        Exception("Test error"),
        Exception("Test error"),
        AsyncMock(return_value=mock_chat_result),
    ]

    provider = LLMProvider(model=model)
    messages = [HumanMessage(content="test")]

    with pytest.raises(ValidationError):
        await provider.generate(messages)

    assert model.agenerate.call_count == 3


@pytest.mark.parametrize(
    "provider_type,missing_key",
    [
        (LLMProviderType.OPENAI, "OpenAI API key"),
        (LLMProviderType.ANTHROPIC, "Anthropic API key"),
        (LLMProviderType.AZURE_OPENAI, "Azure OpenAI credentials"),
    ],
)
def test_provider_missing_credentials(provider_type: str, missing_key: str):
    """Test error handling for missing credentials."""
    settings = LLMSettings(provider=provider_type)
    with pytest.raises(LLMValidationError, match=f".*{missing_key}.*"):
        LLMProvider.from_settings(settings)


def test_provider_from_settings_openai():
    """Test creating OpenAI provider from settings."""
    settings = LLMSettings(provider=LLMProviderType.OPENAI, openai_api_key="test-key", model=OpenAIModels.GPT4)
    provider = LLMProvider.from_settings(settings)
    assert isinstance(provider, LLMProvider)
    assert provider.model.model_name == OpenAIModels.GPT4


def test_provider_from_settings_missing_key():
    """Test error when API key is missing."""
    settings = LLMSettings(provider=LLMProviderType.OPENAI, openai_api_key=None)
    with pytest.raises(LLMValidationError, match="OpenAI API key not configured"):
        LLMProvider.from_settings(settings)


def test_provider_from_settings_invalid():
    """Test error with invalid provider."""
    with pytest.raises(ValidationError, match="1 validation error for LLMSettings"):
        settings = LLMSettings(provider="invalid")  # type: ignore  # noqa
        LLMProvider.from_settings(settings)

from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult, LLMResult

from commons.llm.constants import OpenAIModels
from commons.llm.provider import LLMProvider


@pytest.fixture
def mock_chat_result() -> LLMResult:
    generation = ChatGeneration(
        text="mock response", message=HumanMessage(content="mock response"), generation_info={"finish_reason": "stop"}
    )
    return LLMResult(generations=[[generation]], llm_output={"token_usage": {"total_tokens": 100}})


@pytest.fixture
def mock_llm_model(mock_chat_result: ChatResult) -> AsyncMock:
    model = AsyncMock()
    model.agenerate.return_value = mock_chat_result
    model.model_name = OpenAIModels.GPT4
    return model


@pytest.fixture
def mock_llm_provider(mock_llm_model: AsyncMock) -> LLMProvider:
    return LLMProvider(model=mock_llm_model)

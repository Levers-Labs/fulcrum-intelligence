import pytest
from pydantic import BaseModel

from commons.llm.prompts import BasePrompt, PromptRegistry
from commons.llm.service import LLMService


class TestInputModel(BaseModel):
    text: str


class TestOutputModel(BaseModel):
    result: str


TEST_PROMPT = BasePrompt(
    template="Process this: {{ input.text }}", system_message="You are a test processor", output_schema=TestOutputModel
)


class TestLLMService(LLMService[TestInputModel, TestOutputModel]):
    async def validate_input(self, input_data: TestInputModel) -> None:
        if not input_data.text:
            raise ValueError("Empty input")


@pytest.fixture(autouse=True)
def setup_test_prompt():
    PromptRegistry.register("test_prompt", TEST_PROMPT)
    yield
    PromptRegistry._prompts.clear()


@pytest.mark.asyncio
async def test_llm_service_process(mock_llm_provider):
    """Test service processing flow."""
    service = TestLLMService(provider=mock_llm_provider, prompt_name="test_prompt", output_model=TestOutputModel)

    # Test success case
    mock_llm_provider.model.agenerate.return_value.generations[0][0].text = '{"result": "processed test input"}'
    result = await service.process(TestInputModel(text="test input"))
    assert isinstance(result, TestOutputModel)


@pytest.mark.asyncio
async def test_llm_service_validation(mock_llm_provider):
    """Test input validation."""
    service = TestLLMService(provider=mock_llm_provider, prompt_name="test_prompt", output_model=TestOutputModel)

    with pytest.raises(ValueError, match="Empty input"):
        await service.process(TestInputModel(text=""))

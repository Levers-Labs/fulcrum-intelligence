import pytest
from pydantic import BaseModel

from commons.llm.exceptions import LLMPromptNotFoundError
from commons.llm.prompts import BasePrompt, PromptRegistry


class TestSchema(BaseModel):
    value: str


def test_prompt_format():
    """Test prompt formatting."""
    prompt = BasePrompt(template="Hello {{ name }}!", system_message="Test system")
    result = prompt.format(name="World")
    assert result == "Hello World!"


def test_prompt_registry():
    """Test prompt registry operations."""
    prompt = BasePrompt(template="test")
    PromptRegistry.register("test", prompt)

    retrieved = PromptRegistry.get("test")
    assert retrieved == prompt

    with pytest.raises(LLMPromptNotFoundError):
        PromptRegistry.get("nonexistent")


def test_prompt_messages():
    """Test message formatting."""
    prompt = BasePrompt(template="Hello {{ name }}!", system_message="Test system")
    messages = prompt.get_messages(name="World")
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[1]["content"] == "Hello World!"

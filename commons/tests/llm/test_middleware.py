import pytest

from commons.llm.middleware import log_llm_request


@pytest.mark.asyncio
async def test_log_llm_request(caplog):
    """Test logging LLM requests."""
    caplog.set_level("INFO")

    @log_llm_request
    async def test_func(*args, **kwargs):
        return "test result"

    result = await test_func("arg1", kwarg1="value1")
    assert result == "test result"
    assert "LLM Request" in caplog.text
    assert "LLM Response" in caplog.text


@pytest.mark.asyncio
async def test_log_llm_request_error(caplog):
    """Test logging LLM request errors."""

    @log_llm_request
    async def test_func():
        raise ValueError("test error")

    with pytest.raises(ValueError):
        await test_func()
    assert "LLM Error" in caplog.text

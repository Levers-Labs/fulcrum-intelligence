from unittest.mock import AsyncMock

import pytest

from commons.utilities.retries import retry


@pytest.mark.asyncio
async def test_retry_success():
    """Test that function succeeds on first try."""
    mock_func = AsyncMock(return_value="success")
    decorated = retry()(mock_func)

    result = await decorated()

    assert result == "success"
    assert mock_func.call_count == 1


@pytest.mark.asyncio
async def test_retry_fails_then_succeeds():
    """Test that function retries after failure and eventually succeeds."""
    mock_func = AsyncMock(side_effect=[ValueError("error"), ValueError("error"), "success"])
    decorated = retry(retries=3, exceptions=(ValueError,))(mock_func)

    result = await decorated()

    assert result == "success"
    assert mock_func.call_count == 3


@pytest.mark.asyncio
async def test_retry_all_attempts_fail():
    """Test that function raises exception after all retries fail."""
    mock_func = AsyncMock(side_effect=ValueError("error"))
    decorated = retry(retries=2, exceptions=(ValueError,))(mock_func)

    with pytest.raises(ValueError, match="error"):
        await decorated()

    assert mock_func.call_count == 2


@pytest.mark.asyncio
async def test_retry_unhandled_exception():
    """Test that unspecified exceptions are not retried."""
    mock_func = AsyncMock(side_effect=KeyError("error"))
    decorated = retry(retries=3, exceptions=(ValueError,))(mock_func)

    with pytest.raises(KeyError, match="error"):
        await decorated()

    assert mock_func.call_count == 1


@pytest.mark.asyncio
async def test_retry_custom_delay():
    """Test that retry uses specified delay between attempts."""
    mock_func = AsyncMock(side_effect=[ValueError("error"), "success"])
    decorated = retry(retries=2, delay=0.1, exceptions=(ValueError,))(mock_func)

    result = await decorated()

    assert result == "success"
    assert mock_func.call_count == 2

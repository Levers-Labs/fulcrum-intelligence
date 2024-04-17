from collections.abc import AsyncGenerator, Generator
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlmodel import Session

from commons.db.session import get_async_session, get_session


# Pytest fixtures
@pytest.fixture
def database_url():
    return "mock://database"


@pytest.fixture
def options():
    return {}


# Test cases
@patch("commons.db.session.get_engine")
def test_get_session(mock_get_engine, database_url, options):
    mock_engine = MagicMock()
    mock_get_engine.return_value = mock_engine

    session_generator = get_session(database_url, options)
    assert isinstance(session_generator, Generator)

    session = next(session_generator)
    assert isinstance(session, Session)
    mock_get_engine.assert_called_once_with(database_url, options)

    with pytest.raises(StopIteration):
        next(session_generator)


@pytest.mark.asyncio
@patch("commons.db.session.get_async_engine")
async def test_get_async_session(mock_get_async_engine, database_url, options):
    mock_async_engine = MagicMock(spec=AsyncEngine)
    mock_get_async_engine.return_value = mock_async_engine

    async_session_generator = get_async_session(database_url, options)
    assert isinstance(async_session_generator, AsyncGenerator)

    async for session in async_session_generator:
        assert isinstance(session, AsyncSession)
        mock_get_async_engine.assert_called_once_with(database_url, options)
        break

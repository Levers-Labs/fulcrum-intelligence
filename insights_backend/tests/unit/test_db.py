from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Session

from insights_backend.db.config import get_async_session, get_session


@pytest.fixture
def mock_sync_session(mocker):
    return mocker.patch("insights_backend.db.config._get_session", return_value=Session())


@pytest.fixture
def mock_async_session(mocker):
    # Create a mock instance of AsyncSession
    async_session_mock = AsyncMock(spec=AsyncSession)

    # Create an asynchronous generator that yields the mock AsyncSession
    async def async_generator():
        yield AsyncSession()

    mocker.patch("insights_backend.db.config._get_async_session", return_value=async_generator())
    return async_session_mock


def test_get_session(mock_sync_session):
    # Execute get_session to verify it correctly handles the mock
    session_generator = get_session()
    session = next(session_generator)

    assert isinstance(session, Session)


@pytest.mark.asyncio
async def test_get_async_session(mock_async_session):
    session_generator = get_async_session()
    session = await session_generator.__anext__()

    assert isinstance(session, AsyncSession)

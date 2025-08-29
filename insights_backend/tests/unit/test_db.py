import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Session

from insights_backend.db.config import get_async_session, get_session


@pytest.fixture
def mock_sync_session(mocker):
    return mocker.patch("insights_backend.db.config._get_session", return_value=Session())


@pytest.fixture
def mock_async_session(mocker):
    return mocker.patch("insights_backend.db.config._get_async_session", return_value=AsyncSession())


def test_get_session(mock_sync_session):
    # Execute get_session to verify it correctly handles the mock
    session_generator = get_session()
    session = next(session_generator)

    assert isinstance(session, Session)


@pytest.mark.asyncio
async def test_get_async_session(mock_sync_session, mocker):
    # Mock the session manager
    mock_manager = mocker.MagicMock()
    mock_session = AsyncSession()
    mock_manager.session.return_value.__aenter__.return_value = mock_session
    mock_manager.session.return_value.__aexit__.return_value = None

    session_generator = get_async_session(mock_manager)
    session = await session_generator.__anext__()

    assert session == mock_session

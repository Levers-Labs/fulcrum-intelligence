import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Session

from story_manager.db.config import get_session


@pytest.fixture
def mock_sync_session(mocker):
    return mocker.patch("story_manager.db.config._get_session", return_value=Session())


@pytest.fixture
def mock_async_session(mocker):
    return mocker.patch("story_manager.db.config._get_async_session", return_value=AsyncSession())


def test_get_session(mock_sync_session):
    # Execute get_session to verify it correctly handles the mock
    session_generator = get_session()
    session = next(session_generator)

    assert isinstance(session, Session)

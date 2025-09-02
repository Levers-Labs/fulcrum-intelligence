from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic_settings import BaseSettings

from commons.db.v2.lifecycle import dispose_session_manager, get_session_manager, init_session_manager


class MockSettings(BaseSettings):
    """Mock settings for testing lifecycle functions."""

    DATABASE_URL: str = "postgresql+asyncpg://test:test@localhost:5432/test_db"
    SQLALCHEMY_ENGINE_OPTIONS: dict = {"pool_size": 5, "max_overflow": 10}
    DB_PROFILE: str = "dev"
    DB_MAX_CONCURRENT_SESSIONS: int = 20

    class PATHS:
        BASE_DIR = MagicMock()
        BASE_DIR.name = "test_service"

    model_config = {"extra": "ignore"}


@pytest.fixture
def mock_settings():
    """Provide mock settings for testing."""
    return MockSettings()


@pytest.fixture
async def cleanup_lifecycle():
    """Clean up lifecycle state after each test."""
    yield
    await dispose_session_manager()


def test_get_session_manager_fails_when_not_initialized():
    """Test that get_session_manager fails when not initialized."""
    with pytest.raises(AssertionError, match="Session manager not initialized"):
        get_session_manager()


@pytest.mark.asyncio
async def test_init_session_manager_creates_singleton(mock_settings, cleanup_lifecycle):
    """Test that init_session_manager creates and returns a singleton."""
    # First call should create the manager
    manager1 = init_session_manager(mock_settings)
    assert manager1 is not None

    # Second call should return the same instance
    manager2 = init_session_manager(mock_settings)
    assert manager1 is manager2


@pytest.mark.asyncio
async def test_get_session_manager_returns_initialized_manager(mock_settings, cleanup_lifecycle):
    """Test that get_session_manager returns the initialized manager."""
    # Initialize first
    expected_manager = init_session_manager(mock_settings)

    # Get should return the same instance
    actual_manager = get_session_manager()
    assert actual_manager is expected_manager


@pytest.mark.asyncio
async def test_init_with_different_database_url_raises_error(mock_settings, cleanup_lifecycle):
    """Test that initializing with different DATABASE_URL raises ValueError."""
    # Initialize with first settings
    init_session_manager(mock_settings)

    # Create settings with different DATABASE_URL
    different_settings = MockSettings()
    different_settings.DATABASE_URL = "postgresql+asyncpg://different:test@localhost:5432/different_db"

    # Should raise ValueError
    with pytest.raises(ValueError, match="different DATABASE_URL"):
        init_session_manager(different_settings)


@pytest.mark.asyncio
async def test_dispose_session_manager_resets_state(mock_settings):
    """Test that dispose_session_manager properly resets global state."""
    # Initialize manager
    manager1 = init_session_manager(mock_settings)
    assert manager1 is not None

    # Dispose should reset state
    await dispose_session_manager()

    # get_session_manager should now fail
    with pytest.raises(AssertionError):
        get_session_manager()

    # Re-initialization should create new manager
    manager2 = init_session_manager(mock_settings)
    assert manager2 is not None
    # Should be a different instance (though functionally equivalent)
    assert manager2 is not manager1

    # Clean up
    await dispose_session_manager()


@pytest.mark.asyncio
async def test_app_name_derivation(cleanup_lifecycle):
    """Test that app name is properly derived from settings."""
    settings = MockSettings()

    # Mock the PATHS.BASE_DIR.name
    settings.PATHS.BASE_DIR.name = "my_test_app"

    manager = init_session_manager(settings)
    assert manager is not None
    # Note: We can't easily test the app_name in engine options without
    # deeper inspection, but the creation should succeed


class SettingsWithoutPaths(BaseSettings):
    """Settings without PATHS attribute for fallback testing."""

    DATABASE_URL: str = "postgresql+asyncpg://test:test@localhost:5432/test_db"
    SQLALCHEMY_ENGINE_OPTIONS: dict = {}
    DB_PROFILE: str = "dev"
    DB_MAX_CONCURRENT_SESSIONS: int = 20

    model_config = {"extra": "ignore"}


@pytest.mark.asyncio
async def test_app_name_fallback(cleanup_lifecycle):
    """Test app name fallback when PATHS is not available."""
    settings = SettingsWithoutPaths()

    # Should still work with fallback app name derivation
    manager = init_session_manager(settings)
    assert manager is not None

from unittest.mock import MagicMock, patch

import pytest

from commons.db.engine import get_async_engine, get_engine


def test_get_engine_singleton_behavior():
    with patch("commons.db.engine.create_engine", return_value=MagicMock()) as mock_create_engine:
        # First call should create a new engine
        engine1 = get_engine("db_url")
        assert mock_create_engine.called, "create_engine should be called on first access"
        mock_create_engine.reset_mock()

        # Subsequent calls should return the same engine and not call create_engine again
        engine2 = get_engine("db_url")
        assert not mock_create_engine.called, "create_engine should not be called again"
        assert engine1 is engine2, "The same engine instance should be returned on subsequent calls"


@pytest.mark.asyncio
async def test_get_async_engine_singleton_behavior():
    with patch("commons.db.engine.create_async_engine", return_value=MagicMock()) as mock_create_async_engine:
        # First call should create a new async engine
        async_engine1 = get_async_engine("db_url")
        assert mock_create_async_engine.called, "create_async_engine should be called on first access"
        mock_create_async_engine.reset_mock()

        # Subsequent calls should return the same engine and not call create_async_engine again
        async_engine2 = get_async_engine("db_url")
        assert not mock_create_async_engine.called, "create_async_engine should not be called again"
        assert async_engine1 is async_engine2, "The same async engine instance should be returned on subsequent calls"

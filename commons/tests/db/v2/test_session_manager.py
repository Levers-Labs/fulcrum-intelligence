import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.v2.session_manager import AsyncSessionManager, _build_session_factory


class TestBuildSessionFactory:
    """Test the _build_session_factory helper function."""

    @patch("commons.db.v2.session_manager.get_async_engine")
    @patch("commons.db.v2.session_manager.async_sessionmaker")
    def test_build_session_factory(self, mock_async_sessionmaker, mock_get_async_engine):
        """Test session factory creation."""
        mock_engine = MagicMock()
        mock_get_async_engine.return_value = mock_engine
        mock_factory = MagicMock()
        mock_async_sessionmaker.return_value = mock_factory

        url = "postgresql://test"
        options = {"pool_size": 10}

        result = _build_session_factory(url, options)

        mock_get_async_engine.assert_called_once_with(url, options)
        mock_async_sessionmaker.assert_called_once_with(
            bind=mock_engine, class_=AsyncSession, autoflush=False, autocommit=False, expire_on_commit=False
        )
        assert result == mock_factory


class TestAsyncSessionManager:
    """Test the AsyncSessionManager class."""

    @patch("commons.db.v2.session_manager._build_session_factory")
    def test_init(self, mock_build_session_factory):
        """Test AsyncSessionManager initialization."""
        mock_factory = MagicMock()
        mock_build_session_factory.return_value = mock_factory

        url = "postgresql://test"
        options = {"pool_size": 10}
        max_sessions = 50

        manager = AsyncSessionManager(url, options, max_sessions)

        mock_build_session_factory.assert_called_once_with(url, options)
        assert manager._factory == mock_factory
        assert manager._sem._value == max_sessions
        assert manager._active == 0

    @patch("commons.db.v2.session_manager._build_session_factory")
    def test_init_no_limit(self, mock_build_session_factory):
        """Test initialization without session limit."""
        mock_factory = MagicMock()
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {}, None)

        assert manager._sem is None

    @patch("commons.db.v2.session_manager._build_session_factory")
    def test_current_session_count(self, mock_build_session_factory):
        """Test current_session_count property."""
        manager = AsyncSessionManager("postgresql://test", {})

        assert manager.current_session_count == 0

        manager._active = 5
        assert manager.current_session_count == 5

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    @patch("commons.db.v2.session_manager.get_tenant_id")
    async def test_session_context_manager_basic(self, mock_get_tenant_id, mock_build_session_factory):
        """Test basic session context manager functionality."""
        # Setup mocks
        mock_session = AsyncMock(spec=AsyncSession)
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory
        mock_get_tenant_id.return_value = None

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.session() as session:
            assert session == mock_session
            assert manager._active == 1

        # After context exit
        assert manager._active == 0
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    @patch("commons.db.v2.session_manager.get_tenant_id")
    async def test_session_with_tenant_id(self, mock_get_tenant_id, mock_build_session_factory):
        """Test session with tenant ID setup."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory
        mock_get_tenant_id.return_value = 123

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.session(tenant_id=123) as session:
            assert session == mock_session

        # Should execute tenant setup SQL
        expected_calls = [mock_session.execute.call_args_list[0][0][0], mock_session.execute.call_args_list[1][0][0]]
        assert any("SET SESSION ROLE tenant_user" in str(call) for call in expected_calls)
        assert any("SET app.current_tenant=123" in str(call) for call in expected_calls)

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    @patch("commons.db.v2.session_manager.get_tenant_id")
    async def test_session_with_local_settings(self, mock_get_tenant_id, mock_build_session_factory):
        """Test session with local settings."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory
        mock_get_tenant_id.return_value = None

        manager = AsyncSessionManager("postgresql://test", {})
        local_settings = {"work_mem": "256MB", "statement_timeout": "30000"}

        async with manager.session(local_settings=local_settings) as session:
            assert session == mock_session

        # Check that local settings were applied
        execute_calls = [str(call[0][0]) for call in mock_session.execute.call_args_list]
        assert any("SET LOCAL work_mem = '256MB'" in call for call in execute_calls)
        assert any("SET LOCAL statement_timeout = '30000'" in call for call in execute_calls)

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_session_no_commit(self, mock_build_session_factory):
        """Test session without auto-commit."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.session(commit=False) as session:
            assert session == mock_session

        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_session_exception_rollback(self, mock_build_session_factory):
        """Test session rollback on exception."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {})

        with pytest.raises(ValueError):
            async with manager.session() as session:
                assert session == mock_session
                raise ValueError("Test error")

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()
        assert manager._active == 0

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_session_with_semaphore(self, mock_build_session_factory):
        """Test session with concurrency limiting."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {}, max_concurrent_sessions=1)

        # First session should acquire semaphore
        async with manager.session() as session1:
            assert session1 == mock_session
            assert manager._active == 1

            # Second session should block (we can't easily test this without complex async setup)
            # But we can verify the semaphore exists and has the right value
            assert manager._sem._value == 0  # Should be acquired

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_batch_session(self, mock_build_session_factory):
        """Test batch_session context manager."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.batch_session(tenant_id=123, statement_timeout_ms=600000, work_mem="512MB") as session:
            assert session == mock_session

        # Check that batch settings were applied
        execute_calls = [str(call[0][0]) for call in mock_session.execute.call_args_list]
        assert any("SET LOCAL statement_timeout = '600000'" in call for call in execute_calls)
        assert any("SET LOCAL work_mem = '512MB'" in call for call in execute_calls)

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_batch_session_defaults(self, mock_build_session_factory):
        """Test batch_session with default settings."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.batch_session() as session:
            assert session == mock_session

        # Check default statement_timeout was applied
        execute_calls = [str(call[0][0]) for call in mock_session.execute.call_args_list]
        assert any("SET LOCAL statement_timeout = '900000'" in call for call in execute_calls)

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_batch_session_no_commit(self, mock_build_session_factory):
        """Test batch_session without auto-commit."""
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session.execute = AsyncMock()
        mock_factory = MagicMock()
        mock_factory.return_value = mock_session
        mock_build_session_factory.return_value = mock_factory

        manager = AsyncSessionManager("postgresql://test", {})

        async with manager.batch_session(commit=False) as session:
            assert session == mock_session

        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    @patch("commons.db.v2.session_manager._build_session_factory")
    async def test_concurrent_sessions(self, mock_build_session_factory):
        """Test multiple concurrent sessions."""
        mock_factory = MagicMock()
        mock_build_session_factory.return_value = mock_factory

        # Create separate mock sessions for each call
        mock_session1 = AsyncMock(spec=AsyncSession)
        mock_session2 = AsyncMock(spec=AsyncSession)
        mock_factory.side_effect = [mock_session1, mock_session2]

        manager = AsyncSessionManager("postgresql://test", {})

        async def use_session(session_num):
            async with manager.session() as session:
                # Simulate some work
                await asyncio.sleep(0.01)
                return session_num, session

        # Run two sessions concurrently
        results = await asyncio.gather(use_session(1), use_session(2))

        # Both should complete successfully
        assert len(results) == 2
        assert results[0][0] == 1
        assert results[1][0] == 2

        # Both sessions should be closed
        mock_session1.close.assert_called_once()
        mock_session2.close.assert_called_once()

        # Active count should be back to 0
        assert manager._active == 0

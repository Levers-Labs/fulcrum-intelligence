from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from commons.db.v2.engine import (
    _key,
    dispose_all_async_engines,
    dispose_async_engine,
    get_async_engine,
)


class TestEngineKey:
    """Test the _key utility function."""

    def test_key_generation(self):
        """Test key generation with URL and options."""
        url = "postgresql://user:pass@host:5432/db"
        options = {"pool_size": 10, "echo": True}

        key = _key(url, options)

        # Should contain URL and sorted JSON representation
        assert url in key
        assert "echo" in key
        assert "pool_size" in key
        # Keys should be sorted for consistency
        assert key.index("echo") < key.index("pool_size")

    def test_key_consistency(self):
        """Test that same inputs produce same key."""
        url = "postgresql://test"
        options = {"b": 2, "a": 1}

        key1 = _key(url, options)
        key2 = _key(url, options)

        assert key1 == key2

    def test_key_different_options(self):
        """Test that different options produce different keys."""
        url = "postgresql://test"
        options1 = {"pool_size": 10}
        options2 = {"pool_size": 20}

        key1 = _key(url, options1)
        key2 = _key(url, options2)

        assert key1 != key2

    def test_key_empty_options(self):
        """Test key generation with empty options."""
        url = "postgresql://test"
        options = {}

        key = _key(url, options)

        assert url in key
        assert "{}" in key


class TestGetAsyncEngine:
    """Test the get_async_engine function."""

    def setUp(self):
        """Clear engine cache before each test."""
        # Access the private _engines dict to clear it
        from commons.db.v2.engine import _engines

        _engines.clear()

    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    def test_create_new_engine(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test creating a new engine when none exists."""
        self.setUp()

        mock_engine = MagicMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        result = get_async_engine(url, options)

        assert result == mock_engine
        mock_resolve_pool_class.assert_called_once_with(options)
        mock_create_async_engine.assert_called_once_with(url, pool_size=10)

    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    def test_reuse_existing_engine(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test reusing existing engine with same parameters."""
        self.setUp()

        mock_engine = MagicMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        # First call should create engine
        result1 = get_async_engine(url, options)
        # Second call should reuse engine
        result2 = get_async_engine(url, options)

        assert result1 == result2 == mock_engine
        # create_async_engine should only be called once
        assert mock_create_async_engine.call_count == 1

    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    def test_different_options_create_different_engines(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test that different options create different engines."""
        self.setUp()

        mock_engine1 = MagicMock()
        mock_engine2 = MagicMock()
        mock_create_async_engine.side_effect = [mock_engine1, mock_engine2]
        mock_resolve_pool_class.side_effect = [{"pool_size": 10}, {"pool_size": 20}]

        url = "postgresql://test"

        result1 = get_async_engine(url, {"pool_size": 10})
        result2 = get_async_engine(url, {"pool_size": 20})

        assert result1 == mock_engine1
        assert result2 == mock_engine2
        assert result1 != result2
        assert mock_create_async_engine.call_count == 2

    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    def test_none_options_handled(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test that None options are handled properly."""
        self.setUp()

        mock_engine = MagicMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {}

        url = "postgresql://test"

        result = get_async_engine(url, None)

        assert result == mock_engine
        mock_resolve_pool_class.assert_called_once_with({})


class TestDisposeAsyncEngine:
    """Test the dispose_async_engine function."""

    def setUp(self):
        """Clear engine cache before each test."""
        from commons.db.v2.engine import _engines

        _engines.clear()

    @pytest.mark.asyncio
    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    async def test_dispose_existing_engine(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test disposing an existing engine."""
        self.setUp()

        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        # Create engine first
        get_async_engine(url, options)

        # Now dispose it
        await dispose_async_engine(url, options)

        mock_engine.dispose.assert_called_once()

    @pytest.mark.asyncio
    @patch("commons.db.v2.engine.resolve_pool_class")
    async def test_dispose_nonexistent_engine(self, mock_resolve_pool_class):
        """Test disposing a nonexistent engine (should not error)."""
        self.setUp()

        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        # Should not raise an exception
        await dispose_async_engine(url, options)

    @pytest.mark.asyncio
    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    async def test_dispose_removes_from_cache(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test that dispose removes engine from cache."""
        self.setUp()

        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        # Create engine
        get_async_engine(url, options)

        # Dispose it
        await dispose_async_engine(url, options)

        # Getting engine again should create a new one
        mock_create_async_engine.reset_mock()
        get_async_engine(url, options)

        # Should have created a new engine
        assert mock_create_async_engine.call_count == 1


class TestDisposeAllAsyncEngines:
    """Test the dispose_all_async_engines function."""

    def setUp(self):
        """Clear engine cache before each test."""
        from commons.db.v2.engine import _engines

        _engines.clear()

    @pytest.mark.asyncio
    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    async def test_dispose_all_engines(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test disposing all engines."""
        self.setUp()

        mock_engine1 = MagicMock()
        mock_engine1.dispose = AsyncMock()
        mock_engine2 = MagicMock()
        mock_engine2.dispose = AsyncMock()

        mock_create_async_engine.side_effect = [mock_engine1, mock_engine2]
        mock_resolve_pool_class.side_effect = [{"pool_size": 10}, {"pool_size": 20}]

        # Create two different engines
        get_async_engine("postgresql://test1", {"pool_size": 10})
        get_async_engine("postgresql://test2", {"pool_size": 20})

        # Dispose all
        await dispose_all_async_engines()

        # Both engines should be disposed
        mock_engine1.dispose.assert_called_once()
        mock_engine2.dispose.assert_called_once()

    @pytest.mark.asyncio
    @patch("commons.db.v2.engine.create_async_engine")
    @patch("commons.db.v2.engine.resolve_pool_class")
    async def test_dispose_all_clears_cache(self, mock_resolve_pool_class, mock_create_async_engine):
        """Test that dispose_all clears the engine cache."""
        self.setUp()

        mock_engine = MagicMock()
        mock_engine.dispose = AsyncMock()
        mock_create_async_engine.return_value = mock_engine
        mock_resolve_pool_class.return_value = {"pool_size": 10}

        url = "postgresql://test"
        options = {"pool_size": 10}

        # Create engine
        get_async_engine(url, options)

        # Dispose all
        await dispose_all_async_engines()

        # Getting engine again should create a new one
        mock_create_async_engine.reset_mock()
        get_async_engine(url, options)

        # Should have created a new engine
        assert mock_create_async_engine.call_count == 1

    @pytest.mark.asyncio
    async def test_dispose_all_empty_cache(self):
        """Test disposing all engines when cache is empty."""
        self.setUp()

        # Should not raise an exception
        await dispose_all_async_engines()

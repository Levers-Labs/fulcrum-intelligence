import pytest

from query_manager.config import Settings, get_settings


def test_assemble_cors_origins():
    assert Settings.assemble_cors_origins("http://localhost:8000") == ["http://localhost:8000"]
    assert Settings.assemble_cors_origins(["http://localhost:8000"]) == ["http://localhost:8000"]
    assert Settings.assemble_cors_origins("http://localhost:8000, http://localhost:8001") == [
        "http://localhost:8000",
        "http://localhost:8001",
    ]
    assert Settings.assemble_cors_origins(["http://localhost:8000", "http://localhost:8001"]) == [
        "http://localhost:8000",
        "http://localhost:8001",
    ]
    with pytest.raises(ValueError):
        Settings.assemble_cors_origins(123)


def test_get_settings_caching():
    """Test that get_settings properly caches configuration."""
    # First call
    settings1 = get_settings()

    # Second call should return same instance due to @lru_cache
    settings2 = get_settings()

    assert settings1 is settings2


def test_get_settings_returns_valid_config():
    """Test get_settings returns a valid configuration object."""
    settings = get_settings()

    # Test basic attributes exist
    assert hasattr(settings, "SECRET_KEY")
    assert hasattr(settings, "DEBUG")
    assert hasattr(settings, "ENV")

    # Test settings has expected structure
    assert isinstance(settings.SECRET_KEY, str)
    assert isinstance(settings.DEBUG, bool)


def test_settings_has_required_attributes():
    """Test that settings object has all expected attributes."""
    settings = get_settings()

    # Check for basic configuration attributes
    assert hasattr(settings, "PATHS")
    assert hasattr(settings, "ENV")

    # Check environment is accessible
    env_value = settings.ENV
    assert env_value is not None


def test_settings_paths_configuration():
    """Test that PATHS configuration is properly set."""
    settings = get_settings()

    # Test PATHS exists and has structure
    assert hasattr(settings, "PATHS")
    paths = settings.PATHS

    # Paths should be an object
    assert paths is not None

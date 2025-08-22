import os
from unittest.mock import patch

from commons.db.v2.profiles import _deep_merge, build_engine_options


class TestDeepMerge:
    """Test the _deep_merge utility function."""

    def test_simple_merge(self):
        """Test merging simple dictionaries."""
        base = {"a": 1, "b": 2}
        override = {"c": 3, "b": 20}
        result = _deep_merge(base, override)

        expected = {"a": 1, "b": 20, "c": 3}
        assert result == expected

    def test_connect_args_merge(self):
        """Test merging connect_args without server_settings."""
        base = {"connect_args": {"timeout": 30}}
        override = {"connect_args": {"keepalive": True}}
        result = _deep_merge(base, override)

        expected = {"connect_args": {"timeout": 30, "keepalive": True}}
        assert result == expected

    def test_server_settings_merge(self):
        """Test merging server_settings within connect_args."""
        base = {"connect_args": {"timeout": 30, "server_settings": {"app_name": "base", "setting1": "value1"}}}
        override = {
            "connect_args": {"keepalive": True, "server_settings": {"app_name": "override", "setting2": "value2"}}
        }
        result = _deep_merge(base, override)

        expected = {
            "connect_args": {
                "timeout": 30,
                "keepalive": True,
                "server_settings": {"app_name": "override", "setting1": "value1", "setting2": "value2"},
            }
        }
        assert result == expected

    def test_empty_override(self):
        """Test merging with empty override."""
        base = {"a": 1, "b": 2}
        override = {}
        result = _deep_merge(base, override)

        assert result == base
        assert result is not base  # Should be a copy

    def test_none_server_settings(self):
        """Test handling None server_settings in override."""
        base = {"connect_args": {"server_settings": {"app_name": "base"}}}
        override = {"connect_args": {"server_settings": None}}
        result = _deep_merge(base, override)

        expected = {"connect_args": {"server_settings": {"app_name": "base"}}}
        assert result == expected


class TestBuildEngineOptions:
    """Test the build_engine_options function."""

    def test_no_profile_defaults_only(self):
        """Test with no profile returns base options only."""
        result = build_engine_options(None)

        expected = {
            "pool_pre_ping": True,
            "pool_reset_on_return": "rollback",
            "echo": False,
            "future": True,
        }
        assert result == expected

    def test_prod_profile(self):
        """Test prod profile includes production settings."""
        result = build_engine_options("prod")

        assert result["pool_size"] == 25
        assert result["max_overflow"] == 40
        assert result["pool_timeout"] == 45
        assert result["pool_recycle"] == 1200
        assert result["connect_args"]["command_timeout"] == 180
        assert "fulcrum-intellegence" in result["connect_args"]["server_settings"]["application_name"]

    def test_dev_profile(self):
        """Test dev profile includes development settings."""
        result = build_engine_options("dev")

        assert result["pool_size"] == 5
        assert result["max_overflow"] == 20
        assert result["pool_timeout"] == 30
        assert result["pool_recycle"] == 600
        assert result["echo"] is True

    def test_test_profile(self):
        """Test test profile includes test settings."""
        result = build_engine_options("test")

        assert result["poolclass"] == "StaticPool"
        assert result["echo"] is False

    def test_unknown_profile(self):
        """Test unknown profile returns base options only."""
        result = build_engine_options("unknown")

        expected = {
            "pool_pre_ping": True,
            "pool_reset_on_return": "rollback",
            "echo": False,
            "future": True,
        }
        assert result == expected

    def test_overrides_simple(self):
        """Test simple overrides are applied."""
        overrides = {"pool_size": 100, "echo": True}
        result = build_engine_options("prod", overrides)

        assert result["pool_size"] == 100
        assert result["echo"] is True
        assert result["max_overflow"] == 40  # prod default preserved

    def test_overrides_connect_args(self):
        """Test connect_args overrides are merged properly."""
        overrides = {"connect_args": {"command_timeout": 300, "server_settings": {"work_mem": "128MB"}}}
        result = build_engine_options("prod", overrides)

        assert result["connect_args"]["command_timeout"] == 300
        assert result["connect_args"]["server_settings"]["work_mem"] == "128MB"
        # Original prod settings should be preserved
        assert "application_name" in result["connect_args"]["server_settings"]

    def test_app_name_override(self):
        """Test app_name parameter overrides application_name."""
        result = build_engine_options("prod", app_name="custom_app")

        assert result["connect_args"]["server_settings"]["application_name"] == "custom_app"

    def test_app_name_with_overrides(self):
        """Test app_name works with other overrides."""
        overrides = {"connect_args": {"server_settings": {"work_mem": "128MB"}}}
        result = build_engine_options("prod", overrides, app_name="custom_app")

        assert result["connect_args"]["server_settings"]["application_name"] == "custom_app"
        assert result["connect_args"]["server_settings"]["work_mem"] == "128MB"

    def test_app_name_no_connect_args(self):
        """Test app_name creates connect_args if not present."""
        result = build_engine_options(None, app_name="test_app")

        assert result["connect_args"]["server_settings"]["application_name"] == "test_app"

    def test_prod_has_application_name(self):
        """Test prod profile has an application name set."""
        result = build_engine_options("prod")

        # Just verify that an application name is set (could be env var or default)
        assert "application_name" in result["connect_args"]["server_settings"]
        app_name = result["connect_args"]["server_settings"]["application_name"]
        assert isinstance(app_name, str)
        assert len(app_name) > 0

    @patch.dict(os.environ, {}, clear=True)
    def test_prod_fallback_app_name(self):
        """Test prod profile fallback when APP_NAME not in environment."""
        result = build_engine_options("prod")

        assert result["connect_args"]["server_settings"]["application_name"] == "fulcrum-intellegence"

    def test_all_parameters_combined(self):
        """Test all parameters work together."""
        overrides = {"pool_size": 50, "connect_args": {"keepalive": True, "server_settings": {"work_mem": "256MB"}}}
        result = build_engine_options("prod", overrides, "final_app")

        # Override values
        assert result["pool_size"] == 50
        assert result["connect_args"]["keepalive"] is True
        assert result["connect_args"]["server_settings"]["work_mem"] == "256MB"

        # App name override
        assert result["connect_args"]["server_settings"]["application_name"] == "final_app"

        # Preserved prod values
        assert result["max_overflow"] == 40
        assert result["connect_args"]["command_timeout"] == 180

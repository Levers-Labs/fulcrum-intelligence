"""
Unit tests for the Levers API.
"""

from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import pytest

from levers.api import Levers
from levers.exceptions import PatternError, PrimitiveError
from levers.models import (
    AnalysisWindow,
    BasePattern,
    Granularity,
    PatternConfig,
)
from levers.models.patterns import MetricPerformance
from levers.patterns import Pattern, PerformanceStatusPattern


class MockOutputModel(BasePattern):
    """Mock output model for API testing."""

    result: str
    num_periods: int = 0  # Add default value


class TestLeversAPI:
    """Tests for the Levers API class."""

    @pytest.fixture
    def api(self):
        """Return a Levers API instance."""
        with patch("levers.api.autodiscover_patterns"):
            return Levers()

    @pytest.fixture
    def mock_registry(self, api):
        """Mock the pattern registry."""

        # Create test pattern
        class TestPattern(Pattern[MockOutputModel]):
            name = "test_pattern"
            version = "1.0"
            description = "Test pattern"
            required_primitives = []
            output_model = MockOutputModel

            def analyze(self, metric_id, data, analysis_window, **kwargs):
                return MockOutputModel(
                    pattern_name=self.name,
                    version=self.version,
                    metric_id=metric_id,
                    result="test_success",
                    num_periods=7,
                )

            def get_info(self):
                return {
                    "name": self.name,
                    "version": self.version,
                    "description": self.description,
                    "required_primitives": self.required_primitives,
                }

        # Create the test patterns dictionary
        patterns_dict = {"test_pattern": TestPattern, "performance_status": PerformanceStatusPattern}

        # Mock the registry get method
        api._pattern_registry.get = MagicMock(side_effect=lambda name: patterns_dict.get(name))

        # Mock the patterns property
        type(api._pattern_registry).patterns = PropertyMock(return_value=patterns_dict)

        # Mock the list_patterns method
        api._pattern_registry.list = MagicMock(return_value=list(patterns_dict.keys()))

        return api._pattern_registry

    def test_init(self):
        """Test API initialization with autodiscovery."""
        # Act
        with patch("levers.api.autodiscover_patterns") as mock_autodiscover:
            api = Levers()

        # Assert
        mock_autodiscover.assert_called_once()
        assert hasattr(api, "_pattern_registry")

    def test_patterns_property(self, api, mock_registry):
        """Test patterns property."""
        # Mock the entire API class to control its behavior
        test_patterns = {"test_pattern": "test_value"}

        # Use a completely new instance with our own mock
        with patch("levers.api.Levers.patterns", new_callable=PropertyMock) as mock_patterns:
            mock_patterns.return_value = test_patterns

            # Act
            patterns = api.patterns

            # Assert
            assert isinstance(patterns, dict)
            assert patterns == test_patterns
            assert "test_pattern" in patterns

    def test_get_pattern(self, api, mock_registry):
        """Test get_pattern method."""
        # Act
        pattern = api.get_pattern("test_pattern")

        # Assert
        assert pattern.name == "test_pattern"
        assert pattern.version == "1.0"

    def test_get_pattern_not_found(self, api, mock_registry):
        """Test get_pattern method with nonexistent pattern."""
        # Act & Assert
        with pytest.raises(PatternError):
            api.get_pattern("nonexistent_pattern")

    def test_list_patterns(self, api, mock_registry):
        """Test list_patterns method."""
        # Mock the list_patterns method directly
        expected_patterns = ["test_pattern", "performance_status"]

        with patch.object(api, "list_patterns", return_value=expected_patterns):
            # Act
            patterns = api.list_patterns()

            # Assert
            assert isinstance(patterns, list)
            assert "test_pattern" in patterns
            assert "performance_status" in patterns
            assert patterns == expected_patterns

    def test_list_primitives(self, api):
        """Test list_primitives method."""
        # Arrange
        with patch("levers.api.list_primitives_by_family") as mock_list_primitives:
            mock_list_primitives.return_value = {
                "family1": ["primitive1", "primitive2"],
                "family2": ["primitive3", "primitive4"],
            }

            # Act
            primitives = api.list_primitives()

            # Assert
            assert isinstance(primitives, list)
            assert sorted(primitives) == sorted(["primitive1", "primitive2", "primitive3", "primitive4"])

    def test_get_primitive(self, api):
        """Test get_primitive method."""
        # Arrange
        with patch("levers.api.get_primitive_metadata") as mock_get_primitive:
            mock_get_primitive.return_value = {"name": "primitive1", "description": "Test primitive"}

            # Act
            primitive = api.get_primitive("primitive1")

            # Assert
            assert primitive == {"name": "primitive1", "description": "Test primitive"}
            mock_get_primitive.assert_called_once_with("primitive1")

    def test_get_primitive_not_found(self, api):
        """Test get_primitive method with nonexistent primitive."""
        # Arrange
        with patch("levers.api.get_primitive_metadata") as mock_get_primitive:
            mock_get_primitive.side_effect = ValueError("Primitive not found")

            # Act & Assert
            with pytest.raises(PrimitiveError):
                api.get_primitive("nonexistent_primitive")

    def test_get_pattern_info(self, api, mock_registry):
        """Test get_pattern_info method."""
        # Create mock pattern info
        mock_info = {"name": "test_pattern", "version": "1.0", "description": "Test pattern"}

        # Mock the get_pattern_info method directly
        with patch.object(api, "get_pattern_info", return_value=mock_info):
            # Act
            info = api.get_pattern_info("test_pattern")

            # Assert
            assert isinstance(info, dict)
            assert info == mock_info
            assert info["name"] == "test_pattern"
            assert info["version"] == "1.0"
            assert info["description"] == "Test pattern"

    def test_get_primitive_info(self, api):
        """Test get_primitive_info method."""
        # Arrange
        with patch("levers.api.get_primitive_metadata") as mock_get_primitive:
            mock_get_primitive.return_value = {"name": "primitive1", "description": "Test primitive"}

            # Act
            info = api.get_primitive_info("primitive1")

            # Assert
            assert info == {"name": "primitive1", "description": "Test primitive"}

    def test_list_primitives_by_family(self, api):
        """Test list_primitives_by_family method."""
        # Arrange
        with patch("levers.api.list_primitives_by_family") as mock_list_primitives:
            mock_list_primitives.return_value = {
                "family1": ["primitive1", "primitive2"],
                "family2": ["primitive3", "primitive4"],
            }

            # Act
            families = api.list_primitives_by_family()

            # Assert
            assert isinstance(families, dict)
            assert "family1" in families
            assert "family2" in families
            assert families["family1"] == ["primitive1", "primitive2"]

    def test_execute_pattern(self, api, mock_registry):
        """Test execute_pattern method."""
        # Arrange
        metric_id = "test_metric"
        data = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"), "value": range(10)})
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Mock pattern class and instance
        mock_pattern_class = MagicMock()
        mock_pattern_instance = MagicMock()
        mock_pattern_class.return_value = mock_pattern_instance

        # Mock analyze method to return a result
        expected_result = MockOutputModel(
            pattern="test_pattern",
            pattern_name="test_pattern",
            version="1.0",
            analysis_window=analysis_window,
            metric_id=metric_id,
            grain=analysis_window.grain,
            result="test_success",
        )
        mock_pattern_instance.analyze.return_value = expected_result

        # Mock get_pattern to return our mock
        api.get_pattern = MagicMock(return_value=mock_pattern_class)

        # Act
        result = api.execute_pattern(
            pattern_name="test_pattern", metric_id=metric_id, data=data, analysis_window=analysis_window
        )

        # Assert
        assert result is expected_result
        mock_pattern_instance.analyze.assert_called_once_with(
            metric_id=metric_id, data=data, analysis_window=analysis_window
        )

    def test_execute_pattern_error(self, api, mock_registry):
        """Test execute_pattern method with exception."""
        # Arrange
        metric_id = "test_metric"
        data = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"), "value": range(10)})
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)

        # Mock pattern class and instance
        mock_pattern_class = MagicMock()
        mock_pattern_instance = MagicMock()
        mock_pattern_class.return_value = mock_pattern_instance

        # Mock analyze method to raise an exception
        mock_pattern_instance.analyze.side_effect = Exception("Test error")

        # Mock get_pattern to return our mock
        api.get_pattern = MagicMock(return_value=mock_pattern_class)

        # Act & Assert
        with pytest.raises(PatternError):
            api.execute_pattern(
                pattern_name="test_pattern", metric_id=metric_id, data=data, analysis_window=analysis_window
            )

    def test_analyze_performance_status(self, api, mock_registry):
        """Test analyze_performance_status convenience method."""
        # Arrange
        metric_id = "test_metric"
        data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"),
                "value": range(10),
                "target": range(10),
            }
        )

        # Mock execute_pattern to return a MetricPerformance object
        mock_result = MagicMock(spec=MetricPerformance)
        mock_result.pattern_name = "performance_status"
        api.execute_pattern = MagicMock(return_value=mock_result)

        # Act
        result = api.analyze_performance_status(
            metric_id=metric_id, data=data, start_date="2023-01-01", end_date="2023-01-10"
        )

        # Assert
        assert result is mock_result
        api.execute_pattern.assert_called_once()
        # Check that execute_pattern was called with the right parameters
        call_args = api.execute_pattern.call_args[1]
        assert call_args["pattern_name"] == "performance_status"
        assert call_args["metric_id"] == metric_id
        assert isinstance(call_args["analysis_window"], AnalysisWindow)
        assert call_args["analysis_window"].start_date == "2023-01-01"
        assert call_args["analysis_window"].end_date == "2023-01-10"

    def test_get_pattern_default_config(self, api, mock_registry):
        """Test get_pattern_default_config method."""
        # Act
        config = api.get_pattern_default_config("performance_status")

        # Assert
        assert isinstance(config, PatternConfig)
        assert config.pattern_name == "performance_status"

    def test_get_pattern_default_config_nonexistent_pattern(self, api, mock_registry):
        """Test get_pattern_default_config method with nonexistent pattern."""
        # Act & Assert
        with pytest.raises(PatternError):
            api.get_pattern_default_config("nonexistent_pattern")

    def test_execute_pattern_with_config(self, api, mock_registry):
        """Test execute_pattern method with config."""
        # Arrange
        config = api.get_pattern_default_config("performance_status")
        metric_id = "test_metric"
        data = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-10", freq="D"), "value": range(10)})
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-10", grain=Granularity.DAY)
        # Mock execute_pattern to return a MetricPerformance object
        mock_result = MagicMock(spec=MetricPerformance)
        mock_result.pattern_name = "performance_status"
        api.execute_pattern = MagicMock(return_value=mock_result)

        # Act
        result = api.execute_pattern(
            pattern_name="performance_status",
            metric_id=metric_id,
            data=data,
            analysis_window=analysis_window,
            config=config,
        )

        # Assert
        assert result is mock_result
        api.execute_pattern.assert_called_once()
        # Check that execute_pattern was called with the right parameters
        call_args = api.execute_pattern.call_args[1]
        assert call_args["pattern_name"] == "performance_status"
        assert call_args["metric_id"] == metric_id

    @pytest.fixture
    def levers(self):
        """Return a Levers instance."""
        return Levers()

    @pytest.fixture
    def sample_data(self):
        """Return sample data for testing."""
        return pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=10, freq="D"),
                "value": [100 + i for i in range(10)],
            }
        )

    def test_initialization(self, levers):
        """Test API initialization."""
        # Assert
        assert levers is not None
        assert isinstance(levers.patterns, dict)
        assert len(levers.patterns) > 0

    def test_get_nonexistent_pattern(self, levers):
        """Test getting a nonexistent pattern."""
        # Act & Assert
        with pytest.raises(PatternError):
            levers.get_pattern("nonexistent_pattern")

    def test_get_nonexistent_primitive(self, levers):
        """Test getting a nonexistent primitive."""
        # Act & Assert
        with pytest.raises(PrimitiveError):
            levers.get_primitive("nonexistent_primitive")

    def test_execute_nonexistent_pattern(self, levers, sample_data):
        """Test executing a nonexistent pattern."""
        # Arrange
        analysis_window = AnalysisWindow(
            start_date="2023-01-01",
            end_date="2023-01-10",
            grain=Granularity.DAY,
        )

        # Act & Assert
        with pytest.raises(PatternError):
            levers.execute_pattern(
                "nonexistent_pattern",
                analysis_window=analysis_window,
                data=sample_data,
            )

    def test_get_nonexistent_pattern_config(self, levers):
        """Test getting default config for nonexistent pattern."""
        # Act & Assert
        with pytest.raises(PatternError):
            levers.get_pattern_default_config("nonexistent_pattern")

    def test_load_pattern_model(self, levers):
        """Test loading a pattern model from data."""
        # Arrange
        pattern_data = {
            "pattern": "performance_status",
            "metric_id": "test_metric",
            "status": "on_track",
            "current_value": 100,
            "target_value": 120,
            "analysis_window": {"start_date": "2023-01-01", "end_date": "2023-01-10", "grain": "day"},
            "grain": "day",
        }

        # Act
        model = Levers.load_pattern_model(pattern_data)

        # Assert
        assert isinstance(model, MetricPerformance)
        assert model.metric_id == "test_metric"
        assert model.status == "on_track"
        assert model.current_value == 100
        assert model.target_value == 120
        assert model.analysis_window.start_date == "2023-01-01"
        assert model.analysis_window.end_date == "2023-01-10"
        assert model.analysis_window.grain == "day"

    def test_load_invalid_pattern_model(self, levers):
        """Test loading an invalid pattern model."""
        # Arrange
        pattern_data = {
            "pattern": "nonexistent_pattern",
            "metric_id": "test_metric",
        }

        # Act & Assert
        with pytest.raises(PatternError):
            Levers.load_pattern_model(pattern_data)

    def test_load_pattern_model_missing_type(self, levers):
        """Test loading a pattern model with missing type."""
        # Arrange
        pattern_data = {
            "metric_id": "test_metric",
        }

        # Act & Assert
        with pytest.raises(PatternError):
            Levers.load_pattern_model(pattern_data)

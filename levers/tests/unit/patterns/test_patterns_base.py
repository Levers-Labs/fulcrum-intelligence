"""
Unit tests for the Pattern base class.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from levers.exceptions import (
    InvalidDataError,
    MissingDataError,
    PatternError,
    TimeRangeError,
    ValidationError,
)
from levers.models.common import AnalysisWindow, BasePattern, Granularity
from levers.patterns.base import Pattern


# Create a minimal implementation of Pattern for testing
class TestOutputModel(BasePattern):
    """Test output model for pattern testing."""

    result: str
    num_periods: int = 0  # Add default value


class TestPattern(Pattern[TestOutputModel]):
    """Test pattern implementation."""

    name = "test_pattern"
    version = "1.0"
    description = "Test pattern for unit testing"
    required_primitives = ["test_primitive"]
    output_model = TestOutputModel

    def analyze(self, metric_id: str, data: pd.DataFrame, analysis_window: AnalysisWindow, **kwargs) -> TestOutputModel:
        """Test implementation of analyze method."""
        # Validate and preprocess data
        processed_data = self.preprocess_data(data, analysis_window)

        # Return a test result
        return TestOutputModel(
            pattern=self.name,
            version=self.version,
            metric_id=metric_id,
            analysis_window=analysis_window,
            result="test_success",
            num_periods=len(processed_data),  # Calculate from data
        )


class TestPatternBase:
    """Tests for the Pattern base class."""

    def test_init(self):
        """Test pattern initialization."""
        # Arrange & Act
        pattern = TestPattern()

        # Assert
        assert pattern.name == "test_pattern"
        assert pattern.version == "1.0"
        assert pattern.description == "Test pattern for unit testing"
        assert pattern.required_primitives == ["test_primitive"]
        assert pattern.output_model == TestOutputModel

    def test_validate_output_dict(self):
        """Test output validation with dictionary input."""
        # Arrange
        pattern = TestPattern()
        output_dict = {
            "pattern": "test_pattern",
            "version": "1.0",
            "metric_id": "test_metric",
            "analysis_window": AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY),
            "result": "test_success",
            "num_periods": 31,
        }

        # Act
        result = pattern.validate_output(output_dict)

        # Assert
        assert isinstance(result, TestOutputModel)
        assert result.pattern == "test_pattern"
        assert result.metric_id == "test_metric"
        assert result.result == "test_success"

    def test_validate_output_model(self):
        """Test output validation with model input."""
        # Arrange
        pattern = TestPattern()
        output_model = TestOutputModel(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY),
            result="test_success",
            num_periods=31,
        )

        # Act
        result = pattern.validate_output(output_model)

        # Assert
        assert result is output_model
        assert result.pattern == "test_pattern"
        assert result.metric_id == "test_metric"

    def test_validate_output_invalid(self):
        """Test output validation with invalid input."""
        # Arrange
        pattern = TestPattern()
        output_dict = {
            "pattern": "test_pattern",
            "version": "1.0",
            # Missing required fields
        }

        # Act & Assert
        with pytest.raises(ValidationError):
            pattern.validate_output(output_dict)

    def test_validate_output_no_model(self):
        """Test output validation with no output model defined."""
        # Arrange
        pattern = TestPattern()
        pattern.output_model = None  # type: ignore

        # Act & Assert
        with pytest.raises(PatternError):
            pattern.validate_output({})

    def test_validate_time_window(self):
        """Test time window validation."""
        # Arrange
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})
        start_date = pd.Timestamp("2023-01-10")
        end_date = pd.Timestamp("2023-01-20")

        # Act
        result = TestPattern.validate_time_window(df, start_date, end_date)

        # Assert
        assert len(result) == 11  # 10th to 20th inclusive
        assert result["date"].min() == start_date
        assert result["date"].max() == end_date

    def test_validate_time_window_missing_column(self):
        """Test time window validation with missing date column."""
        # Arrange
        df = pd.DataFrame(
            {"not_date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)}
        )
        start_date = pd.Timestamp("2023-01-10")
        end_date = pd.Timestamp("2023-01-20")

        # Act & Assert
        with pytest.raises(ValidationError):
            TestPattern.validate_time_window(df, start_date, end_date)

    def test_validate_time_window_empty_range(self):
        """Test time window validation with empty date range."""
        # Arrange
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})
        start_date = pd.Timestamp("2023-02-01")
        end_date = pd.Timestamp("2023-02-10")

        # Act & Assert
        with pytest.raises(TimeRangeError):
            TestPattern.validate_time_window(df, start_date, end_date)

    def test_validate_data(self):
        """Test data validation."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                "value": range(31),
                "target": range(50, 81),
            }
        )
        required_columns = ["date", "value"]

        # Act
        result = pattern.validate_data(df, required_columns)

        # Assert
        assert result is True

    def test_validate_data_missing_columns(self):
        """Test data validation with missing columns."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"),
                # Missing "value" column
            }
        )
        required_columns = ["date", "value"]

        # Act & Assert
        with pytest.raises(MissingDataError):
            pattern.validate_data(df, required_columns)

    def test_validate_analysis_window(self):
        """Test analysis window validation."""
        # Arrange
        pattern = TestPattern()
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.validate_analysis_window(analysis_window)

        # Assert
        assert result is analysis_window

    def test_validate_analysis_window_invalid(self):
        """Test analysis window validation with invalid window."""
        # Arrange
        pattern = TestPattern()
        analysis_window = AnalysisWindow(
            start_date="2023-01-31", end_date="2023-01-01", grain=Granularity.DAY  # End date before start date
        )

        # Act & Assert
        with pytest.raises(InvalidDataError):
            pattern.validate_analysis_window(analysis_window)

    def test_handle_empty_data(self):
        """Test handling of empty data."""
        # Arrange
        pattern = TestPattern()
        metric_id = "test_metric"
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        with pytest.raises(ValidationError):
            pattern.handle_empty_data(metric_id, analysis_window)

    def test_extract_date_range(self):
        """Test date range extraction."""
        # Arrange
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})

        # Act
        result = TestPattern.extract_date_range(df)

        # Assert
        assert result["start_date"] == "2023-01-01"
        assert result["end_date"] == "2023-01-31"

    def test_extract_date_range_missing_column(self):
        """Test date range extraction with missing date column."""
        # Arrange
        df = pd.DataFrame(
            {"not_date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)}
        )

        # Act & Assert
        with pytest.raises(MissingDataError):
            TestPattern.extract_date_range(df)

    def test_create_analysis_window(self):
        """Test analysis window creation."""
        # Arrange
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})

        # Act
        result = TestPattern.create_analysis_window(df, Granularity.DAY)

        # Assert
        assert result.start_date == "2023-01-01"
        assert result.end_date == "2023-01-31"
        assert result.grain == Granularity.DAY

    def test_preprocess_data(self):
        """Test data preprocessing."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})
        analysis_window = AnalysisWindow(start_date="2023-01-10", end_date="2023-01-20", grain=Granularity.DAY)

        # Act
        result = pattern.preprocess_data(df, analysis_window)

        # Assert
        assert len(result) == 11  # 10th to 20th inclusive
        assert result["date"].min() == pd.Timestamp("2023-01-10")
        assert result["date"].max() == pd.Timestamp("2023-01-20")

    def test_get_info(self):
        """Test getting pattern information."""
        # Arrange
        pattern = TestPattern()

        # Mock the get_info method
        with patch.object(TestPattern, "get_info") as mock_get_info:
            mock_get_info.return_value = {
                "name": "test_pattern",
                "version": "1.0",
                "description": "Test pattern for unit testing",
                "required_primitives": ["test_primitive"],
            }

            # Act
            info = pattern.get_info()

            # Assert
            assert isinstance(info, dict)
            assert info["name"] == "test_pattern"
            assert info["version"] == "1.0"
            assert info["description"] == "Test pattern for unit testing"
            assert info["required_primitives"] == ["test_primitive"]

    def test_analyze(self):
        """Test analyze method."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame({"date": pd.date_range(start="2023-01-01", end="2023-01-31", freq="D"), "value": range(31)})
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Act
        result = pattern.analyze("test_metric", df, analysis_window)

        # Assert
        assert result.pattern == "test_pattern"
        assert result.metric_id == "test_metric"
        assert result.result == "test_success"

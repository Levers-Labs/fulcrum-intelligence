"""
Unit tests for common models used across patterns.
"""

import json
from datetime import date, datetime

import pytest
from pydantic import ValidationError

from levers.models import (
    AnalysisWindow,
    BaseModel,
    BasePattern,
    Granularity,
)


class TestBaseModel:
    """Tests for the BaseModel class."""

    def test_to_dict(self):
        """Test the to_dict method."""

        # Arrange
        class TestModel(BaseModel):
            name: str
            value: int
            timestamp: datetime = None

        test_model = TestModel(name="test", value=42, timestamp=datetime(2023, 1, 1, 12, 0, 0))

        # Act
        result = test_model.to_dict()

        # Assert
        assert isinstance(result, dict)
        assert result["name"] == "test"
        assert result["value"] == 42
        assert result["timestamp"] == "2023-01-01T12:00:00"

    def test_json_serialization(self):
        """Test that models can be JSON serialized."""

        # Arrange
        class TestModel(BaseModel):
            name: str
            value: int
            timestamp: datetime = None

        test_model = TestModel(name="test", value=42, timestamp=datetime(2023, 1, 1, 12, 0, 0))

        # Act
        json_str = test_model.model_dump_json()

        # Assert
        assert isinstance(json_str, str)

        # Check we can deserialize it back
        data = json.loads(json_str)
        assert data["name"] == "test"
        assert data["value"] == 42
        assert data["timestamp"] == "2023-01-01T12:00:00"

    def test_model_validation(self):
        """Test model validation."""

        # Arrange
        class TestModel(BaseModel):
            name: str
            value: int

        # Act & Assert - Valid case
        model = TestModel(name="test", value=42)
        assert model.name == "test"
        assert model.value == 42

        # Act & Assert - Invalid case
        with pytest.raises(ValidationError):
            TestModel(name="test", value="not_an_int")

    def test_nested_models(self):
        """Test nested models."""

        # Arrange
        class NestedModel(BaseModel):
            value: int

        class ParentModel(BaseModel):
            name: str
            nested: NestedModel

        # Act
        model = ParentModel(name="parent", nested={"value": 42})

        # Assert
        assert model.name == "parent"
        assert isinstance(model.nested, NestedModel)
        assert model.nested.value == 42

        # Test to_dict
        result = model.to_dict()
        assert result["name"] == "parent"
        assert result["nested"]["value"] == 42


class TestAnalysisWindow:
    """Tests for the AnalysisWindow class."""

    def test_valid_analysis_window(self):
        """Test creating a valid analysis window."""
        # Arrange & Act
        window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        # Assert
        assert window.start_date == "2023-01-01"
        assert window.end_date == "2023-01-31"
        assert window.grain == Granularity.DAY

    def test_default_grain(self):
        """Test that the default grain is DAY."""
        # Arrange & Act
        window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31")

        # Assert
        assert window.grain == Granularity.DAY

    def test_dict_conversion(self):
        """Test conversion to dictionary."""
        # Arrange
        window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.WEEK)

        # Act
        result = window.to_dict()

        # Assert
        assert result["start_date"] == "2023-01-01"
        assert result["end_date"] == "2023-01-31"
        assert result["grain"] == "week"

    def test_invalid_dates(self):
        """Test validation with invalid dates."""
        # Since AnalysisWindow uses string fields without explicit validation,
        # we need to test the date string format in the context where it's actually used
        # For example, in time_series.py where it's converted to datetime

        # But we can still test some basic string validation
        with pytest.raises(ValidationError):
            AnalysisWindow(start_date=None, end_date="2023-01-31")

        with pytest.raises(ValidationError):
            AnalysisWindow(start_date="2023-01-01", end_date=None)


class TestBasePattern:
    """Tests for the BasePattern class."""

    def test_basic_properties(self):
        """Test the basic properties of BasePattern."""
        # Arrange & Act
        pattern = BasePattern(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            grain=Granularity.DAY,
        )

        # Assert
        assert pattern.pattern == "test_pattern"
        assert pattern.version == "1.0"
        assert pattern.metric_id == "test_metric"
        assert pattern.analysis_window.start_date == "2023-01-01"
        assert pattern.analysis_window.end_date == "2023-01-31"
        assert pattern.analysis_window.grain == Granularity.DAY
        assert pattern.num_periods == 0
        assert pattern.grain == Granularity.DAY
        assert pattern.error is None

    def test_with_error(self):
        """Test creating a pattern with error information."""
        # Arrange & Act
        error_data = {"message": "Test error", "code": "TEST_ERROR"}

        pattern = BasePattern(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            grain=Granularity.DAY,
            error=error_data,
        )

        # Assert
        assert pattern.error is not None
        assert pattern.error["message"] == "Test error"
        assert pattern.error["code"] == "TEST_ERROR"

    def test_default_dates(self):
        """Test that default dates are set correctly."""
        # Arrange & Act
        today = date.today()
        now = datetime.now()

        pattern = BasePattern(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            grain=Granularity.DAY,
        )

        # Assert
        assert pattern.analysis_date == today
        # Ensure the evaluation_time is close to now
        assert abs((pattern.evaluation_time - now).total_seconds()) < 5  # Within 5 seconds

    def test_with_custom_dates(self):
        """Test creating a pattern with custom dates."""
        # Arrange & Act
        custom_date = date(2023, 1, 15)
        custom_datetime = datetime(2023, 1, 15, 12, 0, 0)

        pattern = BasePattern(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            grain=Granularity.DAY,
            analysis_date=custom_date,
            evaluation_time=custom_datetime,
        )

        # Assert
        assert pattern.analysis_date == custom_date
        assert pattern.evaluation_time == custom_datetime

    def test_to_dict(self):
        """Test conversion to dictionary."""
        # Arrange
        pattern = BasePattern(
            pattern="test_pattern",
            version="1.0",
            metric_id="test_metric",
            analysis_window=AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31"),
            grain=Granularity.DAY,
            num_periods=10,
        )

        # Act
        result = pattern.to_dict()

        # Assert
        assert result["pattern"] == "test_pattern"
        assert result["version"] == "1.0"
        assert result["metric_id"] == "test_metric"
        assert result["analysis_window"]["start_date"] == "2023-01-01"
        assert result["analysis_window"]["end_date"] == "2023-01-31"
        assert result["num_periods"] == 10

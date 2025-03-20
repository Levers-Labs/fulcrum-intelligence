"""
Unit tests for the primitives module utility functions.
"""

import pytest

from levers.exceptions import PrimitiveError
from levers.primitives import get_primitive_metadata, list_primitives_by_family


class TestListPrimitivesByFamily:
    """Tests for the list_primitives_by_family function."""

    def test_returns_dict(self):
        """Test that the function returns a dictionary."""
        # Act
        result = list_primitives_by_family()

        # Assert
        assert isinstance(result, dict)

    def test_contains_expected_families(self):
        """Test that the result contains the expected primitive families."""
        # Act
        result = list_primitives_by_family()

        # Assert
        assert "numeric" in result
        assert "performance" in result

    def test_numeric_primitives(self):
        """Test that the numeric primitives are correctly listed."""
        # Act
        result = list_primitives_by_family()

        # Assert
        assert "calculate_difference" in result["numeric"]
        assert "calculate_percentage_difference" in result["numeric"]
        assert "safe_divide" in result["numeric"]
        assert "round_to_precision" in result["numeric"]

    def test_performance_primitives(self):
        """Test that the performance primitives are correctly listed."""
        # Act
        result = list_primitives_by_family()

        # Assert
        assert "calculate_metric_gva" in result["performance"]
        assert "calculate_historical_gva" in result["performance"]
        assert "classify_metric_status" in result["performance"]
        assert "detect_status_changes" in result["performance"]
        assert "track_status_durations" in result["performance"]


class TestGetPrimitiveMetadata:
    """Tests for the get_primitive_metadata function."""

    def test_get_numeric_primitive_metadata(self):
        """Test getting metadata for a numeric primitive."""
        # Act
        result = get_primitive_metadata("calculate_difference")

        # Assert
        assert result["name"] == "calculate_difference"
        assert "description" in result
        assert result["family"] == "numeric"
        assert "version" in result

    def test_get_performance_primitive_metadata(self):
        """Test getting metadata for a performance primitive."""
        # Act
        result = get_primitive_metadata("calculate_metric_gva")

        # Assert
        assert result["name"] == "calculate_metric_gva"
        assert "description" in result
        assert result["family"] == "performance"
        assert "version" in result

    def test_nonexistent_primitive(self):
        """Test getting metadata for a nonexistent primitive."""
        # Act & Assert
        with pytest.raises(PrimitiveError, match="Primitive not found"):
            get_primitive_metadata("nonexistent_primitive")

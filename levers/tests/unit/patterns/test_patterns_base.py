"""
Unit tests for the Pattern base class.
"""

from datetime import date
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
from levers.models import AnalysisWindow, BasePattern, Granularity
from levers.patterns import Pattern


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

    def analyze(
        self,
        metric_id: str,
        data: pd.DataFrame,
        analysis_window: AnalysisWindow,
        analysis_date: date | None = None,
        **kwargs
    ) -> TestOutputModel:
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

    def test_fill_missing_dates_daily_grain(self):
        """Test filling missing dates with daily grain."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-01-03", "2023-01-05"]  # Missing 2nd and 4th
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [10, 20, 30], "category": ["A", "B", "C"]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        assert len(result) == 5  # Should have all days from 1st to 5th
        expected_dates = pd.Series(pd.date_range("2023-01-01", "2023-01-05", freq="D"))
        pd.testing.assert_series_equal(result["date"].reset_index(drop=True), expected_dates, check_names=False)

        # Check that missing dates are filled with 0
        assert result[result["date"] == "2023-01-02"]["value"].iloc[0] == 0.0
        assert result[result["date"] == "2023-01-04"]["value"].iloc[0] == 0.0

        # Check that original values are preserved
        assert result[result["date"] == "2023-01-01"]["value"].iloc[0] == 10
        assert result[result["date"] == "2023-01-03"]["value"].iloc[0] == 20
        assert result[result["date"] == "2023-01-05"]["value"].iloc[0] == 30

    def test_fill_missing_dates_weekly_grain(self):
        """Test filling missing dates with weekly grain (Monday start)."""
        # Arrange
        pattern = TestPattern()
        # Use Mondays for weekly data
        dates = ["2023-01-02", "2023-01-16"]  # Missing week of 9th
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [100, 200]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.WEEK)

        # Assert
        assert len(result) == 3  # Should have 3 weeks: 2nd, 9th, 16th
        expected_dates = pd.Series(pd.date_range("2023-01-02", "2023-01-16", freq="W-MON"))
        pd.testing.assert_series_equal(result["date"].reset_index(drop=True), expected_dates, check_names=False)

        # Check that missing week is filled with 0
        assert result[result["date"] == "2023-01-09"]["value"].iloc[0] == 0.0

    def test_fill_missing_dates_monthly_grain(self):
        """Test filling missing dates with monthly grain (1st of month)."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-03-01"]  # Missing February
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [100, 300]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.MONTH)

        # Assert
        assert len(result) == 3  # Should have 3 months: Jan, Feb, Mar
        expected_dates = pd.Series(pd.date_range("2023-01-01", "2023-03-01", freq="MS"))
        pd.testing.assert_series_equal(result["date"].reset_index(drop=True), expected_dates, check_names=False)

        # Check that missing month is filled with 0
        assert result[result["date"] == "2023-02-01"]["value"].iloc[0] == 0.0

    def test_fill_missing_dates_quarterly_grain(self):
        """Test filling missing dates with quarterly grain."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-07-01"]  # Q1 and Q3, missing Q2
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [1000, 3000]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.QUARTER)

        # Assert
        assert len(result) == 3  # Should have 3 quarters
        expected_dates = pd.Series(pd.date_range("2023-01-01", "2023-07-01", freq="QS"))
        pd.testing.assert_series_equal(result["date"].reset_index(drop=True), expected_dates, check_names=False)

        # Check that missing quarter is filled with 0
        assert result[result["date"] == "2023-04-01"]["value"].iloc[0] == 0.0

    def test_fill_missing_dates_yearly_grain(self):
        """Test filling missing dates with yearly grain."""
        # Arrange
        pattern = TestPattern()
        dates = ["2021-01-01", "2023-01-01"]  # Missing 2022
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [2021, 2023]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.YEAR)

        # Assert
        assert len(result) == 3  # Should have 3 years
        expected_dates = pd.Series(pd.date_range("2021-01-01", "2023-01-01", freq="YS"))
        pd.testing.assert_series_equal(result["date"].reset_index(drop=True), expected_dates, check_names=False)

        # Check that missing year is filled with 0
        assert result[result["date"] == "2022-01-01"]["value"].iloc[0] == 0.0

    def test_fill_missing_dates_with_categorical_columns(self):
        """Test filling missing dates with categorical columns that should be forward/backward filled."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-01-03"]
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(dates),
                "value": [10, 30],
                "dimension_slice": ["product_A", "product_B"],
                "segment": ["segment_1", "segment_2"],
                "label": ["label_X", "label_Y"],
            }
        )

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        assert len(result) == 3  # Should have all 3 days

        # Check that categorical columns are properly filled
        # Should forward fill for the missing date
        missing_date_row = result[result["date"] == "2023-01-02"]
        assert missing_date_row["dimension_slice"].iloc[0] == "product_A"  # Forward filled
        assert missing_date_row["segment"].iloc[0] == "segment_1"  # Forward filled
        assert missing_date_row["label"].iloc[0] == "label_X"  # Forward filled

    def test_fill_missing_dates_with_nan_and_inf_values(self):
        """Test filling missing dates with NaN and infinity values."""
        # Arrange
        pattern = TestPattern()
        import numpy as np

        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
                "value": [10, np.NaN, 30],
                "infinity_col": [5, np.inf, -np.inf],
                "normal_col": [1, 2, 3],
            }
        )

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        # NaN and infinity values should be replaced with 0
        assert result["value"].iloc[1] == 0.0  # NaN replaced
        assert result["infinity_col"].iloc[1] == 0.0  # inf replaced
        assert result["infinity_col"].iloc[2] == 0.0  # -inf replaced
        assert result["normal_col"].iloc[1] == 2  # Normal value preserved

    def test_fill_missing_dates_empty_dataframe(self):
        """Test fill_missing_dates with empty DataFrame."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame({"date": [], "value": []})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        assert result.empty
        assert list(result.columns) == ["date", "value"]

    def test_fill_missing_dates_none_dataframe(self):
        """Test fill_missing_dates with None DataFrame."""
        # Arrange
        pattern = TestPattern()

        # Act
        result = pattern.fill_missing_dates(None, Granularity.DAY)

        # Assert
        assert result is None

    def test_fill_missing_dates_missing_date_column(self):
        """Test fill_missing_dates with missing date column."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame({"not_date": ["2023-01-01", "2023-01-02"], "value": [10, 20]})

        # Act & Assert
        with pytest.raises(MissingDataError) as exc_info:
            pattern.fill_missing_dates(df, Granularity.DAY)

        assert "Date column 'date' not found in data" in str(exc_info.value)

    def test_fill_missing_dates_custom_date_column(self):
        """Test fill_missing_dates with custom date column name."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-01-03"]  # Missing 2nd
        df = pd.DataFrame({"custom_date": pd.to_datetime(dates), "value": [10, 30]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY, date_col="custom_date")

        # Assert
        assert len(result) == 3  # Should have all 3 days
        assert result[result["custom_date"] == "2023-01-02"]["value"].iloc[0] == 0.0

    def test_fill_missing_dates_custom_fill_value(self):
        """Test fill_missing_dates with custom fill value."""
        # Arrange
        pattern = TestPattern()
        dates = ["2023-01-01", "2023-01-03"]  # Missing 2nd
        df = pd.DataFrame({"date": pd.to_datetime(dates), "value": [10, 30]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY, fill_value=-1.0)

        # Assert
        assert len(result) == 3  # Should have all 3 days
        assert result[result["date"] == "2023-01-02"]["value"].iloc[0] == -1.0

    def test_fill_missing_dates_single_date(self):
        """Test fill_missing_dates with single date (no missing dates to fill)."""
        # Arrange
        pattern = TestPattern()
        df = pd.DataFrame({"date": pd.to_datetime(["2023-01-01"]), "value": [10]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        assert len(result) == 1  # Should remain unchanged
        assert result["value"].iloc[0] == 10

    def test_fill_missing_dates_already_complete(self):
        """Test fill_missing_dates with already complete date series."""
        # Arrange
        pattern = TestPattern()
        dates = pd.date_range("2023-01-01", "2023-01-05", freq="D")
        df = pd.DataFrame({"date": dates, "value": [10, 20, 30, 40, 50]})

        # Act
        result = pattern.fill_missing_dates(df, Granularity.DAY)

        # Assert
        assert len(result) == 5  # Should remain unchanged
        pd.testing.assert_frame_equal(result.reset_index(drop=True), df.reset_index(drop=True))

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

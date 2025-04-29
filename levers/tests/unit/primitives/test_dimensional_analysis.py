"""
Unit tests for dimensional analysis primitives.
"""

import pandas as pd
import pytest

from levers.exceptions import CalculationError, ValidationError
from levers.models import ConcentrationMethod
from levers.primitives import (
    build_slices_performance_list,
    calculate_concentration_index,
    calculate_slice_metrics,
    compare_dimension_slices_over_time,
    compute_historical_slice_rankings,
    compute_slice_shares,
    difference_from_average,
    highlight_slice_comparisons,
    identify_largest_smallest_by_share,
)


@pytest.fixture
def sample_slice_data():
    """Fixture providing sample slice data for testing."""
    return pd.DataFrame(
        {"slice_value": ["North America", "Europe", "APAC", "LATAM"], "aggregated_value": [1000, 800, 600, 400]}
    )


@pytest.fixture
def sample_time_series_data():
    """Fixture providing sample time series data for testing."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", end="2023-01-10"),
            "slice_value": ["North America"] * 4 + ["Europe"] * 3 + ["APAC"] * 2 + ["LATAM"],
            "value": [100, 110, 120, 130, 90, 95, 100, 80, 85, 70],
        }
    )


@pytest.fixture
def sample_comparison_data():
    """Fixture providing sample comparison data with prior and current periods."""
    prior = pd.DataFrame(
        {"slice_value": ["North America", "Europe", "APAC", "LATAM"], "aggregated_value": [800, 750, 550, 450]}
    )

    current = pd.DataFrame(
        {"slice_value": ["North America", "Europe", "APAC", "LATAM"], "aggregated_value": [1000, 800, 600, 400]}
    )

    return prior, current


@pytest.fixture
def merged_slice_data():
    """Fixture providing pre-merged slice data for the current and prior periods."""
    return pd.DataFrame(
        {
            "slice_col": ["North America", "Europe", "APAC", "LATAM"],
            "val_current": [1000, 800, 600, 400],
            "val_prior": [800, 750, 550, 450],
            "share_pct_current": [35.7, 28.6, 21.4, 14.3],
            "share_pct_prior": [31.4, 29.4, 21.6, 17.6],
            "share_diff": [4.3, -0.8, -0.2, -3.3],
        }
    )


class TestCalculateSliceMetrics:
    """Tests for the calculate_slice_metrics function."""

    def test_normal_calculation(self, sample_slice_data):
        """Test basic slice metrics calculation."""
        # Act
        result = calculate_slice_metrics(sample_slice_data, "slice_value", "aggregated_value")

        # Assert
        assert len(result) == 4
        assert "aggregated_value" in result.columns
        assert result["aggregated_value"].sum() == 2800
        assert result["aggregated_value"].iloc[0] == 1000  # First row (highest value)

    def test_with_top_n_parameter(self, sample_slice_data):
        """Test slice metrics calculation with top_n parameter."""
        # Act
        result = calculate_slice_metrics(
            sample_slice_data, "slice_value", "aggregated_value", top_n=2, other_label="Other"
        )

        # Assert
        assert len(result) == 3  # 2 top slices + "Other"
        assert "Other" in result["slice_value"].values
        assert result["aggregated_value"].sum() == 2800  # Total should remain the same

    def test_with_invalid_column(self, sample_slice_data):
        """Test with invalid column names."""
        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_slice_metrics(sample_slice_data, "invalid_column", "aggregated_value")

        with pytest.raises(ValidationError):
            calculate_slice_metrics(sample_slice_data, "slice_value", "invalid_column")

    def test_with_invalid_agg_func(self, sample_slice_data):
        """Test with invalid aggregation function."""
        # Act & Assert
        with pytest.raises(ValidationError):
            calculate_slice_metrics(sample_slice_data, "slice_value", "aggregated_value", agg_func="invalid_func")

    def test_with_empty_dataframe(self):
        """Test with empty DataFrame."""
        # Arrange
        empty_df = pd.DataFrame(columns=["slice_value", "aggregated_value"])

        # Act
        result = calculate_slice_metrics(empty_df, "slice_value", "aggregated_value")

        # Assert
        assert result.empty


class TestComputeSliceShares:
    """Tests for the compute_slice_shares function."""

    def test_normal_calculation(self, sample_slice_data):
        """Test basic share calculation."""
        # Act
        result = compute_slice_shares(sample_slice_data, "slice_value", "aggregated_value")

        # Assert
        assert "share_pct" in result.columns
        assert result["share_pct"].sum() == pytest.approx(100.0)
        assert result["share_pct"].iloc[0] == pytest.approx(1000 / 2800 * 100)  # First row share

    def test_custom_share_column_name(self, sample_slice_data):
        """Test with custom share column name."""
        # Act
        result = compute_slice_shares(
            sample_slice_data, "slice_value", "aggregated_value", share_col_name="custom_share"
        )

        # Assert
        assert "custom_share" in result.columns
        assert "share_pct" not in result.columns
        assert result["custom_share"].sum() == pytest.approx(100.0)

    def test_with_zero_total(self):
        """Test with all zero values (total = 0)."""
        # Arrange
        zero_df = pd.DataFrame({"slice_value": ["A", "B", "C"], "aggregated_value": [0, 0, 0]})

        # Act
        result = compute_slice_shares(zero_df, "slice_value", "aggregated_value")

        # Assert
        assert "share_pct" in result.columns
        assert result["share_pct"].sum() == 0  # All shares should be 0
        assert (result["share_pct"] == 0).all()

    def test_with_invalid_column(self, sample_slice_data):
        """Test with invalid column names."""
        # Act & Assert
        with pytest.raises(ValidationError):
            compute_slice_shares(sample_slice_data, "invalid_column", "aggregated_value")

        with pytest.raises(ValidationError):
            compute_slice_shares(sample_slice_data, "slice_value", "invalid_column")


class TestDifferenceFromAverage:
    """Tests for the difference_from_average function."""

    def test_normal_calculation(self, sample_slice_data):
        """Test basic difference from average calculation."""
        # Act
        result = difference_from_average(sample_slice_data, "aggregated_value")

        # Assert
        assert "avg_other_slices_value" in result.columns
        assert "absolute_diff_from_avg" in result.columns
        assert "absolute_diff_percent_from_avg" in result.columns

        # Check first row (North America: 1000)
        # Average of others = (800 + 600 + 400) / 3 = 600
        assert result["avg_other_slices_value"].iloc[0] == pytest.approx(600)
        assert result["absolute_diff_from_avg"].iloc[0] == pytest.approx(400)  # 1000 - 600
        assert result["absolute_diff_percent_from_avg"].iloc[0] == pytest.approx(66.67, abs=0.01)  # (400 / 600) * 100

    def test_with_single_slice(self):
        """Test with only one slice (can't calculate average of others)."""
        # Arrange
        single_df = pd.DataFrame({"slice_value": ["North America"], "aggregated_value": [1000]})

        # Act
        result = difference_from_average(single_df, "aggregated_value")

        # Assert
        assert pd.isna(result["avg_other_slices_value"].iloc[0])
        assert pd.isna(result["absolute_diff_from_avg"].iloc[0])
        assert pd.isna(result["absolute_diff_percent_from_avg"].iloc[0])

    def test_with_invalid_column(self, sample_slice_data):
        """Test with invalid column name."""
        # Act & Assert
        with pytest.raises(ValidationError):
            difference_from_average(sample_slice_data, "invalid_column")


class TestCompareDimensionSlicesOverTime:
    """Tests for the compare_dimension_slices_over_time function."""

    def test_normal_comparison(self, sample_time_series_data):
        """Test basic time comparison with two dates."""
        # Arrange
        prior_date = "2023-01-01"
        current_date = "2023-01-10"

        # Act
        result = compare_dimension_slices_over_time(
            sample_time_series_data,
            slice_col="slice_value",
            date_col="date",
            value_col="value",
            prior_start_date=prior_date,
            current_start_date=current_date,
            agg="sum",
        )

        # Assert
        assert "val_prior" in result.columns
        assert "val_current" in result.columns
        assert "abs_diff" in result.columns
        assert "pct_diff" in result.columns
        assert len(result) > 0

    def test_with_missing_dates(self, sample_time_series_data):
        """Test with dates not present in the data."""
        # Arrange
        prior_date = "2022-01-01"  # Not in data
        current_date = "2024-01-01"  # Not in data

        # Act & Assert
        with pytest.raises(ValidationError):
            compare_dimension_slices_over_time(
                sample_time_series_data,
                slice_col="slice_value",
                date_col="date",
                value_col="value",
                prior_start_date=prior_date,
                current_start_date=current_date,
            )

    def test_with_invalid_columns(self, sample_time_series_data):
        """Test with invalid column names."""
        # Arrange
        prior_date = "2023-01-01"
        current_date = "2023-01-10"

        # Act & Assert
        with pytest.raises(ValidationError):
            compare_dimension_slices_over_time(
                sample_time_series_data,
                slice_col="invalid_column",
                date_col="date",
                value_col="value",
                prior_start_date=prior_date,
                current_start_date=current_date,
            )


class TestCalculateConcentrationIndex:
    """Tests for the calculate_concentration_index function."""

    def test_hhi_calculation(self, sample_slice_data):
        """Test HHI concentration index calculation."""
        # Act
        result = calculate_concentration_index(
            sample_slice_data, val_col="aggregated_value", method=ConcentrationMethod.HHI
        )

        # Assert
        # HHI = sum of squared market shares (as decimals)
        # = (1000/2800)^2 + (800/2800)^2 + (600/2800)^2 + (400/2800)^2
        total = 2800
        expected_hhi = (1000 / total) ** 2 + (800 / total) ** 2 + (600 / total) ** 2 + (400 / total) ** 2
        assert result == pytest.approx(expected_hhi)

    def test_gini_calculation(self, sample_slice_data):
        """Test Gini coefficient calculation."""
        # Act
        result = calculate_concentration_index(
            sample_slice_data, val_col="aggregated_value", method=ConcentrationMethod.GINI
        )

        # Assert
        # Gini is between 0 and 1, where higher values indicate more inequality
        assert 0 <= result <= 1

    def test_with_all_equal_values(self):
        """Test with equal values (should give lowest concentration)."""
        # Arrange
        equal_df = pd.DataFrame({"slice_value": ["A", "B", "C", "D"], "aggregated_value": [100, 100, 100, 100]})

        # Act
        hhi_result = calculate_concentration_index(equal_df, "aggregated_value", ConcentrationMethod.HHI)
        gini_result = calculate_concentration_index(equal_df, "aggregated_value", ConcentrationMethod.GINI)

        # Assert
        # HHI for equal shares (0.25 each) = 4 * 0.25^2 = 0.25
        assert hhi_result == pytest.approx(0.25)
        # Gini for perfect equality = 0
        assert gini_result == pytest.approx(0.0)

    def test_with_single_value(self):
        """Test with a single value (highest concentration)."""
        # Arrange
        single_df = pd.DataFrame({"slice_value": ["A"], "aggregated_value": [100]})

        # Act
        hhi_result = calculate_concentration_index(single_df, "aggregated_value", ConcentrationMethod.HHI)
        gini_result = calculate_concentration_index(single_df, "aggregated_value", ConcentrationMethod.GINI)

        # Assert
        # HHI for monopoly (100% share) = 1.0
        assert hhi_result == pytest.approx(1.0)
        # Gini for single value is undefined, but should be 0 (perfect equality among 1 value)
        assert gini_result == pytest.approx(0.0)

    def test_with_negative_values_gini(self):
        """Test Gini with negative values (should raise error)."""
        # Arrange
        negative_df = pd.DataFrame({"slice_value": ["A", "B", "C"], "aggregated_value": [100, -50, 75]})

        # Act & Assert
        with pytest.raises(CalculationError):
            calculate_concentration_index(negative_df, "aggregated_value", ConcentrationMethod.GINI)

    def test_with_zero_sum(self):
        """Test with values summing to zero."""
        # Arrange
        zero_sum_df = pd.DataFrame({"slice_value": ["A", "B"], "aggregated_value": [0, 0]})

        # Act
        result = calculate_concentration_index(zero_sum_df, "aggregated_value", ConcentrationMethod.HHI)

        # Assert
        assert result == 0.0  # Default for undefined concentration


class TestIdentifyLargestSmallestByShare:
    """Tests for the identify_largest_smallest_by_share function."""

    def test_normal_identification(self):
        """Test identification of largest and smallest slices."""
        # Arrange
        current_df = pd.DataFrame({"slice_col": ["A", "B", "C", "D"], "share_pct": [40, 30, 20, 10]})

        prior_df = pd.DataFrame({"slice_col": ["A", "B", "C", "D"], "share_pct": [35, 35, 20, 10]})

        # Act
        largest, smallest = identify_largest_smallest_by_share(current_df, prior_df, "slice_col")

        # Assert
        assert largest.slice_value == "A"
        assert largest.current_share_of_volume_percent == 40
        assert largest.previous_slice_value == "A"  # A was also largest before
        assert largest.previous_share_percent == 35

        assert smallest.slice_value == "D"
        assert smallest.current_share_of_volume_percent == 10
        assert smallest.previous_slice_value == "D"
        assert smallest.previous_share_percent == 10

    def test_with_changed_positions(self):
        """Test when largest/smallest have changed between periods."""
        # Arrange
        current_df = pd.DataFrame({"slice_col": ["A", "B", "C", "D"], "share_pct": [40, 30, 20, 10]})

        # B was largest before, D was not smallest
        prior_df = pd.DataFrame({"slice_col": ["A", "B", "C", "D"], "share_pct": [30, 45, 15, 10]})

        # Act
        largest, smallest = identify_largest_smallest_by_share(current_df, prior_df, "slice_col")

        # Assert
        assert largest.slice_value == "A"  # Now A is largest
        assert largest.previous_slice_value == "B"  # But B was largest before

        assert smallest.slice_value == "D"
        assert smallest.previous_slice_value == "D"  # D was also smallest before

    def test_with_empty_dataframes(self):
        """Test with empty DataFrames."""
        # Arrange
        empty_df = pd.DataFrame(columns=["slice_col", "share_pct"])

        # Act
        largest, smallest = identify_largest_smallest_by_share(empty_df, empty_df, "slice_col")

        # Assert
        assert largest is None
        assert smallest is None


class TestHighlightSliceComparisons:
    """Tests for the highlight_slice_comparisons function."""

    def test_normal_comparison(self, merged_slice_data):
        """Test normal slice comparison highlighting."""
        # Act
        result = highlight_slice_comparisons(
            merged_slice_data, slice_col="slice_col", current_val_col="val_current", prior_val_col="val_prior", top_n=2
        )

        # Assert
        assert len(result) == 1  # Should compare top 2 slices (1 comparison)
        assert result[0].slice_a == "North America"
        assert result[0].slice_b == "Europe"
        assert result[0].current_value_a == 1000
        assert result[0].current_value_b == 800

        # Performance gap = (1000 - 800) / 800 * 100 = 25%
        assert result[0].performance_gap_percent == pytest.approx(25.0)

    def test_with_insufficient_slices(self):
        """Test with fewer slices than requested."""
        # Arrange
        single_df = pd.DataFrame({"slice_col": ["North America"], "val_current": [1000], "val_prior": [800]})

        # Act
        result = highlight_slice_comparisons(single_df, "slice_col", "val_current", "val_prior", top_n=2)

        # Assert
        assert len(result) == 0  # Not enough slices to compare

    def test_with_zero_values(self, merged_slice_data):
        """Test with zero values (potential division by zero)."""
        # Arrange
        test_df = merged_slice_data.copy()
        test_df.loc[1, "val_current"] = 0  # Set Europe current value to 0

        # Act
        result = highlight_slice_comparisons(test_df, "slice_col", "val_current", "val_prior", top_n=2)

        # Assert
        assert len(result) == 1
        assert result[0].performance_gap_percent is not None  # Should handle div by zero


class TestComputeHistoricalSliceRankings:
    """Tests for the compute_historical_slice_rankings function."""

    def test_with_sample_data(self, sample_time_series_data):
        """Test calculating historical rankings with sample data."""
        # Act
        result = compute_historical_slice_rankings(
            sample_time_series_data,
            slice_col="slice_value",
            date_col="date",
            value_col="value",
            num_periods=2,
            period_length_days=5,
            top_n=2,
        )

        # Assert
        assert result is not None
        assert result.periods_analyzed > 0
        assert len(result.period_rankings) > 0

        # First period should have rankings
        first_period = result.period_rankings[0]
        assert len(first_period.top_slices_by_performance) <= 2  # Up to 2 slices per period

    def test_with_empty_data(self):
        """Test with empty input data."""
        # Arrange
        empty_df = pd.DataFrame(columns=["slice_value", "date", "value"])

        # Act
        result = compute_historical_slice_rankings(empty_df, "slice_value", "date", "value", num_periods=2)

        # Assert
        assert result is None


class TestBuildSlicesPerformanceList:
    """Tests for the build_slices_performance_list function."""

    def test_normal_build(self, merged_slice_data):
        """Test building slice performance list from merged data."""
        # Act
        result = build_slices_performance_list(
            merged_slice_data,
            "slice_col",
            current_val_col="val_current",
            prior_val_col="val_prior",
            include_shares=True,
        )

        # Assert
        assert len(result) == 4  # 4 slices

        # Check first slice (North America)
        first_slice = result[0]
        assert first_slice.slice_value == "North America"
        assert first_slice.current_value == 1000
        assert first_slice.prior_value == 800
        assert first_slice.absolute_change == 200
        assert first_slice.current_share_of_volume_percent == 35.7
        assert first_slice.prior_share_of_volume_percent == 31.4
        assert first_slice.share_of_volume_change_percent == 4.3

    def test_with_missing_columns(self, merged_slice_data):
        """Test with missing required columns."""
        # Arrange
        test_df = merged_slice_data.drop(columns=["val_current"])

        # Act & Assert
        with pytest.raises(ValidationError):
            build_slices_performance_list(
                test_df, "slice_col", current_val_col="val_current", prior_val_col="val_prior"
            )

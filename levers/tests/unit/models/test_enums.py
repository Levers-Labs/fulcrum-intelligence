"""
Tests for enums.
"""

import pytest

from levers.models.enums import (
    AnomalyDetectionMethod,
    AverageGrowthMethod,
    ComparisonType,
    ConcentrationMethod,
    CumulativeGrowthMethod,
    DataFillMethod,
    DataSourceType,
    Granularity,
    GrowthTrend,
    MetricGVAStatus,
    PartialInterval,
    PeriodType,
    SmoothingMethod,
    TrendExceptionType,
    TrendType,
    WindowStrategy,
)


class TestGranularity:
    """Test Granularity enum."""

    def test_granularity_values(self):
        """Test all granularity values."""
        assert Granularity.DAY == "day"
        assert Granularity.WEEK == "week"
        assert Granularity.MONTH == "month"
        assert Granularity.QUARTER == "quarter"
        assert Granularity.YEAR == "year"

    def test_granularity_from_string(self):
        """Test creating Granularity from string."""
        assert Granularity("day") == Granularity.DAY
        assert Granularity("week") == Granularity.WEEK
        assert Granularity("month") == Granularity.MONTH
        assert Granularity("quarter") == Granularity.QUARTER
        assert Granularity("year") == Granularity.YEAR

    def test_invalid_granularity(self):
        """Test invalid granularity raises ValueError."""
        with pytest.raises(ValueError):
            Granularity("invalid")

    def test_granularity_iteration(self):
        """Test iterating over Granularity values."""
        values = list(Granularity)
        expected = [Granularity.DAY, Granularity.WEEK, Granularity.MONTH, Granularity.QUARTER, Granularity.YEAR]
        assert values == expected


class TestDataSourceType:
    """Test DataSourceType enum."""

    def test_data_source_type_values(self):
        """Test all data source type values."""
        assert DataSourceType.METRIC_TIME_SERIES == "metric_time_series"
        assert DataSourceType.METRIC_WITH_TARGETS == "metric_with_targets"
        assert DataSourceType.DIMENSIONAL_TIME_SERIES == "dimensional_time_series"
        assert DataSourceType.MULTI_METRIC == "multi_metric"

    def test_data_source_type_from_string(self):
        """Test creating DataSourceType from string."""
        assert DataSourceType("metric_time_series") == DataSourceType.METRIC_TIME_SERIES
        assert DataSourceType("dimensional_time_series") == DataSourceType.DIMENSIONAL_TIME_SERIES


class TestWindowStrategy:
    """Test WindowStrategy enum."""

    def test_window_strategy_values(self):
        """Test all window strategy values."""
        assert WindowStrategy.FIXED_TIME == "fixed_time"
        assert WindowStrategy.GRAIN_SPECIFIC_TIME == "grain_specific_time"
        assert WindowStrategy.FIXED_DATAPOINTS == "fixed_datapoints"


class TestGrowthTrend:
    """Test GrowthTrend enum."""

    def test_growth_trend_values(self):
        """Test all growth trend values."""
        assert GrowthTrend.STABLE == "stable"
        assert GrowthTrend.ACCELERATING == "accelerating"
        assert GrowthTrend.DECELERATING == "decelerating"
        assert GrowthTrend.VOLATILE == "volatile"


class TestTrendExceptionType:
    """Test TrendExceptionType enum."""

    def test_trend_exception_type_values(self):
        """Test all trend exception type values."""
        assert TrendExceptionType.SPIKE == "Spike"
        assert TrendExceptionType.DROP == "Drop"


class TestAnomalyDetectionMethod:
    """Test AnomalyDetectionMethod enum."""

    def test_anomaly_detection_method_values(self):
        """Test all anomaly detection method values."""
        assert AnomalyDetectionMethod.VARIANCE == "variance"
        assert AnomalyDetectionMethod.SPC == "spc"
        assert AnomalyDetectionMethod.COMBINED == "combined"


class TestTrendType:
    """Test TrendType enum."""

    def test_trend_type_values(self):
        """Test all trend type values."""
        assert TrendType.STABLE == "stable"
        assert TrendType.UPWARD == "upward"
        assert TrendType.DOWNWARD == "downward"
        assert TrendType.PLATEAU == "plateau"


class TestSmoothingMethod:
    """Test SmoothingMethod enum."""

    def test_smoothing_method_values(self):
        """Test all smoothing method values."""
        assert SmoothingMethod.LINEAR == "linear"
        assert SmoothingMethod.FRONT_LOADED == "front_loaded"
        assert SmoothingMethod.BACK_LOADED == "back_loaded"


class TestMetricGVAStatus:
    """Test MetricGVAStatus enum."""

    def test_metric_gva_status_values(self):
        """Test all metric GVA status values."""
        assert MetricGVAStatus.ON_TRACK == "on_track"
        assert MetricGVAStatus.OFF_TRACK == "off_track"
        assert MetricGVAStatus.NO_TARGET == "no_target"


class TestDataFillMethod:
    """Test DataFillMethod enum."""

    def test_data_fill_method_values(self):
        """Test all data fill method values."""
        assert DataFillMethod.FORWARD_FILL == "ffill"
        assert DataFillMethod.BACKWARD_FILL == "bfill"
        assert DataFillMethod.INTERPOLATE == "interpolate"


class TestAverageGrowthMethod:
    """Test AverageGrowthMethod enum."""

    def test_average_growth_method_values(self):
        """Test all average growth method values."""
        assert AverageGrowthMethod.ARITHMETIC == "arithmetic"
        assert AverageGrowthMethod.CAGR == "cagr"


class TestPartialInterval:
    """Test PartialInterval enum."""

    def test_partial_interval_values(self):
        """Test all partial interval values."""
        assert PartialInterval.MTD == "MTD"
        assert PartialInterval.QTD == "QTD"
        assert PartialInterval.YTD == "YTD"
        assert PartialInterval.WTD == "WTD"


class TestCumulativeGrowthMethod:
    """Test CumulativeGrowthMethod enum."""

    def test_cumulative_growth_method_values(self):
        """Test all cumulative growth method values."""
        assert CumulativeGrowthMethod.INDEX == "index"
        assert CumulativeGrowthMethod.CUMSUM == "cumsum"
        assert CumulativeGrowthMethod.CUMPROD == "cumprod"


class TestConcentrationMethod:
    """Test ConcentrationMethod enum."""

    def test_concentration_method_values(self):
        """Test all concentration method values."""
        assert ConcentrationMethod.GINI == "GINI"
        assert ConcentrationMethod.HHI == "HHI"


class TestComparisonType:
    """Test ComparisonType enum."""

    def test_comparison_type_values(self):
        """Test all comparison type values."""
        # Week-based comparisons
        assert ComparisonType.LAST_WEEK == "last_week"
        assert ComparisonType.WEEK_IN_LAST_MONTH == "week_in_last_month"
        assert ComparisonType.WEEK_IN_LAST_QUARTER == "week_in_last_quarter"
        assert ComparisonType.WEEK_IN_LAST_YEAR == "week_in_last_year"

        # Month-based comparisons
        assert ComparisonType.LAST_MONTH == "last_month"
        assert ComparisonType.MONTH_IN_LAST_QUARTER == "month_in_last_quarter"
        assert ComparisonType.MONTH_IN_LAST_YEAR == "month_in_last_year"


class TestPeriodName:
    """Test PeriodType enum."""

    def test_period_name_values(self):
        """Test all period name values."""
        assert PeriodType.END_OF_WEEK == "END_OF_WEEK"
        assert PeriodType.END_OF_MONTH == "END_OF_MONTH"
        assert PeriodType.END_OF_QUARTER == "END_OF_QUARTER"
        assert PeriodType.END_OF_NEXT_MONTH == "END_OF_NEXT_MONTH"

    def test_period_name_from_string(self):
        """Test creating PeriodType from string."""
        assert PeriodType("END_OF_WEEK") == PeriodType.END_OF_WEEK
        assert PeriodType("END_OF_MONTH") == PeriodType.END_OF_MONTH
        assert PeriodType("END_OF_QUARTER") == PeriodType.END_OF_QUARTER
        assert PeriodType("END_OF_NEXT_MONTH") == PeriodType.END_OF_NEXT_MONTH

    def test_invalid_period_name(self):
        """Test invalid period name raises ValueError."""
        with pytest.raises(ValueError):
            PeriodType("END_OF_DECADE")

    def test_period_name_iteration(self):
        """Test iterating over PeriodType values."""
        values = list(PeriodType)
        expected = [
            PeriodType.END_OF_WEEK,
            PeriodType.END_OF_MONTH,
            PeriodType.END_OF_QUARTER,
            PeriodType.END_OF_YEAR,
            PeriodType.END_OF_NEXT_MONTH,
        ]
        assert values == expected


class TestEnumInteroperability:
    """Test interoperability between enums."""

    def test_enum_comparison(self):
        """Test enum comparison works correctly."""
        assert Granularity.DAY == "day"
        assert Granularity.DAY != "week"
        assert Granularity.DAY != Granularity.WEEK

    def test_enum_in_containers(self):
        """Test enums work correctly in containers."""
        granularities = {Granularity.DAY, Granularity.WEEK}
        assert Granularity.DAY in granularities
        assert Granularity.MONTH not in granularities

        # Test with strings
        assert "day" in [g.value for g in granularities]

    def test_enum_sorting(self):
        """Test enums can be sorted."""
        periods = [PeriodType.END_OF_QUARTER, PeriodType.END_OF_WEEK, PeriodType.END_OF_MONTH]
        sorted_periods = sorted(periods, key=lambda x: x.value)

        # Should be sorted alphabetically by value
        expected = [PeriodType.END_OF_MONTH, PeriodType.END_OF_QUARTER, PeriodType.END_OF_WEEK]
        assert sorted_periods == expected

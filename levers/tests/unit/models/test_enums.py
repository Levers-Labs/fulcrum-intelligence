"""
Unit tests for enum models used across patterns.
"""

from levers.models import (
    AnomalyDetectionMethod,
    AverageGrowthMethod,
    CumulativeGrowthMethod,
    DataFillMethod,
    DataSourceType,
    Granularity,
    GrowthTrend,
    MetricGVAStatus,
    PartialInterval,
    SmoothingMethod,
    TrendExceptionType,
    TrendType,
    WindowStrategy,
)


class TestGranularity:
    """Tests for the Granularity enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert Granularity.DAY == "day"
        assert Granularity.WEEK == "week"
        assert Granularity.MONTH == "month"
        assert Granularity.QUARTER == "quarter"
        assert Granularity.YEAR == "year"

    def test_string_comparison(self):
        """Test string comparison."""
        assert Granularity.DAY == "day"
        assert "day" == Granularity.DAY
        assert Granularity.WEEK == "week"
        assert Granularity.MONTH == "month"
        assert Granularity.QUARTER == "quarter"
        assert Granularity.YEAR == "year"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"day", "week", "month", "quarter", "year"}
        enum_values = {g.value for g in Granularity}
        assert enum_values == expected_values

    def test_string_conversion(self):
        """Test string conversion."""
        assert str(Granularity.DAY) == "Granularity.DAY"
        assert str(Granularity.WEEK) == "Granularity.WEEK"
        assert str(Granularity.MONTH) == "Granularity.MONTH"
        assert str(Granularity.QUARTER) == "Granularity.QUARTER"
        assert str(Granularity.YEAR) == "Granularity.YEAR"

        assert Granularity.DAY.value == "day"
        assert Granularity.WEEK.value == "week"
        assert Granularity.MONTH.value == "month"
        assert Granularity.QUARTER.value == "quarter"
        assert Granularity.YEAR.value == "year"


class TestDataSourceType:
    """Tests for the DataSourceType enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert DataSourceType.METRIC_TIME_SERIES == "metric_time_series"
        assert DataSourceType.METRIC_WITH_TARGETS == "metric_with_targets"
        assert DataSourceType.DIMENSIONAL_TIME_SERIES == "dimensional_time_series"
        assert DataSourceType.MULTI_METRIC == "multi_metric"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"metric_time_series", "metric_with_targets", "dimensional_time_series", "multi_metric"}
        enum_values = {ds.value for ds in DataSourceType}
        assert enum_values == expected_values


class TestWindowStrategy:
    """Tests for the WindowStrategy enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert WindowStrategy.FIXED_TIME == "fixed_time"
        assert WindowStrategy.GRAIN_SPECIFIC_TIME == "grain_specific_time"
        assert WindowStrategy.FIXED_DATAPOINTS == "fixed_datapoints"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"fixed_time", "grain_specific_time", "fixed_datapoints"}
        enum_values = {ws.value for ws in WindowStrategy}
        assert enum_values == expected_values


class TestGrowthTrend:
    """Tests for the GrowthTrend enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert GrowthTrend.STABLE == "stable"
        assert GrowthTrend.ACCELERATING == "accelerating"
        assert GrowthTrend.DECELERATING == "decelerating"
        assert GrowthTrend.VOLATILE == "volatile"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"stable", "accelerating", "decelerating", "volatile"}
        enum_values = {gt.value for gt in GrowthTrend}
        assert enum_values == expected_values


class TestTrendExceptionType:
    """Tests for the TrendExceptionType enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert TrendExceptionType.SPIKE == "Spike"
        assert TrendExceptionType.DROP == "Drop"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"Spike", "Drop"}
        enum_values = {tet.value for tet in TrendExceptionType}
        assert enum_values == expected_values


class TestAnomalyDetectionMethod:
    """Tests for the AnomalyDetectionMethod enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert AnomalyDetectionMethod.VARIANCE == "variance"
        assert AnomalyDetectionMethod.SPC == "spc"
        assert AnomalyDetectionMethod.COMBINED == "combined"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"variance", "spc", "combined"}
        enum_values = {adm.value for adm in AnomalyDetectionMethod}
        assert enum_values == expected_values


class TestTrendType:
    """Tests for the TrendType enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert TrendType.STABLE == "stable"
        assert TrendType.UPWARD == "upward"
        assert TrendType.DOWNWARD == "downward"
        assert TrendType.PLATEAU == "plateau"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"stable", "upward", "downward", "plateau"}
        enum_values = {tt.value for tt in TrendType}
        assert enum_values == expected_values


class TestSmoothingMethod:
    """Tests for the SmoothingMethod enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert SmoothingMethod.LINEAR == "linear"
        assert SmoothingMethod.FRONT_LOADED == "front_loaded"
        assert SmoothingMethod.BACK_LOADED == "back_loaded"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"linear", "front_loaded", "back_loaded"}
        enum_values = {sm.value for sm in SmoothingMethod}
        assert enum_values == expected_values


class TestMetricGVAStatus:
    """Tests for the MetricGVAStatus enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert MetricGVAStatus.ON_TRACK == "on_track"
        assert MetricGVAStatus.OFF_TRACK == "off_track"
        assert MetricGVAStatus.NO_TARGET == "no_target"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"on_track", "off_track", "no_target"}
        enum_values = {mgva.value for mgva in MetricGVAStatus}
        assert enum_values == expected_values


class TestDataFillMethod:
    """Tests for the DataFillMethod enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert DataFillMethod.FORWARD_FILL == "ffill"
        assert DataFillMethod.BACKWARD_FILL == "bfill"
        assert DataFillMethod.INTERPOLATE == "interpolate"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"ffill", "bfill", "interpolate"}
        enum_values = {dfm.value for dfm in DataFillMethod}
        assert enum_values == expected_values


class TestAverageGrowthMethod:
    """Tests for the AverageGrowthMethod enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert AverageGrowthMethod.ARITHMETIC == "arithmetic"
        assert AverageGrowthMethod.CAGR == "cagr"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"arithmetic", "cagr"}
        enum_values = {agm.value for agm in AverageGrowthMethod}
        assert enum_values == expected_values


class TestPartialInterval:
    """Tests for the PartialInterval enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert PartialInterval.MTD == "MTD"
        assert PartialInterval.QTD == "QTD"
        assert PartialInterval.YTD == "YTD"
        assert PartialInterval.WTD == "WTD"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"MTD", "QTD", "YTD", "WTD"}
        enum_values = {pi.value for pi in PartialInterval}
        assert enum_values == expected_values


class TestCumulativeGrowthMethod:
    """Tests for the CumulativeGrowthMethod enum."""

    def test_enum_values(self):
        """Test the enum values."""
        assert CumulativeGrowthMethod.INDEX == "index"
        assert CumulativeGrowthMethod.CUMSUM == "cumsum"
        assert CumulativeGrowthMethod.CUMPROD == "cumprod"

    def test_iteration(self):
        """Test iteration over enum values."""
        expected_values = {"index", "cumsum", "cumprod"}
        enum_values = {cgm.value for cgm in CumulativeGrowthMethod}
        assert enum_values == expected_values

"""
Unit tests for the DimensionAnalysisPattern class.
"""

import datetime

import pandas as pd
import pytest

from levers.models import AnalysisWindow, Granularity
from levers.models.patterns import DimensionAnalysis
from levers.patterns import DimensionAnalysisPattern


class TestDimensionAnalysisPattern:
    """Tests for the DimensionAnalysisPattern class."""

    @pytest.fixture
    def pattern(self):
        """Return a DimensionAnalysisPattern instance."""
        return DimensionAnalysisPattern()

    @pytest.fixture
    def sample_data(self):
        """Return sample dimensional data for testing."""
        # Create sample data with date, dimension, slice_value, and metric_value columns
        data = []

        # Generate data for past 60 days with 4 slices
        for day in range(60):
            date = pd.Timestamp("2023-01-01") + pd.Timedelta(days=day)

            # North America data (consistently high values)
            data.append(
                {
                    "date": date,
                    "dimension_name": "region",
                    "dimension_slice": "North America",
                    "value": 1000 + day * 10,  # Growing trend
                    "metric_id": "revenue",
                }
            )

            # Europe data (medium values)
            data.append(
                {
                    "date": date,
                    "dimension_name": "region",
                    "dimension_slice": "Europe",
                    "value": 800 + day * 5,  # Slower growth
                    "metric_id": "revenue",
                }
            )

            # APAC data (values dip in the middle)
            trend_factor = 1 if day < 30 else 2  # Faster growth after day 30
            data.append(
                {
                    "date": date,
                    "dimension_name": "region",
                    "dimension_slice": "APAC",
                    "value": 600 + day * trend_factor,
                    "metric_id": "revenue",
                }
            )

            # LATAM data (values decline then recover)
            decline = max(0, 30 - day) * 5  # Declining for first 30 days
            recovery = max(0, day - 30) * 10  # Recovering after day 30
            data.append(
                {
                    "date": date,
                    "dimension_name": "region",
                    "dimension_slice": "LATAM",
                    "value": 400 - decline + recovery,
                    "metric_id": "revenue",
                }
            )

        return pd.DataFrame(data)

    @pytest.fixture
    def analysis_window(self):
        """Return an AnalysisWindow for testing."""
        return AnalysisWindow(start_date="2023-02-15", end_date="2023-02-28", grain=Granularity.DAY)

    def test_pattern_attributes(self, pattern):
        """Test pattern class attributes."""
        assert pattern.name == "dimension_analysis"
        assert pattern.version == "1.0.0"
        assert pattern.description is not None
        assert pattern.output_model == DimensionAnalysis
        assert len(pattern.required_primitives) > 0
        assert "compute_slice_shares" in pattern.required_primitives
        assert "difference_from_average" in pattern.required_primitives

    def test_default_config(self, pattern):
        """Test the default configuration."""
        config = pattern.get_default_config()

        assert config.pattern_name == "dimension_analysis"
        assert config.version == "1.0.0"
        assert len(config.data_sources) == 1
        assert config.data_sources[0].data_key == "data"
        assert config.analysis_window.days == 180

    def test_analyze_basic_functionality(self, pattern, sample_data, analysis_window, mocker):
        """Test the basic analyze functionality with sample data."""
        # Arrange
        metric_id = "revenue"
        dimension_name = "region"

        # Patch some of the used primitives to ensure they're called
        mocker.patch(
            "levers.patterns.dimension_analysis.compare_dimension_slices_over_time",
            return_value=pd.DataFrame(
                {
                    "slice_value": ["North America", "Europe", "APAC", "LATAM"],
                    "val_current": [1500, 1200, 900, 500],
                    "val_prior": [1400, 1100, 800, 450],
                    "abs_diff": [100, 100, 100, 50],
                    "pct_diff": [7.14, 9.09, 12.5, 11.11],
                }
            ),
        )

        # Let the _compute_slice_shares method pass through to implementation
        # but spy on it to verify it was called
        compute_shares_spy = mocker.spy(pattern, "_compute_slice_shares")

        # Mock a few of the more complex primitives
        mocker.patch(
            "levers.patterns.dimension_analysis.difference_from_average",
            return_value=pd.DataFrame(
                {
                    "slice_value": ["North America", "Europe", "APAC", "LATAM"],
                    "val_current": [1500, 1200, 900, 500],
                    "val_prior": [1400, 1100, 800, 450],
                    "avg_other_slices_value": [866.67, 966.67, 1066.67, 1200],
                    "absolute_diff_from_avg": [633.33, 233.33, -166.67, -700],
                    "absolute_diff_percent_from_avg": [73.08, 24.14, -15.63, -58.33],
                }
            ),
        )

        mocker.patch(
            "levers.patterns.dimension_analysis.build_slices_performance_list",
            return_value=[
                {
                    "slice_value": "North America",
                    "current_value": 1500,
                    "prior_value": 1400,
                    "absolute_change": 100,
                    "relative_change_percent": 7.14,
                }
            ],
        )

        mocker.patch(
            "levers.patterns.dimension_analysis.compute_top_bottom_slices",
            return_value=(
                [{"dimension": "region", "slice_value": "North America", "metric_value": 1500, "rank": 1}],
                [{"dimension": "region", "slice_value": "LATAM", "metric_value": 500, "rank": 4}],
            ),
        )

        mocker.patch(
            "levers.patterns.dimension_analysis.compute_historical_slice_rankings",
            return_value=[
                {
                    "start_date": "2023-02-15",
                    "end_date": "2023-02-21",
                    "top_slices_by_performance": [
                        {"dimension": "region", "slice_value": "North America", "metric_value": 1500}
                    ],
                }
            ],
        )

        # Act
        result = pattern.analyze(
            metric_id=metric_id,
            data=sample_data,
            analysis_date=datetime.date(2023, 2, 28),
            dimension_name=dimension_name,
            analysis_window=analysis_window,
        )

        # Assert
        assert result is not None
        assert result.pattern == "dimension_analysis"
        assert result.metric_id == metric_id
        assert result.dimension_name == dimension_name
        assert result.slices is not None
        assert result.top_slices is not None
        assert result.bottom_slices is not None
        assert compute_shares_spy.called  # Verify our method was called

    def test_analyze_with_empty_data(self, pattern, analysis_window):
        """Test the analyze method with empty data."""
        # Arrange
        empty_data = pd.DataFrame()
        metric_id = "revenue"

        # Should raise InsufficientDataError for empty data
        from levers.exceptions import InsufficientDataError

        with pytest.raises(InsufficientDataError):
            pattern.analyze(
                metric_id=metric_id,
                data=empty_data,
                analysis_date=datetime.date(2023, 1, 1),
                dimension_name="test",
                analysis_window=analysis_window,
            )

    def test_analyze_with_missing_dimensions(self, pattern, sample_data, analysis_window):
        """Test analyze with a metric that doesn't have the requested dimension."""
        # Arrange
        metric_id = "revenue"
        dimension_name = "nonexistent_dimension"  # This dimension doesn't exist in the data

        # Act
        result = pattern.analyze(
            metric_id=metric_id,
            data=sample_data,
            analysis_date=datetime.date(2023, 1, 1),
            dimension_name=dimension_name,
            analysis_window=analysis_window,
        )

        # Assert
        assert result is not None
        assert result.pattern == "dimension_analysis"
        assert result.metric_id == metric_id
        assert len(result.slices) == 0
        assert len(result.top_slices) == 0
        assert len(result.bottom_slices) == 0

    def test_analyze_with_different_grain(self, pattern, sample_data):
        """Test analyze with a different grain (weekly)."""
        # Arrange
        metric_id = "revenue"
        dimension_name = "region"
        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-02-28", grain=Granularity.WEEK)

        # Act
        result = pattern.analyze(
            metric_id=metric_id,
            data=sample_data,
            analysis_date=datetime.date(2023, 2, 1),
            dimension_name=dimension_name,
            analysis_window=analysis_window,
        )

        # Assert
        assert result is not None
        assert result.pattern == "dimension_analysis"
        assert result.metric_id == metric_id
        assert result.analysis_window.grain == Granularity.WEEK

    def test_analyze_with_invalid_data(self, pattern, analysis_window):
        """Test analyze with invalid data (missing required columns)."""
        # Arrange
        invalid_data = pd.DataFrame(
            {
                "date": pd.date_range(start="2023-01-01", periods=10),
                # Missing required columns (dimension_name, dimension_slice, value)
                "some_other_column": range(10),
                "metric_id": ["revenue"] * 10,
            }
        )
        metric_id = "revenue"

        # Act & Assert - Should raise MissingDataError for missing required columns
        from levers.exceptions import MissingDataError

        with pytest.raises(MissingDataError):
            pattern.analyze(
                metric_id=metric_id,
                data=invalid_data,
                analysis_date=datetime.date(2023, 1, 1),
                dimension_name="region",
                analysis_window=analysis_window,
            )

    # def test_analyze_with_nonexistent_metric(self, pattern, sample_data, analysis_window):
    #     """Test analyze with a metric ID that doesn't exist in the data."""
    #     # Arrange
    #     metric_id = "nonexistent_metric"  # This metric doesn't exist in sample_data
    #     dimension_name = "region"

    #     # Act
    #     result = pattern.analyze(
    #         metric_id=metric_id,
    #         data=sample_data,
    #         analysis_window=analysis_window,
    #         dimension_name=dimension_name
    #     )

    #     # Assert
    #     assert result is not None
    #     assert result.pattern == "dimension_analysis"
    #     assert result.metric_id == metric_id
    #     assert result.dimension_name == dimension_name
    #     assert len(result.slices) == 0

    def test_compute_slice_shares_method(self, pattern):
        """Test the _compute_slice_shares helper method directly."""
        # Arrange
        compare_df = pd.DataFrame(
            {
                "slice_value": ["North America", "Europe", "APAC", "LATAM"],
                "val_current": [1500, 1200, 900, 500],
                "val_prior": [1400, 1100, 800, 450],
            }
        )

        # Act
        prior_df, current_df, merged = pattern._compute_slice_shares(compare_df)

        # Assert
        # Check prior_df
        assert "share_pct_prior" in prior_df.columns
        assert prior_df["share_pct_prior"].sum() == pytest.approx(100.0)

        # Check current_df
        assert "share_pct_current" in current_df.columns
        assert current_df["share_pct_current"].sum() == pytest.approx(100.0)

        # Check merged
        assert "slice_value" in merged.columns
        assert "share_pct_prior" in merged.columns
        assert "share_pct_current" in merged.columns
        assert "share_diff" in merged.columns

        # Check sample values (North America)
        na_row = merged[merged["slice_value"] == "North America"]
        assert na_row["share_pct_current"].iloc[0] == pytest.approx(1500 / 4100 * 100)  # 36.59% of current
        assert na_row["share_pct_prior"].iloc[0] == pytest.approx(1400 / 3750 * 100)  # 37.33% of prior
        assert na_row["share_diff"].iloc[0] == pytest.approx(-0.74, abs=0.01)  # Slight decrease in share

    def test_validate_output(self, pattern, analysis_window):
        """Test that the output validation produces a proper model."""
        # Arrange
        result_dict = {
            "pattern": "dimension_analysis",
            "version": "1.0.0",
            "metric_id": "revenue",
            "analysis_window": analysis_window,
            "dimension_name": "region",
            "slices": [
                {
                    "slice_value": "North America",
                    "current_value": 1500,
                    "prior_value": 1400,
                    "absolute_change": 100,
                    "relative_change_percent": 7.14,
                }
            ],
            "top_slices": [{"dimension": "region", "slice_value": "North America", "metric_value": 1500, "rank": 1}],
            "bottom_slices": [{"dimension": "region", "slice_value": "LATAM", "metric_value": 500, "rank": 4}],
            "largest_slice": None,
            "smallest_slice": None,
            "strongest_slice": None,
            "weakest_slice": None,
            "comparison_highlights": [],
            "historical_slice_rankings": [],
        }

        # Act
        result = pattern.validate_output(result_dict)

        # Assert
        assert isinstance(result, DimensionAnalysis)
        assert result.pattern == "dimension_analysis"
        assert result.metric_id == "revenue"
        assert len(result.slices) == 1
        assert len(result.top_slices) == 1
        assert len(result.bottom_slices) == 1
        assert result.largest_slice is None
        assert result.smallest_slice is None

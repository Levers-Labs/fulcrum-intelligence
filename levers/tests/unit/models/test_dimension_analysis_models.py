"""
Unit tests for dimension analysis models.
"""

import pytest
from pydantic import ValidationError

from levers.models import (
    AnalysisWindow,
    Granularity,
    HistoricalPeriodRanking,
    SliceComparison,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
    TopSlice,
)
from levers.models.patterns import DimensionAnalysis


class TestSliceModels:
    """Tests for slice-related models."""

    def test_slice_ranking_model(self):
        """Test the SliceRanking model."""
        # Arrange & Act
        model = SliceRanking(
            dimension="region",
            slice_value="North America",
            metric_value=1500.0,
            rank=1,
            avg_other_slices_value=800.0,
            absolute_diff_from_avg=700.0,
            absolute_diff_percent_from_avg=87.5,
        )

        # Assert
        assert model.dimension == "region"
        assert model.slice_value == "North America"
        assert model.metric_value == 1500.0
        assert model.rank == 1
        assert model.avg_other_slices_value == 800.0
        assert model.absolute_diff_from_avg == 700.0
        assert model.absolute_diff_percent_from_avg == 87.5

    def test_slice_ranking_model_validation(self):
        """Test validation in the SliceRanking model."""
        # Arrange & Act & Assert
        # Missing required fields
        with pytest.raises(ValidationError):
            SliceRanking(
                dimension="region",
                slice_value="North America",
                # Missing metric_value
                rank=1,
            )

        # Wrong type
        with pytest.raises(ValidationError):
            SliceRanking(
                dimension="region",
                slice_value="North America",
                metric_value="not a number",  # Should be a number
                rank=1,
            )

    def test_slice_performance_model(self):
        """Test the SlicePerformance model."""
        # Arrange & Act
        model = SlicePerformance(
            slice_value="North America",
            current_value=1500.0,
            prior_value=1400.0,
            absolute_change=100.0,
            relative_change_percent=7.14,
            current_share_of_volume_percent=36.59,
            prior_share_of_volume_percent=37.33,
            share_of_volume_change_percent=-0.74,
            absolute_marginal_impact=100.0,
            relative_marginal_impact_percent=28.57,
            avg_other_slices_value=866.67,
            absolute_diff_from_avg=633.33,
            absolute_diff_percent_from_avg=73.08,
            consecutive_above_avg_streak=10,
            rank_by_performance=1,
            rank_by_share=1,
        )

        # Assert
        assert model.slice_value == "North America"
        assert model.current_value == 1500.0
        assert model.prior_value == 1400.0
        assert model.absolute_change == 100.0
        assert model.relative_change_percent == 7.14
        assert model.current_share_of_volume_percent == 36.59
        assert model.prior_share_of_volume_percent == 37.33
        assert model.share_of_volume_change_percent == -0.74
        assert model.rank_by_performance == 1
        assert model.rank_by_share == 1

    def test_slice_performance_model_with_minimal_fields(self):
        """Test the SlicePerformance model with only required fields."""
        # Arrange & Act
        model = SlicePerformance(slice_value="North America", current_value=1500.0, prior_value=1400.0)

        # Assert
        assert model.slice_value == "North America"
        assert model.current_value == 1500.0
        assert model.prior_value == 1400.0
        assert model.absolute_change is None
        assert model.relative_change_percent is None
        assert model.current_share_of_volume_percent is None
        assert model.prior_share_of_volume_percent is None
        assert model.rank_by_performance is None

    def test_slice_share_model(self):
        """Test the SliceShare model."""
        # Arrange & Act
        model = SliceShare(
            slice_value="North America",
            current_share_of_volume_percent=36.59,
            previous_slice_value="Europe",
            previous_share_percent=37.33,
        )

        # Assert
        assert model.slice_value == "North America"
        assert model.current_share_of_volume_percent == 36.59
        assert model.previous_slice_value == "Europe"
        assert model.previous_share_percent == 37.33

    def test_slice_strength_model(self):
        """Test the SliceStrength model."""
        # Arrange & Act
        model = SliceStrength(
            slice_value="North America",
            previous_slice_value="Europe",
            current_value=1500.0,
            prior_value=1400.0,
            absolute_delta=100.0,
            relative_delta_percent=7.14,
        )

        # Assert
        assert model.slice_value == "North America"
        assert model.previous_slice_value == "Europe"
        assert model.current_value == 1500.0
        assert model.prior_value == 1400.0
        assert model.absolute_delta == 100.0
        assert model.relative_delta_percent == 7.14

    def test_slice_comparison_model(self):
        """Test the SliceComparison model."""
        # Arrange & Act
        model = SliceComparison(
            slice_a="North America",
            current_value_a=1500.0,
            prior_value_a=1400.0,
            slice_b="Europe",
            current_value_b=1200.0,
            prior_value_b=1100.0,
            performance_gap_percent=25.0,
            gap_change_percent=2.27,
        )

        # Assert
        assert model.slice_a == "North America"
        assert model.current_value_a == 1500.0
        assert model.prior_value_a == 1400.0
        assert model.slice_b == "Europe"
        assert model.current_value_b == 1200.0
        assert model.prior_value_b == 1100.0
        assert model.performance_gap_percent == 25.0
        assert model.gap_change_percent == 2.27

    def test_top_slice_model(self):
        """Test the TopSlice model."""
        # Arrange & Act
        model = TopSlice(dimension="region", slice_value="North America", metric_value=1500.0)

        # Assert
        assert model.dimension == "region"
        assert model.slice_value == "North America"
        assert model.metric_value == 1500.0


class TestHistoricalRankingsModels:
    """Tests for historical rankings models."""

    def test_historical_period_ranking_model(self):
        """Test the HistoricalPeriodRanking model."""
        # Arrange & Act
        model = HistoricalPeriodRanking(
            start_date="2023-01-01",
            end_date="2023-01-07",
            top_slices_by_performance=[
                TopSlice(dimension="region", slice_value="North America", metric_value=1500.0),
                TopSlice(dimension="region", slice_value="Europe", metric_value=1200.0),
            ],
        )

        # Assert
        assert model.start_date == "2023-01-01"
        assert model.end_date == "2023-01-07"
        assert len(model.top_slices_by_performance) == 2
        assert model.top_slices_by_performance[0].slice_value == "North America"
        assert model.top_slices_by_performance[1].slice_value == "Europe"

    def test_historical_slice_rankings_model(self):
        """Test the HistoricalSliceRankings model."""
        # Arrange & Act
        model = [
            HistoricalPeriodRanking(
                start_date="2023-01-01",
                end_date="2023-01-07",
                top_slices_by_performance=[
                    TopSlice(dimension="region", slice_value="North America", metric_value=1500.0)
                ],
            ),
            HistoricalPeriodRanking(
                start_date="2023-01-08",
                end_date="2023-01-14",
                top_slices_by_performance=[TopSlice(dimension="region", slice_value="Europe", metric_value=1600.0)],
            ),
        ]

        # Assert
        assert len(model) == 2
        assert model[0].start_date == "2023-01-01"
        assert model[1].end_date == "2023-01-14"
        assert model[0].top_slices_by_performance[0].slice_value == "North America"
        assert model[1].top_slices_by_performance[0].slice_value == "Europe"


class TestDimensionAnalysisModel:
    """Tests for the DimensionAnalysis model."""

    @pytest.fixture
    def sample_dimension_analysis(self):
        """Return a sample DimensionAnalysis instance."""

        analysis_window = AnalysisWindow(start_date="2023-01-01", end_date="2023-01-31", grain=Granularity.DAY)

        return DimensionAnalysis(
            pattern="dimension_analysis",
            version="1.0.0",
            metric_id="revenue",
            analysis_window=analysis_window,
            dimension_name="region",
            slices=[
                SlicePerformance(
                    slice_value="North America",
                    current_value=1500.0,
                    prior_value=1400.0,
                    absolute_change=100.0,
                    relative_change_percent=7.14,
                )
            ],
            top_slices=[SliceRanking(dimension="region", slice_value="North America", metric_value=1500.0, rank=1)],
            bottom_slices=[SliceRanking(dimension="region", slice_value="LATAM", metric_value=500.0, rank=4)],
            comparison_highlights=[
                SliceComparison(
                    slice_a="North America",
                    current_value_a=1500.0,
                    prior_value_a=1400.0,
                    slice_b="Europe",
                    current_value_b=1200.0,
                    prior_value_b=1100.0,
                    performance_gap_percent=25.0,
                    gap_change_percent=2.27,
                )
            ],
        )

    def test_dimension_analysis_model(self, sample_dimension_analysis):
        """Test the DimensionAnalysis model."""
        # Assert
        assert sample_dimension_analysis.pattern == "dimension_analysis"
        assert sample_dimension_analysis.version == "1.0.0"
        assert sample_dimension_analysis.metric_id == "revenue"
        assert sample_dimension_analysis.dimension_name == "region"
        assert len(sample_dimension_analysis.slices) == 1
        assert len(sample_dimension_analysis.top_slices) == 1
        assert len(sample_dimension_analysis.bottom_slices) == 1
        assert sample_dimension_analysis.largest_slice is None
        assert sample_dimension_analysis.smallest_slice is None
        assert sample_dimension_analysis.strongest_slice is None
        assert sample_dimension_analysis.weakest_slice is None
        assert len(sample_dimension_analysis.comparison_highlights) == 1
        assert len(sample_dimension_analysis.historical_slice_rankings) == 0

    def test_dimension_analysis_model_with_optional_fields(self, sample_dimension_analysis):
        """Test the DimensionAnalysis model with optional fields."""
        # Arrange
        sample_dimension_analysis.largest_slice = SliceShare(
            slice_value="North America",
            current_share_of_volume_percent=36.59,
            previous_slice_value="North America",
            previous_share_percent=37.33,
        )

        sample_dimension_analysis.smallest_slice = SliceShare(
            slice_value="LATAM",
            current_share_of_volume_percent=12.2,
            previous_slice_value="LATAM",
            previous_share_percent=12.0,
        )

        sample_dimension_analysis.strongest_slice = SliceStrength(
            slice_value="North America",
            previous_slice_value="Europe",
            current_value=1500.0,
            prior_value=1400.0,
            absolute_delta=100.0,
            relative_delta_percent=7.14,
        )

        sample_dimension_analysis.weakest_slice = SliceStrength(
            slice_value="LATAM",
            previous_slice_value="LATAM",
            current_value=500.0,
            prior_value=450.0,
            absolute_delta=50.0,
            relative_delta_percent=11.11,
        )

        sample_dimension_analysis.historical_slice_rankings = [
            HistoricalPeriodRanking(
                start_date="2023-01-01",
                end_date="2023-01-07",
                top_slices_by_performance=[
                    TopSlice(dimension="region", slice_value="North America", metric_value=1500.0)
                ],
            )
        ]

        # Assert
        assert sample_dimension_analysis.largest_slice.slice_value == "North America"
        assert sample_dimension_analysis.smallest_slice.slice_value == "LATAM"
        assert sample_dimension_analysis.strongest_slice.slice_value == "North America"
        assert sample_dimension_analysis.weakest_slice.slice_value == "LATAM"
        assert sample_dimension_analysis.historical_slice_rankings
        assert len(sample_dimension_analysis.historical_slice_rankings) == 1

    def test_dimension_analysis_validation(self):
        """Test validation in the DimensionAnalysis model."""
        # Act & Assert
        with pytest.raises(ValidationError):
            # Missing required fields
            DimensionAnalysis(
                pattern="dimension_analysis",
                version="1.0.0",
                metric_id="revenue",
                grain="day",
                # Missing dimension_name
                slices=[],
                top_slices=[],
                bottom_slices=[],
                comparison_highlights=[],
            )

        with pytest.raises(ValidationError):
            # Wrong type for slices
            DimensionAnalysis(
                pattern="dimension_analysis",
                version="1.0.0",
                metric_id="revenue",
                grain="day",
                dimension_name="region",
                slices="not a list",  # Should be a list
                top_slices=[],
                bottom_slices=[],
                comparison_highlights=[],
            )

        with pytest.raises(ValidationError):
            # Wrong element type in slices
            DimensionAnalysis(
                pattern="dimension_analysis",
                version="1.0.0",
                metric_id="revenue",
                grain="day",
                dimension_name="region",
                slices=["not a SlicePerformance object"],  # Should be SlicePerformance objects
                top_slices=[],
                bottom_slices=[],
                comparison_highlights=[],
            )

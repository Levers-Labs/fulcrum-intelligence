"""
Tests for the dimension analysis evaluator.
"""

import pytest

from commons.models.enums import Granularity
from levers.models import (
    AnalysisWindow,
    SliceComparison,
    SlicePerformance,
    SliceRanking,
    SliceShare,
    SliceStrength,
)
from levers.models.patterns.dimension_analysis import DimensionAnalysis
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.evaluators.dimension_analysis import DimensionAnalysisEvaluator


@pytest.fixture
def mock_top_slices():
    """Fixture for mock top slices."""
    return [
        SliceRanking(
            dimension="region",
            slice_value="North America",
            metric_value=1500.0,
            rank=1,
            avg_other_slices_value=800.0,
            absolute_diff_from_avg=700.0,
            absolute_diff_percent_from_avg=87.5,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Europe",
            metric_value=1300.0,
            rank=2,
            avg_other_slices_value=800.0,
            absolute_diff_from_avg=500.0,
            absolute_diff_percent_from_avg=62.5,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Asia Pacific",
            metric_value=1200.0,
            rank=3,
            avg_other_slices_value=800.0,
            absolute_diff_from_avg=400.0,
            absolute_diff_percent_from_avg=50.0,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Latin America",
            metric_value=1100.0,
            rank=4,
            avg_other_slices_value=800.0,
            absolute_diff_from_avg=300.0,
            absolute_diff_percent_from_avg=37.5,
        ),
    ]


@pytest.fixture
def mock_bottom_slices():
    """Fixture for mock bottom slices."""
    return [
        SliceRanking(
            dimension="region",
            slice_value="Middle East",
            metric_value=500.0,
            rank=5,
            avg_other_slices_value=1000.0,
            absolute_diff_from_avg=-500.0,
            absolute_diff_percent_from_avg=-50.0,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Africa",
            metric_value=400.0,
            rank=6,
            avg_other_slices_value=1000.0,
            absolute_diff_from_avg=-600.0,
            absolute_diff_percent_from_avg=-60.0,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Central Asia",
            metric_value=300.0,
            rank=7,
            avg_other_slices_value=1000.0,
            absolute_diff_from_avg=-700.0,
            absolute_diff_percent_from_avg=-70.0,
        ),
        SliceRanking(
            dimension="region",
            slice_value="Antarctica",
            metric_value=200.0,
            rank=8,
            avg_other_slices_value=1000.0,
            absolute_diff_from_avg=-800.0,
            absolute_diff_percent_from_avg=-80.0,
        ),
    ]


@pytest.fixture
def mock_slice_performances():
    """Fixture for mock slice performances."""
    return [
        SlicePerformance(
            slice_value="North America",
            current_value=1500.0,
            prior_value=1400.0,
            absolute_change=100.0,
            relative_change_percent=7.14,
            current_share_of_volume_percent=36.59,
            prior_share_of_volume_percent=37.33,
            share_of_volume_change_percent=-0.74,
            consecutive_above_avg_streak=10,
        ),
        SlicePerformance(
            slice_value="Europe",
            current_value=1300.0,
            prior_value=1200.0,
            absolute_change=100.0,
            relative_change_percent=8.33,
            current_share_of_volume_percent=31.71,
            prior_share_of_volume_percent=32.0,
            share_of_volume_change_percent=-0.29,
            consecutive_above_avg_streak=8,
        ),
        SlicePerformance(
            slice_value="Asia Pacific",
            current_value=1200.0,
            prior_value=1100.0,
            absolute_change=100.0,
            relative_change_percent=9.09,
            current_share_of_volume_percent=29.27,
            prior_share_of_volume_percent=29.33,
            share_of_volume_change_percent=-0.06,
            consecutive_above_avg_streak=6,
        ),
        SlicePerformance(
            slice_value="Latin America",
            current_value=1100.0,
            prior_value=1000.0,
            absolute_change=100.0,
            relative_change_percent=10.0,
            current_share_of_volume_percent=26.83,
            prior_share_of_volume_percent=26.67,
            share_of_volume_change_percent=0.16,
            consecutive_above_avg_streak=4,
        ),
        SlicePerformance(
            slice_value="Middle East",
            current_value=500.0,
            prior_value=600.0,
            absolute_change=-100.0,
            relative_change_percent=-16.67,
            current_share_of_volume_percent=12.20,
            prior_share_of_volume_percent=16.0,
            share_of_volume_change_percent=-3.8,
            consecutive_above_avg_streak=-2,
        ),
        SlicePerformance(
            slice_value="Africa",
            current_value=400.0,
            prior_value=500.0,
            absolute_change=-100.0,
            relative_change_percent=-20.0,
            current_share_of_volume_percent=9.76,
            prior_share_of_volume_percent=13.33,
            share_of_volume_change_percent=-3.57,
            consecutive_above_avg_streak=-4,
        ),
        SlicePerformance(
            slice_value="Central Asia",
            current_value=300.0,
            prior_value=400.0,
            absolute_change=-100.0,
            relative_change_percent=-25.0,
            current_share_of_volume_percent=7.32,
            prior_share_of_volume_percent=10.67,
            share_of_volume_change_percent=-3.35,
            consecutive_above_avg_streak=-6,
        ),
        SlicePerformance(
            slice_value="Antarctica",
            current_value=200.0,
            prior_value=300.0,
            absolute_change=-100.0,
            relative_change_percent=-33.33,
            current_share_of_volume_percent=4.88,
            prior_share_of_volume_percent=8.0,
            share_of_volume_change_percent=-3.12,
            consecutive_above_avg_streak=-8,
        ),
    ]


@pytest.fixture
def mock_comparison_highlights():
    """Fixture for mock comparison highlights."""
    return [
        SliceComparison(
            slice_a="North America",
            current_value_a=1500.0,
            prior_value_a=1400.0,
            slice_b="Antarctica",
            current_value_b=200.0,
            prior_value_b=300.0,
            performance_gap_percent=650.0,
            gap_change_percent=283.33,
        ),
        SliceComparison(
            slice_a="Europe",
            current_value_a=1300.0,
            prior_value_a=1200.0,
            slice_b="Africa",
            current_value_b=400.0,
            prior_value_b=500.0,
            performance_gap_percent=225.0,
            gap_change_percent=85.0,
        ),
    ]


@pytest.fixture
def mock_new_strongest_slice():
    """Fixture for mock new strongest slice."""
    return SliceStrength(
        slice_value="North America",
        previous_slice_value="Europe",
        current_value=1500.0,
        prior_value=1400.0,
        absolute_delta=100.0,
        relative_delta_percent=7.14,
    )


@pytest.fixture
def mock_new_weakest_slice():
    """Fixture for mock new weakest slice."""
    return SliceStrength(
        slice_value="Antarctica",
        previous_slice_value="Central Asia",
        current_value=200.0,
        prior_value=300.0,
        absolute_delta=-100.0,
        relative_delta_percent=-33.33,
    )


@pytest.fixture
def mock_largest_slice():
    """Fixture for mock largest slice."""
    return SliceShare(
        slice_value="North America",
        current_share_of_volume_percent=36.59,
        previous_slice_value="Europe",
        previous_share_percent=32.0,
    )


@pytest.fixture
def mock_smallest_slice():
    """Fixture for mock smallest slice."""
    return SliceShare(
        slice_value="Antarctica",
        current_share_of_volume_percent=4.88,
        previous_slice_value="Central Asia",
        previous_share_percent=7.32,
    )


@pytest.fixture
def mock_dimension_analysis(
    mock_top_slices,
    mock_bottom_slices,
    mock_slice_performances,
    mock_comparison_highlights,
    mock_new_strongest_slice,
    mock_new_weakest_slice,
    mock_largest_slice,
    mock_smallest_slice,
):
    """Fixture for mock dimension analysis."""
    return DimensionAnalysis(
        pattern="dimension_analysis",
        id=1,
        metric_id="test_metric",
        analysis_date="2024-01-01",
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
        dimension_name="region",
        slices=mock_slice_performances,
        top_slices=mock_top_slices,
        bottom_slices=mock_bottom_slices,
        comparison_highlights=mock_comparison_highlights,
        new_strongest_slice=mock_new_strongest_slice,
        new_weakest_slice=mock_new_weakest_slice,
        largest_slice=mock_largest_slice,
        smallest_slice=mock_smallest_slice,
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {"label": "Test Metric", "value": 1000.0}


@pytest.fixture
def evaluator():
    """Fixture for evaluator."""
    return DimensionAnalysisEvaluator()


@pytest.mark.asyncio
async def test_evaluate_all_stories(mock_dimension_analysis, mock_metric):
    """Test that all stories are generated."""
    evaluator = DimensionAnalysisEvaluator()
    stories = await evaluator.evaluate(mock_dimension_analysis, mock_metric)

    # Should generate one story for each story type
    assert len(stories) == 7

    # Check that each story type is generated
    story_types = [story["story_type"] for story in stories]
    assert StoryType.TOP_4_SEGMENTS in story_types
    assert StoryType.BOTTOM_4_SEGMENTS in story_types
    assert StoryType.SEGMENT_COMPARISONS in story_types
    assert StoryType.NEW_STRONGEST_SEGMENT in story_types
    assert StoryType.NEW_WEAKEST_SEGMENT in story_types
    assert StoryType.NEW_LARGEST_SEGMENT in story_types
    assert StoryType.NEW_SMALLEST_SEGMENT in story_types


@pytest.mark.asyncio
async def test_evaluate_top_segments_only(
    mock_dimension_analysis, mock_metric, mock_top_slices, mock_slice_performances
):
    """Test with only top segments data."""
    mock_dimension_analysis.bottom_slices = []
    mock_dimension_analysis.comparison_highlights = []
    mock_dimension_analysis.new_strongest_slice = None
    mock_dimension_analysis.new_weakest_slice = None
    mock_dimension_analysis.largest_slice = None
    mock_dimension_analysis.smallest_slice = None

    evaluator = DimensionAnalysisEvaluator()
    stories = await evaluator.evaluate(mock_dimension_analysis, mock_metric)

    assert len(stories) == 1
    assert stories[0]["story_type"] == StoryType.TOP_4_SEGMENTS


@pytest.mark.asyncio
async def test_evaluate_empty_slices(mock_dimension_analysis, mock_metric):
    """Test with empty slices."""
    mock_dimension_analysis.slices = []
    mock_dimension_analysis.top_slices = []
    mock_dimension_analysis.bottom_slices = []
    mock_dimension_analysis.comparison_highlights = []
    mock_dimension_analysis.new_strongest_slice = None
    mock_dimension_analysis.new_weakest_slice = None
    mock_dimension_analysis.largest_slice = None
    mock_dimension_analysis.smallest_slice = None

    evaluator = DimensionAnalysisEvaluator()
    stories = await evaluator.evaluate(mock_dimension_analysis, mock_metric)

    assert len(stories) == 0


def test_populate_template_context(evaluator, mock_dimension_analysis, mock_metric):
    """Test _populate_template_context method."""
    context = evaluator._populate_template_context(mock_dimension_analysis, mock_metric, Granularity.DAY)

    assert context["metric"] == mock_metric
    assert context["dimension_name"] == "region"
    assert context["grain_label"] == "day"
    assert "pop" in context


def test_create_top_segments_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_top_segments_story method."""
    story = evaluator._create_top_segments_story(mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY)

    assert story["story_type"] == StoryType.TOP_4_SEGMENTS
    assert story["story_group"] == StoryGroup.SIGNIFICANT_SEGMENTS
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert "top_segments" in story["variables"]
    assert "min_diff_percent" in story["variables"]
    assert "max_diff_percent" in story["variables"]
    assert "total_share_percent" in story["variables"]


def test_create_bottom_segments_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_bottom_segments_story method."""
    story = evaluator._create_bottom_segments_story(
        mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.BOTTOM_4_SEGMENTS
    assert story["story_group"] == StoryGroup.SIGNIFICANT_SEGMENTS
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert "bottom_segments" in story["variables"]
    assert "min_diff_percent" in story["variables"]
    assert "max_diff_percent" in story["variables"]
    assert "total_share_percent" in story["variables"]


def test_create_segment_comparison_story(evaluator, mock_dimension_analysis, mock_metric, mock_comparison_highlights):
    """Test _create_segment_comparison_story method."""
    story = evaluator._create_segment_comparison_story(
        mock_comparison_highlights[0], mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.SEGMENT_COMPARISONS
    assert story["story_group"] == StoryGroup.SIGNIFICANT_SEGMENTS
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert "segment_a" in story["variables"]
    assert "segment_b" in story["variables"]
    assert "performance_diff_percent" in story["variables"]
    assert "gap_trend" in story["variables"]
    assert "gap_change_percent" in story["variables"]


def test_create_new_strongest_segment_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_new_strongest_segment_story method."""
    story = evaluator._create_new_strongest_segment_story(
        mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.NEW_STRONGEST_SEGMENT
    assert story["story_group"] == StoryGroup.SEGMENT_CHANGES
    assert story["genre"] == StoryGenre.TRENDS
    assert "segment_name" in story["variables"]
    assert "current_value" in story["variables"]
    assert "previous_segment" in story["variables"]
    assert "trend_direction" in story["variables"]
    assert "diff_from_avg_percent" in story["variables"]


def test_create_new_weakest_segment_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_new_weakest_segment_story method."""
    story = evaluator._create_new_weakest_segment_story(
        mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.NEW_WEAKEST_SEGMENT
    assert story["story_group"] == StoryGroup.SEGMENT_CHANGES
    assert story["genre"] == StoryGenre.TRENDS
    assert "segment_name" in story["variables"]
    assert "current_value" in story["variables"]
    assert "previous_segment" in story["variables"]
    assert "trend_direction" in story["variables"]
    assert "diff_from_avg_percent" in story["variables"]


def test_create_largest_segment_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_largest_segment_story method."""
    story = evaluator._create_largest_segment_story(
        mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.NEW_LARGEST_SEGMENT
    assert story["story_group"] == StoryGroup.SEGMENT_CHANGES
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert "segment_name" in story["variables"]
    assert "current_share_percent" in story["variables"]
    assert "prior_share_percent" in story["variables"]
    assert "previous_segment" in story["variables"]
    assert "previous_share_percent" in story["variables"]


def test_create_smallest_segment_story(evaluator, mock_dimension_analysis, mock_metric):
    """Test _create_smallest_segment_story method."""
    story = evaluator._create_smallest_segment_story(
        mock_dimension_analysis, "test_metric", mock_metric, Granularity.DAY
    )

    assert story["story_type"] == StoryType.NEW_SMALLEST_SEGMENT
    assert story["story_group"] == StoryGroup.SEGMENT_CHANGES
    assert story["genre"] == StoryGenre.TRENDS
    assert "segment_name" in story["variables"]
    assert "current_share_percent" in story["variables"]
    assert "prior_share_percent" in story["variables"]
    assert "previous_segment" in story["variables"]
    assert "previous_share_percent" in story["variables"]
    assert "previous_prior_share_percent" in story["variables"]

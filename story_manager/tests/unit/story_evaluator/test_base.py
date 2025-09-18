"""
Tests for the story evaluator base module.
"""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from commons.models.enums import Granularity
from levers.models import AnalysisWindow, BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.base import StoryEvaluatorBase
from story_manager.story_evaluator.constants import STORY_TYPE_TIME_DURATIONS


class MockPattern(BasePattern):
    """Mock pattern for testing."""

    pattern: str = "test_pattern"
    pattern_run_id: int = 9
    analysis_date: date = date(2024, 1, 1)
    metric_id: str = "test_metric"
    grain: Granularity = Granularity.DAY


class MockEvaluator(StoryEvaluatorBase[MockPattern]):
    """Mock evaluator implementation for testing."""

    pattern_name = "test_pattern"
    genre = StoryGenre.PERFORMANCE

    async def evaluate(self, pattern_result: MockPattern, metric: dict) -> list[dict]:
        """Evaluate the pattern result and generate stories."""
        return [
            self.prepare_story_model(
                genre=StoryGenre.PERFORMANCE,
                story_type=StoryType.ON_TRACK,
                story_group=StoryGroup.GOAL_VS_ACTUAL,
                metric_id=pattern_result.metric_id,
                pattern_result=pattern_result,
                title="Test Title",
                detail="Test Detail",
                grain=pattern_result.grain,
                test_var="test_value",
            )
        ]


@pytest.fixture
def mock_pattern():
    """Fixture for mock pattern."""
    return MockPattern(
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31")
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {"label": "Test Metric", "metric_id": "test_metric", "unit": "n"}


@pytest.fixture
def mock_evaluator():
    """Fixture for mock evaluator."""
    return MockEvaluator()


@pytest.fixture
def mock_series_df():
    """Fixture for mock series dataframe."""
    return pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=10),
            "value": [10, 20, 30, 40, 50, float("inf"), float("-inf"), np.nan, 90, 100],
            "metric": ["Test"] * 10,
        }
    )


@pytest.mark.asyncio
async def test_prepare_story_model(mock_evaluator, mock_pattern, mock_metric):
    """Test prepare_story_model method."""
    story = mock_evaluator.prepare_story_model(
        genre=StoryGenre.PERFORMANCE,
        story_type=StoryType.ON_TRACK,
        story_group=StoryGroup.GOAL_VS_ACTUAL,
        metric_id=mock_pattern.metric_id,
        pattern_result=mock_pattern,
        title="Test Title",
        detail="Test Detail",
        grain=mock_pattern.grain,
        test_var="test_value",
    )

    assert story["version"] == 2
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert story["story_type"] == StoryType.ON_TRACK
    assert story["story_group"] == StoryGroup.GOAL_VS_ACTUAL
    assert story["grain"] == mock_pattern.grain
    assert story["metric_id"] == mock_pattern.metric_id
    assert story["title"] == "Test Title"
    assert story["detail"] == "Test Detail"
    assert story["story_date"] == mock_pattern.analysis_date
    assert story["variables"]["test_var"] == "test_value"
    assert story["metadata"]["pattern"] == mock_pattern.pattern


@pytest.mark.asyncio
async def test_prepare_story_model_with_series_data(mock_evaluator, mock_pattern, mock_series_df):
    """Test prepare_story_model method with series data."""
    mock_evaluator.series_df = mock_series_df

    # Test with custom series data
    custom_series_data = [{"date": "2024-01-01", "value": 100}]
    story = mock_evaluator.prepare_story_model(
        genre=StoryGenre.PERFORMANCE,
        story_type=StoryType.ON_TRACK,
        story_group=StoryGroup.GOAL_VS_ACTUAL,
        metric_id=mock_pattern.metric_id,
        pattern_result=mock_pattern,
        title="Test Title",
        detail="Test Detail",
        grain=mock_pattern.grain,
        series_data=custom_series_data,
        test_var="test_value",
    )

    assert story["series"] == custom_series_data


def test_get_template_string(mock_evaluator):
    """Test get_template_string method."""
    title_template = mock_evaluator.get_template_string(StoryType.ON_TRACK, "title")
    detail_template = mock_evaluator.get_template_string(StoryType.ON_TRACK, "detail")

    assert "is on track" in title_template
    assert "is at" in detail_template

    # Test with non-existent story type
    non_existent_template = mock_evaluator.get_template_string("NON_EXISTENT", "title")
    assert non_existent_template == ""

    # Test with non-existent field
    non_existent_field = mock_evaluator.get_template_string(StoryType.ON_TRACK, "non_existent")
    assert non_existent_field == ""


def test_export_dataframe_as_story_series(mock_evaluator, mock_series_df):
    """Test export_dataframe_as_story_series method."""
    # Set series_df on the evaluator
    mock_evaluator.series_df = mock_series_df

    # Test with story type duration
    series_data = mock_evaluator.export_dataframe_as_story_series(
        mock_series_df, StoryType.ON_TRACK, StoryGroup.GOAL_VS_ACTUAL, Granularity.DAY
    )

    # Verify output format and handling of special values
    assert len(series_data) > 0
    assert all(isinstance(item, dict) for item in series_data)
    assert "date" in series_data[0]
    assert "value" in series_data[0]
    assert not any(pd.isnull(item["value"]) for item in series_data if item["value"] is not None)
    assert not any(item["value"] == float("inf") for item in series_data if item["value"] is not None)
    assert not any(item["value"] == float("-inf") for item in series_data if item["value"] is not None)

    # Test with empty dataframe
    empty_df = pd.DataFrame()
    empty_series_data = mock_evaluator.export_dataframe_as_story_series(
        empty_df, StoryType.ON_TRACK, StoryGroup.GOAL_VS_ACTUAL, Granularity.DAY
    )
    assert empty_series_data == []

    # Test with None dataframe
    none_series_data = mock_evaluator.export_dataframe_as_story_series(
        None, StoryType.ON_TRACK, StoryGroup.GOAL_VS_ACTUAL, Granularity.DAY
    )
    assert none_series_data == []


def test_prepare_base_context(mock_evaluator, mock_metric):
    """Test prepare_base_context method."""
    # Test with DAY grain
    day_context = mock_evaluator.prepare_base_context(mock_metric, Granularity.DAY)
    assert day_context["metric"] == mock_metric
    assert day_context["grain_label"] == "day"
    assert day_context["pop"] == "d/d"

    # Test with WEEK grain
    week_context = mock_evaluator.prepare_base_context(mock_metric, Granularity.WEEK)
    assert week_context["grain_label"] == "week"
    assert week_context["pop"] == "w/w"

    # Test with MONTH grain
    month_context = mock_evaluator.prepare_base_context(mock_metric, Granularity.MONTH)
    assert month_context["grain_label"] == "month"
    assert month_context["pop"] == "m/m"

    # Test with QUARTER grain
    quarter_context = mock_evaluator.prepare_base_context(mock_metric, Granularity.QUARTER)
    assert quarter_context["grain_label"] == "quarter"
    assert quarter_context["pop"] == "q/q"

    # Test with YEAR grain
    year_context = mock_evaluator.prepare_base_context(mock_metric, Granularity.YEAR)
    assert year_context["grain_label"] == "year"
    assert year_context["pop"] == "y/y"

    # Test with unknown grain (should use defaults)
    unknown_context = mock_evaluator.prepare_base_context(mock_metric, "UNKNOWN")
    assert unknown_context["grain_label"] == "period"
    assert unknown_context["pop"] == "PoP"


def test_get_output_length(mock_evaluator):
    """Test get_output_length method."""
    # Test with story type duration configuration
    length_by_type = mock_evaluator.get_output_length(StoryType.ON_TRACK, StoryGroup.GOAL_VS_ACTUAL, Granularity.DAY)
    assert length_by_type is not None
    assert isinstance(length_by_type, int)

    # Test with story group duration configuration
    test_type = next((t for t in StoryType if t not in STORY_TYPE_TIME_DURATIONS), None)
    if test_type:
        length_by_group = mock_evaluator.get_output_length(test_type, StoryGroup.GOAL_VS_ACTUAL, Granularity.DAY)
        assert length_by_group is not None
        assert isinstance(length_by_group, int)

    # Test with non-existent configurations
    length_not_found = mock_evaluator.get_output_length("NON_EXISTENT_TYPE", "NON_EXISTENT_GROUP", Granularity.DAY)
    assert length_not_found is None


@pytest.mark.asyncio
async def test_run(mock_evaluator, mock_pattern, mock_metric):
    """Test run method."""
    stories = await mock_evaluator.run(mock_pattern, mock_metric)

    assert len(stories) == 1
    assert stories[0]["title"] == "Test Title"
    assert stories[0]["detail"] == "Test Detail"


@pytest.mark.asyncio
async def test_run_no_stories(mock_evaluator, mock_pattern, mock_metric):
    """Test run method when no stories are generated."""

    # Override evaluate to return empty list
    async def empty_evaluate(p, m):
        return []

    mock_evaluator.evaluate = empty_evaluate

    stories = await mock_evaluator.run(mock_pattern, mock_metric)
    assert len(stories) == 0

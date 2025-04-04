"""
Tests for the story evaluator base module.
"""

import pytest

from commons.models.enums import Granularity
from levers.models.common import AnalysisWindow, BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.base import StoryEvaluatorBase


class MockPattern(BasePattern):
    """Mock pattern for testing."""

    pattern: str = "test_pattern"
    pattern_run_id: str = "test_run_id"
    analysis_date: str = "2024-01-01"
    metric_id: str = "test_metric"


class MockEvaluator(StoryEvaluatorBase[MockPattern]):
    """Mock evaluator implementation for testing."""

    pattern_name = "test_pattern"
    genre = StoryGenre.PERFORMANCE

    async def evaluate(self, pattern_result: MockPattern, metric: dict) -> list[dict]:
        """Evaluate the pattern result and generate stories."""
        return [
            self.prepare_story_model(
                story_type=StoryType.ON_TRACK,
                story_group=StoryGroup.GOAL_VS_ACTUAL,
                metric_id=pattern_result.metric_id,
                pattern_result=pattern_result,
                metric=metric,
                title="Test Title",
                detail="Test Detail",
                grain=Granularity.DAY,
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
    return {"label": "Test Metric", "value": 100.0}


@pytest.fixture
def mock_evaluator():
    """Fixture for mock evaluator."""
    return MockEvaluator()


@pytest.mark.asyncio
async def test_prepare_story_model(mock_evaluator, mock_pattern, mock_metric):
    """Test prepare_story_model method."""
    story = mock_evaluator.prepare_story_model(
        story_type=StoryType.ON_TRACK,
        story_group=StoryGroup.GOAL_VS_ACTUAL,
        metric_id=mock_pattern.metric_id,
        pattern_result=mock_pattern,
        metric=mock_metric,
        title="Test Title",
        detail="Test Detail",
        grain=Granularity.DAY,
        test_var="test_value",
    )

    assert story["version"] == 2
    assert story["genre"] == StoryGenre.PERFORMANCE
    assert story["story_type"] == StoryType.ON_TRACK
    assert story["story_group"] == StoryGroup.GOAL_VS_ACTUAL
    assert story["grain"] == Granularity.DAY
    assert story["metric_id"] == mock_pattern.metric_id
    assert story["title"] == "Test Title"
    assert story["detail"] == "Test Detail"
    assert story["story_date"] == mock_pattern.analysis_date
    assert story["variables"]["test_var"] == "test_value"
    assert story["metadata"]["pattern"] == mock_pattern.pattern
    assert story["pattern_run_id"] == mock_pattern.pattern_run_id


def test_get_template_string(mock_evaluator):
    """Test get_template_string method."""
    title_template = mock_evaluator.get_template_string(StoryType.ON_TRACK, "title")
    detail_template = mock_evaluator.get_template_string(StoryType.ON_TRACK, "detail")

    assert "is on track" in title_template
    assert "is at" in detail_template


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

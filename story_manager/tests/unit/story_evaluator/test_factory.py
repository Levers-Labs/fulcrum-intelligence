"""
Tests for the story evaluator factory module.
"""

import pytest

from commons.models.enums import Granularity
from levers.models.common import AnalysisWindow, BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.story_evaluator.base import StoryEvaluatorBase
from story_manager.story_evaluator.factory import StoryEvaluatorFactory


class MockPattern(BasePattern):
    """Mock pattern for testing."""

    pattern: str = "test_pattern"
    pattern_run_id: str = "test_run_id"
    analysis_date: str = "2024-01-01"
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
    return {"label": "Test Metric", "value": 100.0}


@pytest.fixture
def mock_evaluator():
    """Fixture for mock evaluator."""

    return MockEvaluator()


@pytest.mark.asyncio
async def test_create_story_evaluator(mock_pattern, mock_metric, mock_evaluator, mocker):
    """Test create_story_evaluator method."""
    # Mock the evaluator class
    mocker.patch(
        "story_manager.story_evaluator.factory.StoryEvaluatorFactory.get_story_evaluator",
        return_value=mock_evaluator.__class__,
    )

    factory = StoryEvaluatorFactory()
    evaluator = factory.create_story_evaluator(mock_pattern.pattern)

    assert evaluator is not None
    assert isinstance(evaluator, MockEvaluator)


@pytest.mark.asyncio
async def test_create_story_evaluator_unknown_pattern():
    """Test create_story_evaluator method with unknown pattern."""
    factory = StoryEvaluatorFactory()

    with pytest.raises(ValueError, match="No story evaluator found for pattern"):
        factory.create_story_evaluator("invalid")


@pytest.mark.asyncio
async def test_get_evaluator_class():
    """Test _get_evaluator_class method."""
    factory = StoryEvaluatorFactory()

    # Test with known pattern
    evaluator_class = factory.get_story_evaluator("performance_status")
    assert evaluator_class is not None

    # Test with unknown pattern
    with pytest.raises(ValueError, match="No story evaluator found for pattern unknown_pattern"):
        factory.get_story_evaluator("unknown_pattern")


@pytest.mark.asyncio
async def test_evaluator_run(mock_pattern, mock_metric, mock_evaluator):
    """Test evaluator run method."""
    stories = await mock_evaluator.run(mock_pattern, mock_metric)

    assert len(stories) == 1
    assert stories[0]["title"] == "Test Title"
    assert stories[0]["grain"] == mock_pattern.grain

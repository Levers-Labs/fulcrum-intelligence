"""
Tests for the story evaluator manager module.
"""

from datetime import date

import pytest
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.models.enums import Granularity
from commons.utilities.context import set_tenant_id
from levers.models.common import AnalysisWindow, BasePattern
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType
from story_manager.core.models import Story
from story_manager.story_evaluator.manager import StoryEvaluatorManager


class MockPattern(BasePattern):
    """Mock pattern for testing."""

    pattern: str = "test_pattern"
    pattern_run_id: str = "test_run_id"
    analysis_date: str = "2024-01-01"
    metric_id: str = "test_metric"
    grain: Granularity = Granularity.DAY


@pytest.fixture
def mock_pattern():
    """Fixture for mock pattern."""
    return MockPattern(
        analysis_window=AnalysisWindow(grain=Granularity.DAY, start_date="2024-01-01", end_date="2024-01-31"),
    )


@pytest.fixture
def mock_metric():
    """Fixture for mock metric."""
    return {"label": "Test Metric", "value": 100.0, "metric_id": "test_metric", "unit": "n"}


@pytest.fixture
def mock_stories():
    """Fixture for mock stories."""
    return [
        {
            "tenant_id": 1,
            "version": 2,
            "genre": StoryGenre.PERFORMANCE,
            "story_type": StoryType.ON_TRACK,
            "story_group": StoryGroup.GOAL_VS_ACTUAL,
            "grain": Granularity.DAY,
            "metric_id": "test_metric",
            "title": "Test Title",
            "detail": "Test Detail",
            "title_template": "Test Title Template",
            "detail_template": "Test Detail Template",
            "story_date": date(2024, 1, 1),
            "variables": {"test_var": "test_value"},
            "metadata": {"pattern": "test_pattern"},
            "pattern_run_id": 1,
        }
    ]


@pytest.fixture
def mock_db_session(mocker):
    """Fixture for mock database session."""
    session = mocker.Mock(spec=AsyncSession)
    session.commit = mocker.AsyncMock()
    session.refresh = mocker.AsyncMock()
    session.add = mocker.Mock()

    # Create a mock result that has scalar_one_or_none
    mock_result = mocker.Mock()
    mock_result.scalar_one_or_none = mocker.Mock(return_value=None)

    # Make execute return the mock result
    session.execute = mocker.AsyncMock(return_value=mock_result)

    # Also mock scalar_one_or_none directly on session for compatibility
    session.scalar_one_or_none = mocker.AsyncMock(return_value=None)
    return session


@pytest.mark.asyncio
async def test_persist_stories(mock_db_session, mock_stories, jwt_payload, mocker):
    """Test persist_stories method."""
    # Set up the mock JWT payload
    set_tenant_id(jwt_payload["tenant_id"])

    # Create a mock Story object that would be returned by the CRUD layer
    mock_story = Story(
        id=1,
        title="Test Title",
        detail="Test Detail",
        metric_id="test_metric",
        pattern_run_id=1,
        grain=Granularity.DAY,
        tenant_id=jwt_payload["tenant_id"],
    )

    # Mock the CRUDStory.upsert_stories method directly instead of session operations
    mock_crud_story = mocker.Mock()
    mock_crud_story.upsert_stories = mocker.AsyncMock(return_value=[mock_story])

    mocker.patch("story_manager.story_evaluator.manager.CRUDStory", return_value=mock_crud_story)

    manager = StoryEvaluatorManager()
    stories = await manager.persist_stories(mock_stories, mock_db_session)

    assert len(stories) == 1
    assert isinstance(stories[0], Story)
    assert stories[0].title == "Test Title"
    assert stories[0].detail == "Test Detail"
    assert stories[0].metric_id == "test_metric"
    assert stories[0].pattern_run_id == 1
    assert stories[0].grain == Granularity.DAY

    # Verify the CRUD method was called
    mock_crud_story.upsert_stories.assert_called_once_with(mock_stories)


@pytest.mark.asyncio
async def test_persist_stories_empty(mock_db_session):
    """Test persist_stories method with empty list."""
    manager = StoryEvaluatorManager()
    stories = await manager.persist_stories([], mock_db_session)

    assert len(stories) == 0
    mock_db_session.add.assert_not_called()
    mock_db_session.commit.assert_not_called()
    mock_db_session.refresh.assert_not_called()


@pytest.mark.asyncio
async def test_evaluate_pattern_result(mock_pattern, mock_metric, mocker):
    """Test evaluate_pattern_result method."""
    # Mock the factory to return a mock evaluator
    mock_evaluator = mocker.Mock()
    mock_evaluator.run = mocker.AsyncMock(return_value=[{"title": "Test Story"}])
    mocker.patch(
        "story_manager.story_evaluator.manager.StoryEvaluatorFactory.create_story_evaluator",
        return_value=mock_evaluator,
    )

    manager = StoryEvaluatorManager()
    stories = await manager.evaluate_pattern_result(mock_pattern, mock_metric)

    assert len(stories) == 1
    assert stories[0]["title"] == "Test Story"
    mock_evaluator.run.assert_called_once_with(mock_pattern, mock_metric)


@pytest.mark.asyncio
async def test_evaluate_pattern_result_missing_pattern(mock_pattern, mock_metric):
    """Test evaluate_pattern_result method with missing pattern attribute."""
    mock_pattern.pattern = None
    manager = StoryEvaluatorManager()

    with pytest.raises(ValueError, match="Pattern result missing 'pattern' attribute"):
        await manager.evaluate_pattern_result(mock_pattern, mock_metric)


@pytest.mark.asyncio
async def test_evaluate_pattern_result_missing_metric_id(mock_pattern, mock_metric):
    """Test evaluate_pattern_result method with missing metric_id attribute."""
    mock_pattern.metric_id = None
    manager = StoryEvaluatorManager()

    with pytest.raises(ValueError, match="Pattern result missing 'metric_id' attribute"):
        await manager.evaluate_pattern_result(mock_pattern, mock_metric)


@pytest.mark.asyncio
async def test_evaluate_pattern_result_error(mock_pattern, mock_metric, mocker):
    """Test evaluate_pattern_result method with evaluator error."""
    # Mock the factory to raise an exception
    mocker.patch(
        "story_manager.story_evaluator.manager.StoryEvaluatorFactory.create_story_evaluator",
        side_effect=Exception("Test error"),
    )

    manager = StoryEvaluatorManager()

    with pytest.raises(Exception, match="Test error"):
        await manager.evaluate_pattern_result(mock_pattern, mock_metric)

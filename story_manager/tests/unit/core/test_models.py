from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from commons.models.enums import Granularity
from story_manager.core.enums import (
    GROUP_TO_STORY_TYPE_MAPPING,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.models import Story


@pytest.mark.parametrize("group, story_type", [(StoryGroup.GROWTH_RATES, StoryType.ACCELERATING_GROWTH)])
def test_invalid_type_genre_combination(group, story_type):
    data = {
        "story_group": group,
        "story_type": story_type,
        "metric_id": "test_metric",
        "title": "d/d growth is speeding up",
        "title_template": "{{pop}} growth is speeding up",
        "detail": "The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% "
        "average over the past 11 days.",
        "detail_template": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{"
        "current_growth}}% and up from the {{reference_growth}}% average over the past {{"
        "reference_period_days}} {{days}}s.",
    }
    with pytest.raises(ValidationError):
        Story.model_validate(data)


@pytest.mark.parametrize(
    "group, story_type",
    [(group, story_type) for group, valid_types in GROUP_TO_STORY_TYPE_MAPPING.items() for story_type in valid_types],
)
def test_valid_type_group_combination(group, story_type):
    data = {
        "genre": StoryGenre.GROWTH,
        "story_group": group,
        "grain": Granularity.DAY,
        "story_date": datetime(2020, 1, 1),
        "story_type": story_type,
        "metric_id": "test_metric",
        "title": "d/d growth is speeding up",
        "title_template": "{{pop}} growth is speeding up",
        "detail": "The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% "
        "average over the past 11 days.",
        "detail_template": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{"
        "current_growth}}% and up from the {{reference_growth}}% average over the past {{"
        "reference_period_days}} {{days}}s.",
    }
    validated_data = Story.model_validate(data)
    assert validated_data.story_group == group
    assert validated_data.story_type == story_type


@pytest.mark.asyncio
async def test_set_heuristics_salient():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 10, "var2": 20},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (True, False, True)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is True
    assert story.in_cool_off is False
    assert story.is_heuristic is True
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_not_salient():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 5, "var2": 15},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (False, False, False)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is False
    assert story.in_cool_off is False
    assert story.is_heuristic is False
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_in_cool_off():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 15, "var2": 25},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (True, True, False)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is True
    assert story.in_cool_off is True
    assert story.is_heuristic is False
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_just_entering_cool_off():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 15, "var2": 25},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (True, True, False)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is True
    assert story.in_cool_off is True
    assert story.is_heuristic is False
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_just_exiting_cool_off():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 15, "var2": 25},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (True, False, True)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is True
    assert story.in_cool_off is False
    assert story.is_heuristic is True
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_edge_of_cool_off():
    story = Story(
        genre=StoryGenre.GROWTH,
        story_group=StoryGroup.GROWTH_RATES,
        story_type=StoryType.ACCELERATING_GROWTH,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="Test Title",
        title_template="Test Template",
        detail="Test Detail",
        detail_template="Test Detail Template",
        story_date=datetime.now(),
        variables={"var1": 15, "var2": 25},
    )

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.return_value = (True, False, True)

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        await story.set_heuristics(mock_session)

    assert story.is_salient is True
    assert story.in_cool_off is False
    assert story.is_heuristic is True
    mock_evaluator.evaluate.assert_called_once_with(story.variables)


@pytest.mark.asyncio
async def test_set_heuristics_multiple_stories_in_cool_off():
    stories = [
        Story(
            genre=StoryGenre.GROWTH,
            story_group=StoryGroup.GROWTH_RATES,
            story_type=StoryType.ACCELERATING_GROWTH,
            grain=Granularity.DAY,
            metric_id="test_metric",
            title=f"Test Title {i}",
            title_template="Test Template",
            detail=f"Test Detail {i}",
            detail_template="Test Detail Template",
            story_date=datetime.now() - timedelta(hours=i),
            variables={"var1": 15 + i, "var2": 25 + i},
        )
        for i in range(3)
    ]

    mock_session = AsyncMock()
    mock_evaluator = AsyncMock()
    mock_evaluator.evaluate.side_effect = [
        (True, True, False),  # First story: in cool-off
        (True, True, False),  # Second story: still in cool-off
        (True, False, True),  # Third story: out of cool-off
    ]

    with patch("story_manager.core.heuristics.StoryHeuristicEvaluator", return_value=mock_evaluator):
        for story in stories:
            await story.set_heuristics(mock_session)

    assert all(story.is_salient for story in stories)
    assert stories[0].in_cool_off is True and stories[0].is_heuristic is False
    assert stories[1].in_cool_off is True and stories[1].is_heuristic is False
    assert stories[2].in_cool_off is False and stories[2].is_heuristic is True

    assert mock_evaluator.evaluate.call_count == 3
    for _, story in enumerate(stories):
        mock_evaluator.evaluate.assert_any_call(story.variables)

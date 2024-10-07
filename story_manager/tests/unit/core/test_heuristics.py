from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.heuristics import StoryHeuristicEvaluator
from story_manager.core.models import Story


class MockResult:
    def __init__(self, return_value):
        self.return_value = return_value

    def unique(self):
        return self

    def scalar_one_or_none(self):
        return self.return_value


@pytest.fixture
def mock_session():
    return AsyncMock()


@pytest.fixture
def evaluator(mock_session):
    return StoryHeuristicEvaluator(StoryType.STABLE_TREND, Granularity.DAY, mock_session)


async def async_return(value):
    return value


def test_render_expression(evaluator):
    variables = {"var1": 10, "var2": 20}
    expression = "{{ var1 }} + {{ var2 }}"
    rendered = evaluator._render_expression(expression, variables)
    assert rendered == "10 + 20"


def test_evaluate_expression(evaluator):
    expression = "10 + 20 == 30"
    result = evaluator._evaluate_expression(expression)
    assert result is True

    expression = "10 + 20 == 25"
    result = evaluator._evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) < 25"
    result = evaluator._evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) == 30 and 5 < 10"
    result = evaluator._evaluate_expression(expression)
    assert result is True

    expression = "(10 + 20) == 30 and 5 > 10"
    result = evaluator._evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) == 30 or 5 > 10"
    result = evaluator._evaluate_expression(expression)
    assert result is True


@pytest.mark.asyncio
async def test_evaluate_salience(evaluator):
    variables = {"var1": 10, "var2": 20}

    with patch("story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock) as mock_get_config:
        mock_get_config.return_value = ("{{ var1 }} + {{ var2 }} == 30", None)
        result = await evaluator._evaluate_salience(variables)
        assert result is True

    with patch("story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock) as mock_get_config:
        mock_get_config.return_value = ("{{ var1 }} + {{ var2 }} == 25", None)
        result = await evaluator._evaluate_salience(variables)
        assert result is False

    with patch("story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock) as mock_get_config:
        mock_get_config.return_value = (None, None)
        result = await evaluator._evaluate_salience(variables)
        assert result is True


@pytest.mark.asyncio
async def test_evaluate(evaluator):
    variables = {"var1": 10, "var2": 20}

    with patch(
        "story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock
    ) as mock_get_config, patch(
        "story_manager.core.crud.CRUDStory.get_last_rendered_story", new_callable=AsyncMock
    ) as mock_get_last_story:
        mock_get_config.return_value = ("{{ var1 }} + {{ var2 }} == 30", timedelta(days=1))
        mock_get_last_story.return_value = None

        is_salient, in_cool_off, render_story = await evaluator.evaluate(variables)
        assert is_salient is True
        assert in_cool_off is False
        assert render_story is True


@pytest.mark.asyncio
async def test_evaluate_cool_off(evaluator):
    with patch(
        "story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock
    ) as mock_get_config, patch(
        "story_manager.core.crud.CRUDStory.get_last_rendered_story", new_callable=AsyncMock
    ) as mock_get_last_story, patch.object(
        evaluator, "_calculate_time_difference", return_value=timedelta(hours=12)
    ):
        mock_get_config.return_value = (None, timedelta(days=1))
        mock_story = Story(story_date=datetime.now() - timedelta(hours=12))
        mock_get_last_story.return_value = mock_story

        in_cool_off, render_story = await evaluator._evaluate_cool_off(is_salient=True)
        assert in_cool_off is True
        assert render_story is False


@pytest.mark.parametrize(
    "grain, current_date, last_render_date, expected",
    [
        (Granularity.DAY, datetime(2023, 5, 10), datetime(2023, 5, 8), 2),
        (Granularity.WEEK, datetime(2023, 5, 10), datetime(2023, 4, 25), 2),
        (Granularity.MONTH, datetime(2023, 5, 10), datetime(2023, 3, 10), 2),
    ],
)
def test_calculate_time_difference(evaluator, grain, current_date, last_render_date, expected):
    evaluator.grain = grain
    result = evaluator._calculate_time_difference(current_date, last_render_date)
    assert result == expected


@pytest.mark.asyncio
async def test_evaluate_with_cool_off(evaluator):
    variables = {"var1": 10, "var2": 20}

    with patch(
        "story_manager.core.crud.CRUDStoryConfig.get_story_config", new_callable=AsyncMock
    ) as mock_get_config, patch(
        "story_manager.core.crud.CRUDStory.get_last_rendered_story", new_callable=AsyncMock
    ) as mock_get_last_story, patch.object(
        evaluator, "_calculate_time_difference", return_value=timedelta(hours=12)
    ):
        mock_get_config.return_value = ("{{ var1 }} + {{ var2 }} == 30", timedelta(days=1))
        mock_story = Story(story_date=datetime.now() - timedelta(hours=12))
        mock_get_last_story.return_value = mock_story

        is_salient, in_cool_off, render_story = await evaluator.evaluate(variables)
        assert is_salient is True
        assert in_cool_off is True
        assert render_story is False

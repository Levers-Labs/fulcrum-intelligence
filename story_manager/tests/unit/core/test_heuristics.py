from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.core.heuristics import StoryHeuristicEvaluator
from story_manager.core.models import Story, StoryConfig


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
    return StoryHeuristicEvaluator(
        StoryType.STABLE_TREND, Granularity.DAY, mock_session, datetime.now() - timedelta(hours=12), "metric_1", 1
    )


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

    with patch.object(evaluator.story_config_crud, "get_story_config", new_callable=AsyncMock) as mock_get_story_config:
        mock_get_story_config.return_value = StoryConfig(heuristic_expression="{{ var1 }} + {{ var2 }} == 30")
        result = await evaluator._evaluate_salience(variables)
        assert result is True


@pytest.mark.asyncio
async def test_evaluate_salience_false(evaluator):
    variables = {"var1": 10, "var2": 20}
    with patch.object(evaluator.story_config_crud, "get_story_config", new_callable=AsyncMock) as mock_get_story_config:
        mock_get_story_config.return_value = StoryConfig(heuristic_expression="{{ var1 }} + {{ var2 }} == 25")
        result = await evaluator._evaluate_salience(variables)
        assert result is False


@pytest.mark.asyncio
async def test_evaluate(evaluator):
    variables = {"var1": 10, "var2": 20}

    with patch.object(
        evaluator.story_config_crud, "get_story_config", new_callable=AsyncMock
    ) as mock_get_story_config, patch.object(
        evaluator.story_crud, "get_latest_story", new_callable=AsyncMock
    ) as mock_get_last_story:
        mock_get_story_config.return_value = StoryConfig(
            heuristic_expression="{{ var1 }} + {{ var2 }} == 30", cool_off_duration=1
        )
        mock_get_last_story.return_value = None

        is_salient, in_cool_off, is_heuristic = await evaluator.evaluate(variables)
        assert is_salient is True
        assert in_cool_off is False
        assert is_heuristic is True


@pytest.mark.asyncio
async def test_evaluate_cool_off(evaluator):
    with patch.object(
        evaluator.story_config_crud, "get_story_config", new_callable=AsyncMock
    ) as mock_get_story_config, patch.object(
        evaluator.story_crud, "get_latest_story", new_callable=AsyncMock
    ) as mock_get_last_story, patch(
        "story_manager.core.heuristics.calculate_periods_count", return_value=0
    ):
        mock_get_story_config.return_value = StoryConfig(cool_off_duration=1)
        mock_story = Story(story_date=datetime.now() - timedelta(hours=12))
        mock_get_last_story.return_value = mock_story

        in_cool_off, is_heuristic = await evaluator._evaluate_cool_off(is_salient=True)
        assert in_cool_off is True
        assert is_heuristic is False


@pytest.mark.asyncio
async def test_evaluate_with_cool_off(evaluator):
    variables = {"var1": 10, "var2": 20}

    with patch.object(
        evaluator.story_config_crud, "get_story_config", new_callable=AsyncMock
    ) as mock_get_story_config, patch.object(
        evaluator.story_crud, "get_latest_story", new_callable=AsyncMock
    ) as mock_get_last_story, patch(
        "story_manager.core.heuristics.calculate_periods_count", return_value=0
    ):
        mock_get_story_config.return_value = StoryConfig(
            heuristic_expression="{{ var1 }} + {{ var2 }} == 30", cool_off_duration=1
        )
        mock_story = Story(story_date=datetime.now() - timedelta(hours=12))
        mock_get_last_story.return_value = mock_story

        is_salient, in_cool_off, is_heuristic = await evaluator.evaluate(variables)
        assert is_salient is True
        assert in_cool_off is True
        assert is_heuristic is False

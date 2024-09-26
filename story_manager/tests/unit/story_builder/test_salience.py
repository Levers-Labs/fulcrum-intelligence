from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.salience import SalienceEvaluator


@pytest.fixture
def mock_session():
    session = MagicMock()
    session.close = AsyncMock()  # Ensure close is awaitable
    return session


@pytest.fixture
def evaluator(mock_session):
    return SalienceEvaluator(StoryType.STABLE_TREND, Granularity.DAY, mock_session)


def test_render_expression(evaluator):
    variables = {"var1": 10, "var2": 20}
    expression = "{{ var1 }} + {{ var2 }}"
    rendered = evaluator.render_expression(expression, variables)
    assert rendered == "10 + 20"


def test_evaluate_expression():
    expression = "10 + 20 == 30"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is True

    expression = "10 + 20 == 25"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) < 25"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) == 30 and 5 < 10"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is True

    expression = "(10 + 20) == 30 and 5 > 10"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is False

    expression = "(10 + 20) == 30 or 5 > 10"
    result = SalienceEvaluator.evaluate_expression(expression)
    assert result is True


@pytest.mark.asyncio
async def test_evaluate_salience(evaluator, mock_session):
    variables = {"var1": 10, "var2": 20}
    mock_crud = MagicMock()
    mock_crud.get_heuristic_expression = AsyncMock(return_value="{{ var1 }} + {{ var2 }} == 30")

    with patch("story_manager.story_builder.salience.CRUDStoryConfigDep", return_value=mock_crud):
        result = await evaluator.evaluate_salience(variables)
        assert result is True

    mock_crud.get_heuristic_expression = AsyncMock(return_value="{{ var1 }} + {{ var2 }} == 25")

    with patch("story_manager.story_builder.salience.CRUDStoryConfigDep", return_value=mock_crud):
        result = await evaluator.evaluate_salience(variables)
        assert result is False

    mock_crud.get_heuristic_expression = AsyncMock(return_value=None)

    with patch("story_manager.story_builder.salience.CRUDStoryConfigDep", return_value=mock_crud):
        result = await evaluator.evaluate_salience(variables)
        assert result is True

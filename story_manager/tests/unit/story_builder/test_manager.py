from unittest.mock import MagicMock, patch

import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup
from story_manager.story_builder import StoryBuilderBase, StoryManager


@pytest.fixture
def story_manager(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session):
    return StoryManager(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session)


@pytest.mark.asyncio
async def test_story_manager_run_all_builders(story_manager, mock_query_service):
    with patch("story_manager.story_builder.manager.StoryFactory.create_story_builder") as mock_create_story_builder:
        mock_story_builder = MagicMock(spec=StoryBuilderBase)
        mock_story_builder.supported_grains = [Granularity.DAY, Granularity.WEEK]
        mock_create_story_builder.return_value = mock_story_builder

        await story_manager.run_all_builders()

        mock_query_service.list_metrics.assert_called_once()
        assert mock_create_story_builder.call_count == len(StoryGroup.__members__)


@pytest.mark.asyncio
async def test_story_manager_run_builder_for_metrics(story_manager, mock_query_service):
    mock_story_builder = MagicMock(spec=StoryBuilderBase)
    mock_story_builder.supported_grains = [Granularity.DAY, Granularity.WEEK]

    metrics = mock_query_service.list_metrics.return_value
    await story_manager._run_builder_for_metrics(mock_story_builder, metrics)

    expected_calls = [
        ((metric["id"], grain), {}) for metric in metrics for grain in mock_story_builder.supported_grains
    ]
    assert mock_story_builder.run.call_args_list == expected_calls


@pytest.mark.asyncio
async def test_story_manager_run_builder_for_metrics_error(story_manager, mock_query_service, caplog):
    mock_story_builder = MagicMock(spec=StoryBuilderBase)
    mock_story_builder.supported_grains = [Granularity.DAY]
    mock_story_builder.run.side_effect = ValueError("Test error")

    metrics = mock_query_service.list_metrics.return_value
    await story_manager._run_builder_for_metrics(mock_story_builder, metrics)

    assert len(caplog.records) == len(metrics)
    for record in caplog.records:
        assert record.levelname == "ERROR"
        assert "Error generating stories for metric" in record.message

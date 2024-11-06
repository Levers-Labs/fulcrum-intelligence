import datetime
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
        ((metric["metric_id"], grain), {}) for metric in metrics for grain in mock_story_builder.supported_grains
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


@pytest.mark.asyncio
async def test_run_builder_for_story_group(
    mocker, mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session
):
    # Mock get_query_manager_client
    mocker.patch("story_manager.story_builder.manager.get_query_manager_client", return_value=mock_query_service)

    # Mock get_analysis_manager_client
    mocker.patch("story_manager.story_builder.manager.get_analysis_manager_client", return_value=mock_analysis_service)

    # Mock get_analysis_manager
    mocker.patch("story_manager.story_builder.manager.get_analysis_manager", return_value=mock_analysis_manager)

    # Mock get_async_session
    mock_session_context = mocker.AsyncMock()
    mock_session_context.__aenter__.return_value = mock_db_session
    mocker.patch("story_manager.story_builder.manager.get_async_session", return_value=mock_session_context)

    # Mock StoryFactory.create_story_builder
    mock_story_builder = mocker.AsyncMock(spec=StoryBuilderBase)
    mock_story_builder.supported_grains = [Granularity.DAY, Granularity.WEEK]
    mock_create_story_builder = mocker.patch(
        "story_manager.story_builder.manager.StoryFactory.create_story_builder", return_value=mock_story_builder
    )

    # Test parameters
    test_group = StoryGroup.TREND_CHANGES
    test_metric_id = "test_metric_id"
    test_grain = Granularity.DAY

    # Run the method
    await StoryManager.run_builder_for_story_group(
        test_group, test_metric_id, test_grain, story_date=datetime.date.today()
    )

    # Assertions
    mock_create_story_builder.assert_called_once()
    call_args = mock_create_story_builder.call_args[0]
    call_kwargs = mock_create_story_builder.call_args[1]

    assert call_args[0] == test_group
    assert call_args[1] == mock_query_service
    assert call_args[2] == mock_analysis_service
    assert call_kwargs["analysis_manager"] == mock_analysis_manager
    assert isinstance(call_kwargs["db_session"], mocker.AsyncMock)
    assert call_kwargs["story_date"] == datetime.date.today()

    mock_story_builder.run.assert_called_once_with(test_metric_id, test_grain)


@pytest.mark.asyncio
async def test_run_builder_for_story_group_error_handling(
    mocker, mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, caplog
):
    # Mock dependencies (same as previous tests)
    mocker.patch("story_manager.story_builder.manager.get_query_manager_client", return_value=mock_query_service)
    mocker.patch("story_manager.story_builder.manager.get_analysis_manager_client", return_value=mock_analysis_service)
    mocker.patch("story_manager.story_builder.manager.get_analysis_manager", return_value=mock_analysis_manager)
    mock_session_context = mocker.AsyncMock()
    mock_session_context.__aenter__.return_value = mock_db_session
    mocker.patch("story_manager.story_builder.manager.get_async_session", return_value=mock_session_context)

    # Mock StoryFactory.create_story_builder
    mock_story_builder = mocker.AsyncMock(spec=StoryBuilderBase)
    mock_story_builder.supported_grains = [Granularity.DAY]
    mock_story_builder.run.side_effect = ValueError("Test error")
    mocker.patch(
        "story_manager.story_builder.manager.StoryFactory.create_story_builder", return_value=mock_story_builder
    )

    # Test parameters
    test_group = StoryGroup.TREND_CHANGES
    test_metric_id = "test_metric_id"

    # Run the method
    with pytest.raises(ValueError, match="Test error"):
        await StoryManager.run_builder_for_story_group(test_group, test_metric_id, Granularity.DAY)

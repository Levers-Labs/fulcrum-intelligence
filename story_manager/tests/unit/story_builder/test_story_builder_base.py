from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre
from story_manager.story_builder import StoryBuilderBase


@pytest.fixture
def story_builder(mock_query_service, mock_analysis_service, mock_db_session):
    class ConcreteStoryBuilder(StoryBuilderBase):
        genre = StoryGenre.GROWTH
        supported_grains = [Granularity.DAY]

        def generate_stories(self, metric_id: str, grain: Granularity) -> list:
            return []

    return ConcreteStoryBuilder(mock_query_service, mock_analysis_service, mock_db_session)


def test_story_builder_run_unsupported_grain(story_builder):
    with pytest.raises(ValueError) as excinfo:
        story_builder.run("metric1", Granularity.WEEK)
    assert str(excinfo.value) == "Unsupported grain 'week' for story genre 'GROWTH'"


def test_story_builder_run_success(story_builder, mock_db_session):
    with patch.object(story_builder, "generate_stories", return_value=[{"id": 1}, {"id": 2}]):
        story_builder.run("metric1", Granularity.DAY)
        mock_db_session.add_all.assert_called_once_with([{"id": 1}, {"id": 2}])
        mock_db_session.commit.assert_called_once()


def test_story_builder_get_time_series_data(story_builder, mock_query_service):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 3)
    time_series_df = story_builder._get_time_series_data("metric1", Granularity.DAY, start_date, end_date)
    mock_query_service.get_metric_values.assert_called_once_with("metric1", Granularity.DAY, start_date, end_date)
    assert isinstance(time_series_df, pd.DataFrame)
    assert time_series_df.index.name == "date"
    assert list(time_series_df.columns) == ["value"]


def test_story_builder_persist_stories(story_builder, mock_db_session):
    stories = [{"id": 1}, {"id": 2}]
    story_builder.persist_stories(stories)
    mock_db_session.add_all.assert_called_once_with(stories)
    mock_db_session.commit.assert_called_once()

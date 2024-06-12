from datetime import date, datetime
from unittest.mock import ANY, patch

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Movement,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.story_builder import StoryBuilderBase


@pytest.fixture
def story_builder(mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_story_date):
    class ConcreteStoryBuilder(StoryBuilderBase):
        genre = StoryGenre.GROWTH
        group = StoryGroup.GROWTH_RATES
        supported_grains = [Granularity.DAY]

        async def generate_stories(self, metric_id: str, grain: Granularity) -> list:
            return []

    return ConcreteStoryBuilder(
        mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_story_date
    )


@pytest.mark.asyncio
async def test_story_builder_run_unsupported_grain(story_builder):
    with pytest.raises(ValueError) as excinfo:
        await story_builder.run("metric1", Granularity.WEEK)
    assert str(excinfo.value) == "Unsupported grain 'week' for story genre 'GROWTH' of story group 'GROWTH_RATES'"


@pytest.mark.asyncio
async def test_story_builder_run_success(story_builder, mock_db_session, mock_stories):
    with patch.object(story_builder, "generate_stories", return_value=mock_stories):
        await story_builder.run("metric1", "day")
        assert mock_db_session.add_all.called
        assert len(mock_db_session.add_all.call_args[0][0]) == 2  # Two stories should be added
        assert mock_db_session.commit.called


@pytest.mark.asyncio
async def test_story_builder_get_time_series_data(story_builder, mock_query_service):
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 3)
    time_series_df = await story_builder._get_time_series_data("metric1", Granularity.DAY, start_date, end_date)
    mock_query_service.get_metric_time_series.assert_called_once_with(
        "metric1", start_date=start_date, end_date=end_date, grain=Granularity.DAY
    )
    assert isinstance(time_series_df, pd.DataFrame)
    assert list(time_series_df.columns) == ["date", "value"]

    # set index true
    time_series_df = await story_builder._get_time_series_data(
        "metric1", Granularity.DAY, start_date, end_date, set_index=True
    )
    assert time_series_df.index.name == "date"
    assert list(time_series_df.columns) == ["value"]


@pytest.mark.asyncio
async def test_persist_stories(
    story_builder, mock_query_service, mock_analysis_service, mock_analysis_manager, mock_db_session, mock_stories
):
    await story_builder.persist_stories(mock_stories)

    assert mock_db_session.add_all.called
    assert len(mock_db_session.add_all.call_args[0][0]) == 2  # Two stories should be added
    assert mock_db_session.commit.called

    added_stories = mock_db_session.add_all.call_args[0][0]
    for story_dict, story_obj in zip(mock_stories, added_stories):
        assert story_obj.metric_id == story_dict["metric_id"]
        assert story_obj.genre == story_dict["genre"]
        assert story_obj.story_group == story_dict["story_group"]
        assert story_obj.story_type == story_dict["story_type"]


def test_get_current_period_range_day(story_builder):
    grain = Granularity.DAY
    story_date = date(2023, 4, 17)
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)
    assert start_date == date(2023, 4, 16)
    assert end_date == date(2023, 4, 16)


def test_get_current_period_range_week(story_builder):
    # prepare
    grain = Granularity.WEEK
    story_date = date(2024, 4, 17)
    expected_start_date = date(2024, 4, 8)
    expected_end_date = date(2024, 4, 14)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_month(story_builder):
    # prepare
    grain = Granularity.MONTH
    story_date = date(2024, 4, 17)
    expected_start_date = date(2024, 3, 1)
    expected_end_date = date(2024, 3, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_quarter(story_builder):
    # prepare
    grain = Granularity.QUARTER
    story_date = date(2024, 4, 17)
    expected_start_date = date(2024, 1, 1)
    expected_end_date = date(2024, 3, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date

    # Prepare
    story_date = date(2024, 1, 17)
    expected_start_date = date(2023, 10, 1)
    expected_end_date = date(2023, 12, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_year(story_builder):
    # prepare
    grain = Granularity.YEAR
    story_date = date(2024, 4, 17)
    expected_start_date = date(2023, 1, 1)
    expected_end_date = date(2023, 12, 31)

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain, story_date)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_current_period_range_invalid_grain(story_builder):
    # prepare
    grain = "invalid"
    story_date = date(2024, 4, 17)

    # Act & Assert
    with pytest.raises(ValueError):
        story_builder._get_current_period_range(grain, story_date)  # type: ignore


def test_get_current_period_range_from_today(story_builder):
    # prepare
    grain = Granularity.DAY
    today = date.today()
    expected_start_date = (today - pd.DateOffset(days=1)).date()
    expected_end_date = expected_start_date

    # Act
    start_date, end_date = story_builder._get_current_period_range(grain)
    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date


def test_get_story_context(story_builder):
    # prepare
    grain = Granularity.DAY
    metric = {"id": "metric1", "label": "Metric 1"}

    # Act
    context = story_builder.get_story_context(grain, metric, key="value")

    # Assert
    assert context == {
        "grain": grain.value,
        "eoi": "EOD",
        "metric": {"id": metric["id"], "label": metric["label"]},
        "pop": "d/d",
        "interval": "daily",
        "key": "value",
    }


def test_render_story_texts(story_builder):
    # prepare
    story_type = StoryType.STABLE_TREND
    grain = Granularity.DAY
    metric = {"id": "metric1", "label": "NewBizDeals"}
    variables = {
        "avg_growth": 10,
        "trend_duration": 30,
        "movement": Movement.INCREASE.value,
        "pop": "d/d",
        "interval": "daily",
        "eoi": "EOD",
        "metric": {"id": "metric1", "label": "NewBizDeals"},
        "grain": "day",
    }

    # Act
    story_texts = story_builder._render_story_texts(
        story_type, grain, metric, avg_growth=10, trend_duration=30, movement=Movement.INCREASE.value
    )

    # Assert
    assert "title_template" in story_texts
    assert "detail_template" in story_texts
    assert story_texts["variables"] == variables
    assert story_texts["title"] == "Following a stable trend"
    assert story_texts["detail"] == (
        "NewBizDeals continues to follow the trend line it has followed "
        "for the past 30 days, averaging a 10% d/d increase."
    )


def test_prepare_story_dict(story_builder):
    # prepare
    story_builder.group = StoryGroup.TREND_CHANGES
    story_builder.genre = StoryGenre.TRENDS
    story_type = StoryType.STABLE_TREND
    grain = Granularity.DAY
    metric = {"id": "metric1", "label": "NewBizDeals"}
    df = pd.DataFrame(
        {
            "value": [10, 20, 30, 40, 50],
        },
        index=pd.date_range(start="2023-01-01", periods=5, freq="D"),
    )
    avg_growth = 10
    trend_duration = 30
    movement = Movement.INCREASE.value

    # Act
    story_dict = story_builder.prepare_story_dict(
        story_type,
        grain,
        metric,
        df,
        story_date=datetime(2023, 1, 1),
        avg_growth=avg_growth,
        trend_duration=trend_duration,
        movement=movement,
    )

    # Assert
    assert story_dict == {
        "genre": StoryGenre.TRENDS,
        "story_group": StoryGroup.TREND_CHANGES,
        "story_type": story_type,
        "story_date": datetime(2023, 1, 1),
        "grain": grain,
        "metric_id": "metric1",
        "series": df.to_dict(orient="records"),
        "variables": ANY,
        "title": ANY,
        "detail": ANY,
        "title_template": ANY,
        "detail_template": ANY,
    }


def test_get_time_durations(story_builder):
    # prepare
    story_builder.genre = StoryGenre.TRENDS
    story_builder.group = StoryGroup.TREND_CHANGES
    grain = Granularity.DAY

    # Act
    time_durations = story_builder.get_time_durations(grain)

    # Assert
    assert time_durations == {"input": 90, "min": 30, "output": 20}


def test_get_time_durations_value_error(story_builder):
    # prepare
    story_builder.genre = StoryGenre.TRENDS
    story_builder.group = StoryGroup.TREND_CHANGES
    grain = Granularity.YEAR

    # Act & Assert
    with pytest.raises(ValueError):
        story_builder.get_time_durations(grain)


def test_get_input_time_range(story_builder):
    # prepare
    story_builder.genre = StoryGenre.TRENDS
    story_builder.group = StoryGroup.TREND_CHANGES
    grain = Granularity.MONTH
    expected_end_date = date(2023, 3, 31)
    durations = story_builder.get_time_durations(grain)
    expected_start_date = (expected_end_date.replace(day=1) - pd.DateOffset(months=durations["input"])).date()

    # Act
    start_date, end_date = story_builder._get_input_time_range(grain)

    # Assert
    assert start_date == expected_start_date
    assert end_date == expected_end_date

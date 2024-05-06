import json
import pathlib

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.config import Paths
from story_manager.core.enums import StoryGenre, StoryType
from story_manager.story_builder.plugins import TrendsStoryBuilder


@pytest.fixture
def mock_process_control_output(trend_type):
    trend_files = {
        "normal": "process_control_normal_trend.json",
        "upward": "process_control_upward_trend.json",
        "downward": "process_control_downward_trend.json",
        "sticky": "process_control_sticky_trend.json",
    }

    file_path = trend_files.get(trend_type)
    if file_path is None:
        raise ValueError(f"Invalid trend_type: {trend_type}")

    with open(pathlib.Path.joinpath(Paths.BASE_DIR, f"data/{file_path}")) as fr:
        process_control_output = json.loads(fr.read())
    df = pd.DataFrame(process_control_output)
    df["slope"] = 0.0
    df["has_discontinuity"] = False
    df["growth_rate"] = 0.0
    return df


@pytest.fixture
def trends_story_builder(mock_query_service, mock_analysis_service, mock_db_session):
    return TrendsStoryBuilder(mock_query_service, mock_analysis_service, mock_db_session)


@pytest.mark.parametrize("trend_type", ["normal"])
def test_trends_story_builder_analyze_new_normal_trend(trends_story_builder, mock_process_control_output, trend_type):
    trends_df = trends_story_builder._analyze_trends(mock_process_control_output, "test_metric", Granularity.DAY)

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.NEW_NORMAL
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


@pytest.mark.parametrize("trend_type", ["upward"])
def test_trends_story_builder_analyze_upward_trend(trends_story_builder, mock_process_control_output, trend_type):
    trends_df = trends_story_builder._analyze_trends(mock_process_control_output, "test_metric", Granularity.DAY)

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.NEW_UPWARD_TREND
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


@pytest.mark.parametrize("trend_type", ["downward"])
def test_trends_story_builder_analyze_downward_trend(trends_story_builder, mock_process_control_output, trend_type):
    trends_df = trends_story_builder._analyze_trends(mock_process_control_output, "test_metric", Granularity.DAY)

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.NEW_DOWNWARD_TREND
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


@pytest.mark.parametrize("trend_type", ["sticky"])
def test_trends_story_builder_analyze_sticky_downward_trend(
    trends_story_builder, mock_process_control_output, trend_type
):
    trends_df = trends_story_builder._analyze_trends(mock_process_control_output, "test_metric", Granularity.DAY)

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.STICKY_DOWNWARD_TREND
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


def test_trends_story_builder_analyze_for_empty_data(trends_story_builder):
    mock_df_data = []

    trends_df = trends_story_builder._analyze_trends(pd.DataFrame(mock_df_data), "test_metric", Granularity.DAY)

    assert len(trends_df) == 0

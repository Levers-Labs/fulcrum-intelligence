from datetime import date

import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryType
from story_manager.story_builder.plugins import TrendsStoryBuilder

start_date = date(2024, 4, 7)
end_date = date(2024, 5, 7)


@pytest.fixture
def trends_story_builder(mock_query_service, mock_analysis_service, mock_db_session):
    return TrendsStoryBuilder(mock_query_service, mock_analysis_service, mock_db_session)


@pytest.fixture
def mock_process_control_output(trend_type):
    trend_data = {
        "upward": [
            {
                "date": "2024-02-05",
                "metric_id": "test_metric",
                "value": 3332,
                "central_line": 3529.48,
                "trend_type": "",
            },
            {
                "date": "2024-02-22",
                "metric_id": "test_metric",
                "value": 3576,
                "central_line": 3614.56,
                "trend_type": "",
            },
            {
                "date": "2024-04-14",
                "metric_id": "test_metric",
                "value": 3646,
                "central_line": 3699.63,
                "trend_type": "",
            },
            {
                "date": "2024-02-09",
                "metric_id": "test_metric",
                "value": 4026,
                "central_line": 3784.7,
                "trend_type": "",
            },
            {
                "date": "2024-02-18",
                "metric_id": "test_metric",
                "value": 3841,
                "central_line": 3869.78,
                "trend_type": "",
            },
            {
                "date": "2024-03-18",
                "metric_id": "test_metric",
                "value": 3315,
                "central_line": 3954.85,
                "trend_type": "",
            },
            {
                "date": "2024-04-11",
                "metric_id": "test_metric",
                "value": 3843,
                "central_line": 4039.93,
                "trend_type": "",
            },
        ],
        "downward": [
            {
                "date": "2024-04-11",
                "metric_id": "test_metric",
                "value": 3843,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-12",
                "metric_id": "test_metric",
                "value": 3800,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-13",
                "metric_id": "test_metric",
                "value": 3775,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-15",
                "metric_id": "test_metric",
                "value": 3725,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-16",
                "metric_id": "test_metric",
                "value": 3700,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-17",
                "metric_id": "test_metric",
                "value": 3675,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-18",
                "metric_id": "test_metric",
                "value": 3650,
                "central_line": 4039.93,
                "trend_type": "",
            },
            {
                "date": "2024-04-19",
                "metric_id": "test_metric",
                "value": 3550,
                "central_line": 4039.93,
                "trend_type": "",
            },
        ],
    }

    if trend_type not in trend_data:
        raise ValueError(f"Invalid trend_type: {trend_type}")

    df = pd.DataFrame(trend_data[trend_type])
    df["slope"] = 0.0
    df["has_discontinuity"] = False
    df["growth_rate"] = 0.0
    return df


@pytest.mark.parametrize("trend_type", ["upward"])
def test_trends_story_builder_analyze_upward_trend(trends_story_builder, mock_process_control_output, trend_type):
    trends_df = trends_story_builder._analyze_trends(
        mock_process_control_output, "test_metric", Granularity.DAY, start_date, end_date
    )

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.NEW_UPWARD_TREND
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


@pytest.mark.parametrize("trend_type", ["downward"])
def test_trends_story_builder_analyze_downward_trend(trends_story_builder, mock_process_control_output, trend_type):
    trends_df = trends_story_builder._analyze_trends(
        mock_process_control_output, "test_metric", Granularity.DAY, start_date, end_date
    )

    assert trends_df[0]["metric_id"] == "test_metric"
    assert trends_df[0]["type"] == StoryType.NEW_DOWNWARD_TREND
    assert trends_df[0]["genre"] == StoryGenre.TRENDS


def test_trends_story_builder_analyze_for_empty_data(trends_story_builder):
    mock_df_data = []

    trends_df = trends_story_builder._analyze_trends(
        pd.DataFrame(mock_df_data), "test_metric", Granularity.DAY, start_date, end_date
    )

    assert len(trends_df) == 0


@pytest.mark.parametrize(
    "grain, expected_start_date",
    [
        ("day", date(2024, 4, 7)),
        ("week", date(2024, 3, 12)),
    ],
)
def test_get_sliding_start_date(grain, expected_start_date):
    curr_start_date = end_date
    actual_start_date = TrendsStoryBuilder._get_sliding_start_date(curr_start_date, grain)

    assert actual_start_date == expected_start_date


def test_has_discontinuity_condition(trends_story_builder):
    series_df = pd.DataFrame(
        {
            "has_discontinuity": [False, False, False, False, False, False, False],
            "central_line": [100, 100, 100, 100, 100, 100, 100],
            "value": [110, 105, 103, 102, 101, 100, 99],
        }
    )
    assert trends_story_builder._has_discontinuity_condition(series_df, 6)


@pytest.mark.parametrize(
    "prev_slope, slope, slope_change, expected_trend_type",
    [
        (0.0, 100.0, 100.0, StoryType.NEW_UPWARD_TREND),
        (10.0, 0.0, -100.0, StoryType.NEW_DOWNWARD_TREND),
    ],
)
def test_identify_trend_type(trends_story_builder, prev_slope, slope, slope_change, expected_trend_type):
    assert trends_story_builder._identify_trend_type(prev_slope, slope, slope_change) == expected_trend_type

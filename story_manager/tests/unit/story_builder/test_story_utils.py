from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType
from story_manager.story_builder.utils import (
    calculate_periods_count,
    determine_status_for_value_and_target,
    get_target_value_for_date,
)


def test_get_target_value_for_date():
    # Prepare
    target_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2022-01-10", periods=5, freq="D"),
            "target": [100, 200, 300, 400, 500],
        }
    )
    ref_date = datetime(2022, 1, 12).date()

    # Act
    target_value = get_target_value_for_date(target_df, ref_date)

    # Assert
    assert target_value == 300

    # Prepare
    ref_date = datetime(2022, 1, 9).date()

    # Act
    target_value = get_target_value_for_date(target_df, ref_date)

    # Assert
    assert target_value is None


def test_determine_status_for_value_and_target():
    df = pd.DataFrame(
        {
            "value": [np.NaN, 200, 300, 200, np.NaN],
            "target": [100, np.NaN, 300, 400, np.NaN],
            "expected": [None, None, StoryType.ON_TRACK, StoryType.OFF_TRACK, None],
        }
    )

    for _, row in df.iterrows():
        res = determine_status_for_value_and_target(row)
        assert res == row["expected"]


def test_calculate_periods_count_day_grain():
    from_date = date(2024, 6, 6)
    to_date = date(2024, 6, 1)
    assert calculate_periods_count(from_date, to_date, Granularity.DAY) == 5


def test_calculate_periods_count_week_grain():
    from_date = date(2024, 6, 6)
    to_date = date(2024, 5, 23)
    assert calculate_periods_count(from_date, to_date, Granularity.WEEK) == 2


def test_calculate_periods_count_month_grain():
    from_date = date(2024, 6, 6)
    to_date = date(2024, 3, 6)
    assert calculate_periods_count(from_date, to_date, Granularity.MONTH) == 3


def test_calculate_periods_count_invalid_grain():
    from_date = date(2024, 6, 6)
    to_date = date(2024, 6, 1)
    with pytest.raises(ValueError, match="Unsupported grain: year"):
        calculate_periods_count(from_date, to_date, "year")


def test_calculate_periods_count_same_start_to_date():
    date_value = date(2024, 6, 6)
    assert calculate_periods_count(date_value, date_value, Granularity.DAY) == 0
    assert calculate_periods_count(date_value, date_value, Granularity.WEEK) == 0
    assert calculate_periods_count(date_value, date_value, Granularity.MONTH) == 0


def test_calculate_periods_count_cross_year_boundary():
    from_date = date(2024, 1, 1)
    to_date = date(2023, 12, 1)
    assert calculate_periods_count(from_date, to_date, Granularity.MONTH) == 1


def test_calculate_periods_count_partial_weeks():
    from_date = date(2024, 6, 6)
    to_date = date(2024, 6, 1)
    assert calculate_periods_count(from_date, to_date, Granularity.WEEK) == 1


def test_calculate_periods_count_negative_difference():
    from_date = date(2024, 6, 1)
    to_date = date(2024, 6, 6)
    assert calculate_periods_count(from_date, to_date, Granularity.DAY) == 5
    assert calculate_periods_count(from_date, to_date, Granularity.WEEK) == 0
    assert calculate_periods_count(from_date, to_date, Granularity.MONTH) == 0


def test_calculate_periods_count_cross_month_boundary_in_weeks():
    from_date = date(2024, 7, 2)
    to_date = date(2024, 6, 26)
    assert calculate_periods_count(from_date, to_date, Granularity.WEEK) == 1

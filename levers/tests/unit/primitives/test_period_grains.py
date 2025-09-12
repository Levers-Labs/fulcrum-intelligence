from datetime import date

import pandas as pd
import pytest

from levers.exceptions import ValidationError
from levers.models import Granularity
from levers.primitives.period_grains import (
    get_date_range_from_window,
    get_period_length_for_grain,
    get_period_range_for_grain,
    get_prev_period_start_date,
    get_prior_period_range,
)


class TestGetPeriodRangeForGrain:
    """Tests for the get_period_range_for_grain function."""

    def test_day_grain(self):
        analysis_date = date(2024, 3, 15)
        start, end = get_period_range_for_grain(Granularity.DAY, analysis_date, include_today=True)
        assert start == pd.Timestamp("2024-03-15")
        assert end == pd.Timestamp("2024-03-15")

    def test_week_grain(self):
        analysis_date = date(2024, 3, 13)  # Wednesday
        start, end = get_period_range_for_grain(Granularity.WEEK, analysis_date, include_today=True)
        assert start == pd.Timestamp("2024-03-11")  # Monday
        assert end == pd.Timestamp("2024-03-17")  # Sunday

    def test_month_grain(self):
        analysis_date = date(2024, 2, 10)
        start, end = get_period_range_for_grain(Granularity.MONTH, analysis_date)
        assert start == pd.Timestamp("2024-02-01")
        assert end == pd.Timestamp("2024-02-29")

    def test_quarter_grain(self):
        analysis_date = date(2024, 5, 20)
        start, end = get_period_range_for_grain(Granularity.QUARTER, analysis_date, include_today=True)
        assert start == pd.Timestamp("2024-04-01")
        assert end == pd.Timestamp("2024-06-30")

    def test_year_grain(self):
        analysis_date = date(2024, 8, 25)
        start, end = get_period_range_for_grain(Granularity.YEAR, analysis_date, include_today=True)
        assert start == pd.Timestamp("2024-01-01")
        assert end == pd.Timestamp("2024-12-31")

    def test_invalid_date(self):
        with pytest.raises(ValidationError):
            get_period_range_for_grain(Granularity.DAY, "invalid-date")

    def test_invalid_grain_string(self):
        with pytest.raises(ValidationError):
            get_period_range_for_grain(Granularity.DAY, "invalid")

    def test_day_grain_include_today_false(self):
        analysis_date = date(2024, 3, 15)
        start, end = get_period_range_for_grain(Granularity.DAY, analysis_date)
        assert start == pd.Timestamp("2024-03-14")
        assert end == pd.Timestamp("2024-03-14")

    def test_week_grain_include_today_false(self):
        analysis_date = date(2024, 3, 13)  # Wednesday
        start, end = get_period_range_for_grain(Granularity.WEEK, analysis_date)
        assert start == pd.Timestamp("2024-03-04")  # Monday
        assert end == pd.Timestamp("2024-03-10")  # Sunday

    def test_month_grain_include_today_false(self):
        analysis_date = date(2024, 2, 10)
        start, end = get_period_range_for_grain(Granularity.MONTH, analysis_date)
        assert start == pd.Timestamp("2024-02-01")
        assert end == pd.Timestamp("2024-02-29")

    def test_quarter_grain_include_today_false(self):
        analysis_date = date(2024, 5, 20)
        start, end = get_period_range_for_grain(Granularity.QUARTER, analysis_date)
        assert start == pd.Timestamp("2024-01-01")
        assert end == pd.Timestamp("2024-03-31")

    def test_year_grain_include_today_false(self):
        analysis_date = date(2024, 8, 25)
        start, end = get_period_range_for_grain(Granularity.YEAR, analysis_date)
        assert start == pd.Timestamp("2023-01-01")
        assert end == pd.Timestamp("2023-12-31")


class TestGetPriorPeriodRange:
    """Tests for the get_prior_period_range function."""

    def test_prior_week(self):
        start = pd.Timestamp("2024-03-11")
        end = pd.Timestamp("2024-03-17")
        prior_start, prior_end = get_prior_period_range(start, end, Granularity.WEEK)
        assert prior_start == pd.Timestamp("2024-03-04")
        assert prior_end == pd.Timestamp("2024-03-10")

    def test_prior_month(self):
        start = pd.Timestamp("2024-04-01")
        end = pd.Timestamp("2024-04-30")
        prior_start, prior_end = get_prior_period_range(start, end, Granularity.MONTH)
        assert prior_start == pd.Timestamp("2024-03-01")
        assert prior_end == pd.Timestamp("2024-03-31")

    def test_invalid_timestamps(self):
        with pytest.raises(ValidationError):
            get_prior_period_range("2024-03-01", "2024-03-31", Granularity.MONTH)

    def test_invalid_grain(self):
        with pytest.raises(ValidationError):
            get_prior_period_range(pd.Timestamp("2024-03-01"), pd.Timestamp("2024-03-31"), "invalid")


class TestGetPrevPeriodStartDate:
    """Tests for the get_prev_period_start_date function."""

    def test_previous_month(self):
        latest_start = pd.Timestamp("2024-04-01")
        result = get_prev_period_start_date(Granularity.MONTH, 1, latest_start)
        assert result == date(2024, 3, 1)

    def test_previous_quarter(self):
        latest_start = pd.Timestamp("2024-10-01")
        result = get_prev_period_start_date(Granularity.QUARTER, 2, latest_start)
        assert result == date(2024, 4, 1)

    def test_invalid_grain(self):
        with pytest.raises(ValidationError):
            get_prev_period_start_date("invalid", 1, date.today())

    def test_invalid_date(self):
        with pytest.raises(ValidationError):
            get_prev_period_start_date(Granularity.MONTH, 1, "not-a-date")


class TestGetDateRangeFromWindow:
    """Tests for the get_date_range_from_window function."""

    def test_basic_window(self):
        start, end = get_date_range_from_window(7, end_date="2024-03-31")
        assert start == date(2024, 3, 25)
        assert end == date(2024, 3, 31)

    def test_include_today(self):
        today = date.today()
        start, end = get_date_range_from_window(3, include_today=True)
        assert end == today
        assert start == today - pd.Timedelta(days=2)

    def test_invalid_window_days(self):
        with pytest.raises(ValidationError):
            get_date_range_from_window(0)

    def test_invalid_end_date(self):
        with pytest.raises(ValidationError):
            get_date_range_from_window(5, end_date="not-a-date")


class TestGetPeriodLengthForGrain:
    """Tests for the get_period_length_for_grain function."""

    def test_known_lengths(self):
        assert get_period_length_for_grain(Granularity.DAY) == 1
        assert get_period_length_for_grain(Granularity.WEEK) == 7
        assert get_period_length_for_grain(Granularity.MONTH) == 30
        assert get_period_length_for_grain(Granularity.QUARTER) == 90
        assert get_period_length_for_grain(Granularity.YEAR) == 365

    def test_string_grain(self):
        assert get_period_length_for_grain("week") == 7

    def test_invalid_grain(self):
        with pytest.raises(ValidationError):
            get_period_length_for_grain("invalid-grain")

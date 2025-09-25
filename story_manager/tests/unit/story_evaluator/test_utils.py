"""
Tests for the story evaluator utils module.
"""

import pandas as pd
import pytest

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.utils import (
    format_date_column,
    format_number,
    format_ordinal,
    format_percent,
    format_segment_names,
    get_story_template,
    get_template_env,
    render_story_text,
)


def test_format_number():
    """Test format_number function."""
    # Test smart precision behavior (new default)
    assert format_number(123.4567) == "123"  # Smart: no decimals for >= 100
    assert format_number(12.3456) == "12.3"  # Smart: 1 decimal for 10-99
    assert format_number(1.2345) == "1.23"  # Smart: 2 decimals for 1-9

    # Test backward compatibility with smart_precision=False
    assert format_number(123.4567, precision=2, smart_precision=False) == "123.46"
    assert format_number(123.4567, precision=3, smart_precision=False) == "123.457"

    # Test compact notation
    assert format_number(1500) == "1.5K"
    assert format_number(1500, compact=False) == "1,500"

    assert format_number(None) == "N/A"


def test_format_percent():
    """Test format_percent function."""
    assert format_percent(12.3456) == "12.3"
    assert format_percent(12.3456, precision=2) == "12.35"
    assert format_percent(None) == "N/A"


def test_format_ordinal():
    """Test format_ordinal function."""
    assert "1" in format_ordinal(1)
    assert "2" in format_ordinal(2)
    assert "3" in format_ordinal(3)
    assert "4" in format_ordinal(4)
    assert "11" in format_ordinal(11)
    assert "21" in format_ordinal(21)
    assert format_ordinal(None) == "N/A"


def test_get_template_env():
    """Test get_template_env function."""
    env = get_template_env()

    # Test format_number filter with new smart behavior
    assert env.filters["format_number"](123.4567) == "123"  # Smart precision: no decimals for >= 100

    # Test format_percent filter
    assert env.filters["format_percent"](12.3456) == "12.3"

    # Test abs filter
    assert env.filters["abs"](-123) == 123


def test_get_story_template():
    """Test get_story_template function."""
    template = get_story_template(StoryType.ON_TRACK, "title")
    assert "is on track" in template.render(metric={"label": "Test Metric", "metric_id": "test_metric", "unit": "n"})

    template = get_story_template(StoryType.ON_TRACK, "detail")
    assert "is at" in template.render(
        metric={"label": "Test Metric", "metric_id": "test_metric", "unit": "n"},
        current_value=100.0,
        trend_direction="up",
        change_percent=5.0,
        pop="m/m",
        target_value=90.0,
        performance_percent=10.0,
        streak_length=3,
        grain_label="month",
        performance_trend="improving",
    )


def test_get_story_template_invalid_type():
    """Test get_story_template function with invalid story type."""
    with pytest.raises(ValueError, match="No templates found for story type"):
        get_story_template("invalid_type", "title")  # type: ignore


def test_get_story_template_invalid_field():
    """Test get_story_template function with invalid field."""
    with pytest.raises(ValueError, match="No invalid_field template found for story type ON_TRACK"):
        get_story_template(StoryType.ON_TRACK, "invalid_field")


def test_render_story_text():
    """Test render_story_text function."""
    context = {
        "metric": {"label": "Test Metric", "metric_id": "test_metric", "unit": "n"},
        "current_value": 100.0,
        "trend_direction": "up",
        "change_percent": 5.0,
        "pop": "m/m",
        "target_value": 90.0,
        "performance_percent": 10.0,
        "streak_length": 3,
        "grain_label": "month",
        "performance_trend": "improving",
    }

    title = render_story_text(StoryType.ON_TRACK, "title", context)
    assert "Test Metric is on track" in title

    detail = render_story_text(StoryType.ON_TRACK, "detail", context)
    assert "Test Metric is at 100" in detail  # Smart precision: no decimals for >= 100
    assert "up 5.0% m/m" in detail
    assert "beating its target of 90" in detail  # Smart precision: no decimals for >= 100
    assert "by 10.0%" in detail
    assert "3-month streak" in detail
    assert "improving by 5.0% over this period" in detail


def test_format_segment_names():
    """Test format_segment_names function."""
    # Test single segment
    assert format_segment_names(["Segment A"]) == "Segment A"

    # Test two segments
    assert format_segment_names(["Segment A", "Segment B"]) == "Segment A, and Segment B"

    # Test multiple segments
    assert format_segment_names(["Segment A", "Segment B", "Segment C"]) == "Segment A, Segment B, and Segment C"

    # Test four segments
    assert (
        format_segment_names(["Segment A", "Segment B", "Segment C", "Segment D"])
        == "Segment A, Segment B, Segment C, and Segment D"
    )

    # Test with numbers as segments
    assert format_segment_names(["1", "2", "3"]) == "1, 2, and 3"

    # Test with empty strings
    assert format_segment_names(["", "", ""]) == ", , and "

    # Test empty list
    assert format_segment_names([]) == ""


def test_format_date_column():
    """Test format_date_column function."""
    # Test with default parameters
    df = pd.DataFrame({"date": ["2023-01-03", "2023-01-01", "2023-01-02"], "value": [3, 1, 2]})
    result = format_date_column(df)
    assert list(result["date"]) == ["2023-01-01", "2023-01-02", "2023-01-03"]  # Check sorting
    assert isinstance(result["date"].iloc[0], str)  # Verify ISO format string

    # Test with custom date_column
    df = pd.DataFrame({"my_date": ["2023-01-03", "2023-01-01", "2023-01-02"], "value": [3, 1, 2]})
    result = format_date_column(df, date_column="my_date")
    assert list(result["my_date"]) == ["2023-01-01", "2023-01-02", "2023-01-03"]

    # Test with sort_values=False
    df = pd.DataFrame({"date": ["2023-01-03", "2023-01-01", "2023-01-02"], "value": [3, 1, 2]})
    result = format_date_column(df, sort_values=False)
    assert list(result["date"]) == ["2023-01-03", "2023-01-01", "2023-01-02"]  # Original order preserved

    # Test with ascending=False
    df = pd.DataFrame({"date": ["2023-01-03", "2023-01-01", "2023-01-02"], "value": [3, 1, 2]})
    result = format_date_column(df, ascending=False)
    assert list(result["date"]) == ["2023-01-03", "2023-01-02", "2023-01-01"]  # Descending order

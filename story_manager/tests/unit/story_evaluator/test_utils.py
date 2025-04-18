"""
Tests for the story evaluator utils module.
"""

import pytest

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.utils import (
    format_number,
    format_percent,
    get_story_template,
    get_template_env,
    render_story_text,
)


def test_format_number():
    """Test format_number function."""
    assert format_number(123.4567) == "123.46"
    assert format_number(123.4567, precision=3) == "123.457"
    assert format_number(None) == "N/A"


def test_format_percent():
    """Test format_percent function."""
    assert format_percent(12.3456) == "12.3"
    assert format_percent(12.3456, precision=2) == "12.35"
    assert format_percent(None) == "N/A"


def test_get_template_env():
    """Test get_template_env function."""
    env = get_template_env()

    # Test format_number filter
    assert env.filters["format_number"](123.4567) == "123.46"

    # Test format_percent filter
    assert env.filters["format_percent"](12.3456) == "12.3"

    # Test abs filter
    assert env.filters["abs"](-123) == 123


def test_get_story_template():
    """Test get_story_template function."""
    template = get_story_template(StoryType.ON_TRACK, "title")
    assert "is on track" in template.render(metric={"label": "Test Metric"})

    template = get_story_template(StoryType.ON_TRACK, "detail")
    assert "is at" in template.render(
        metric={"label": "Test Metric"},
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
        get_story_template("invalid_type", "title")


def test_get_story_template_invalid_field():
    """Test get_story_template function with invalid field."""
    with pytest.raises(ValueError, match="No invalid_field template found for story type ON_TRACK"):
        get_story_template(StoryType.ON_TRACK, "invalid_field")


def test_render_story_text():
    """Test render_story_text function."""
    context = {
        "metric": {"label": "Test Metric"},
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
    assert "Test Metric is at 100.00" in detail
    assert "up 5.0% m/m" in detail
    assert "beating its target of 90.00" in detail
    assert "by 10.0%" in detail
    assert "3-month streak" in detail
    assert "improving by 5.0% over this period" in detail

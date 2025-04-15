"""
Tests for the story evaluator constants module.
"""

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.constants import STORY_TEMPLATES


def test_story_templates():
    """Test STORY_TEMPLATES constant."""
    # Test ON_TRACK template
    on_track = STORY_TEMPLATES[StoryType.ON_TRACK]
    assert "is on track" in on_track["title"]
    assert "is at" in on_track["detail"]
    assert "beating its target" in on_track["detail"]
    assert "streak of exceeding target" in on_track["detail"]

    # Test OFF_TRACK template
    off_track = STORY_TEMPLATES[StoryType.OFF_TRACK]
    assert "is off track" in off_track["title"]
    assert "is at" in off_track["detail"]
    assert "missing its target" in off_track["detail"]
    assert "gap" in off_track["detail"]

    # Test IMPROVING_STATUS template
    improving = STORY_TEMPLATES[StoryType.IMPROVING_STATUS]
    assert "status has improved" in improving["title"]
    assert "now On-Track" in improving["detail"]
    assert "previously being Off-Track" in improving["detail"]

    # Test WORSENING_STATUS template
    worsening = STORY_TEMPLATES[StoryType.WORSENING_STATUS]
    assert "status has worsened" in worsening["title"]
    assert "now Off-Track" in worsening["detail"]
    assert "previously being On-Track" in worsening["detail"]

    # Test HOLD_STEADY template
    hold_steady = STORY_TEMPLATES[StoryType.HOLD_STEADY]
    assert "needs to hold steady" in hold_steady["title"]
    assert "already performing at its target level" in hold_steady["detail"]
    assert "needs to maintain this lead" in hold_steady["detail"]

    # Test growth story templates
    slowing_growth = STORY_TEMPLATES[StoryType.SLOWING_GROWTH]
    assert "Growth is slowing down" in slowing_growth["title"]
    assert "growth has slowed" in slowing_growth["detail"]
    assert "current_growth" in slowing_growth["detail"]
    assert "average_growth" in slowing_growth["detail"]

    accelerating_growth = STORY_TEMPLATES[StoryType.ACCELERATING_GROWTH]
    assert "growth is speeding up" in accelerating_growth["title"]
    assert "growth has accelerated" in accelerating_growth["detail"]
    assert "current_growth" in accelerating_growth["detail"]
    assert "average_growth" in accelerating_growth["detail"]

    # Test trend story templates
    stable_trend = STORY_TEMPLATES[StoryType.STABLE_TREND]
    assert "Following a stable trend" in stable_trend["title"]
    assert "continues to follow the trend line" in stable_trend["detail"]
    assert "trend_duration" in stable_trend["detail"]
    assert "trend_avg_growth" in stable_trend["detail"]

    new_upward_trend = STORY_TEMPLATES[StoryType.NEW_UPWARD_TREND]
    assert "New upward trend" in new_upward_trend["title"]
    assert "has been following a new, upward trend line" in new_upward_trend["detail"]
    assert "trend_start_date" in new_upward_trend["detail"]
    assert "prev_trend_duration" in new_upward_trend["detail"]

    new_downward_trend = STORY_TEMPLATES[StoryType.NEW_DOWNWARD_TREND]
    assert "New downward trend" in new_downward_trend["title"]
    assert "has been following a new, downward trend line" in new_downward_trend["detail"]
    assert "trend_start_date" in new_downward_trend["detail"]
    assert "prev_trend_duration" in new_downward_trend["detail"]

    performance_plateau = STORY_TEMPLATES[StoryType.PERFORMANCE_PLATEAU]
    assert "Performance has leveled off" in performance_plateau["title"]
    assert "growth has steadied into a new normal" in performance_plateau["detail"]
    assert "trend_start_date" in performance_plateau["detail"]

    # Test trend exception templates
    spike = STORY_TEMPLATES[StoryType.SPIKE]
    assert "Performance spike above trend" in spike["title"]
    assert "above its normal range" in spike["detail"]
    assert "deviation_percent" in spike["detail"]

    drop = STORY_TEMPLATES[StoryType.DROP]
    assert "Performance drop below trend" in drop["title"]
    assert "below its normal range" in drop["detail"]
    assert "deviation_percent" in drop["detail"]

    # Test performance change templates
    improving_performance = STORY_TEMPLATES[StoryType.IMPROVING_PERFORMANCE]
    assert "Improved performance" in improving_performance["title"]
    assert "has been averaging" in improving_performance["detail"]
    assert "trend_duration" in improving_performance["detail"]
    assert "trend_avg_growth" in improving_performance["detail"]

    worsening_performance = STORY_TEMPLATES[StoryType.WORSENING_PERFORMANCE]
    assert "Worsening performance" in worsening_performance["title"]
    assert "has been declining" in worsening_performance["detail"]
    assert "trend_duration" in worsening_performance["detail"]
    assert "trend_avg_growth" in worsening_performance["detail"]

    # Test seasonality templates
    seasonal_pattern_match = STORY_TEMPLATES[StoryType.SEASONAL_PATTERN_MATCH]
    assert "Expected seasonal behavior" in seasonal_pattern_match["title"]
    assert "is following its usual seasonal pattern" in seasonal_pattern_match["detail"]
    assert "seasonal_deviation" in seasonal_pattern_match["detail"]

    seasonal_pattern_break = STORY_TEMPLATES[StoryType.SEASONAL_PATTERN_BREAK]
    assert "Unexpected seasonal deviation" in seasonal_pattern_break["title"]
    assert "is diverging from its usual seasonal trend" in seasonal_pattern_break["detail"]
    assert "seasonal_deviation" in seasonal_pattern_break["detail"]
    assert "expected_direction" in seasonal_pattern_break["detail"]
    assert "actual_direction" in seasonal_pattern_break["detail"]

    # Test record value templates
    record_high = STORY_TEMPLATES[StoryType.RECORD_HIGH]
    assert "highest value" in record_high["title"]
    assert "highest value in" in record_high["detail"]
    assert "high_rank" in record_high["title"]
    assert "high_value" in record_high["detail"]

    record_low = STORY_TEMPLATES[StoryType.RECORD_LOW]
    assert "lowest value" in record_low["title"]
    assert "lowest value in" in record_low["detail"]
    assert "low_rank" in record_low["title"]
    assert "low_value" in record_low["detail"]

    # Test benchmark template
    benchmarks = STORY_TEMPLATES[StoryType.BENCHMARKS]
    assert "Performance Against Historical Benchmarks" in benchmarks["title"]
    assert "current performance comes in" in benchmarks["detail"]
    assert "direction_1" in benchmarks["detail"]
    assert "ref_period_1" in benchmarks["detail"]
    assert "change_percent_1" in benchmarks["detail"]

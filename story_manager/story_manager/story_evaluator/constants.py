"""
Constants for story evaluators including templates for different story types.
"""

from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup, StoryType

STORY_GROUP_TIME_DURATIONS: dict[str, Any] = {
    StoryGroup.TREND_CHANGES: {
        Granularity.DAY: {"output": 20},
        Granularity.WEEK: {"output": 20},
        Granularity.MONTH: {"output": 10},
    },
    StoryGroup.TREND_EXCEPTIONS: {
        Granularity.DAY: {"output": 20},
        Granularity.WEEK: {"output": 20},
        Granularity.MONTH: {"output": 10},
    },
    StoryGroup.LONG_RANGE: {
        Granularity.DAY: {"output": 30},
        Granularity.WEEK: {"output": 14},
        Granularity.MONTH: {"output": 4},
    },
    StoryGroup.RECORD_VALUES: {
        Granularity.DAY: {"output": 20},
        Granularity.WEEK: {"output": 20},
        Granularity.MONTH: {"output": 10},
    },
    StoryGroup.GROWTH_RATES: {
        Granularity.DAY: {"output": 30},
        Granularity.WEEK: {"output": 14},
        Granularity.MONTH: {"output": 4},
    },
    StoryGroup.GOAL_VS_ACTUAL: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
    StoryGroup.STATUS_CHANGE: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
    StoryGroup.SEGMENT_DRIFT: {
        Granularity.DAY: {"output": 2},
        Granularity.WEEK: {"output": 2},
        Granularity.MONTH: {"output": 2},
    },
    StoryGroup.INFLUENCE_DRIFT: {
        Granularity.DAY: {"output": 2},
        Granularity.WEEK: {"output": 2},
        Granularity.MONTH: {"output": 2},
    },
    StoryGroup.COMPONENT_DRIFT: {
        Granularity.DAY: {"output": 2},
        Granularity.WEEK: {"output": 2},
        Granularity.MONTH: {"output": 2},
    },
    StoryGroup.BENCHMARK_COMPARISONS: {
        Granularity.DAY: {"output": 2},
        Granularity.WEEK: {"output": 53},  # 53 weeks for week grain
        Granularity.MONTH: {"output": 13},  # 13 months for month grain
    },
}

STORY_TYPE_TIME_DURATIONS: dict[str, Any] = {
    StoryType.SLOWING_GROWTH: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.ACCELERATING_GROWTH: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.STABLE_TREND: {
        Granularity.DAY: {"output": 45, "min": 30},
        Granularity.WEEK: {"output": 16, "min": 10},
        Granularity.MONTH: {"output": 9, "min": 6},
    },
    StoryType.NEW_UPWARD_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.NEW_DOWNWARD_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.PERFORMANCE_PLATEAU: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.SPIKE: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.DROP: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.IMPROVING_PERFORMANCE: {
        Granularity.DAY: {"output": 45, "min": 30},
        Granularity.WEEK: {"output": 16, "min": 10},
        Granularity.MONTH: {"output": 9, "min": 6},
    },
    StoryType.WORSENING_PERFORMANCE: {
        Granularity.DAY: {"output": 45, "min": 30},
        Granularity.WEEK: {"output": 16, "min": 10},
        Granularity.MONTH: {"output": 9, "min": 6},
    },
    StoryType.RECORD_HIGH: {
        Granularity.DAY: {"output": 21, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.RECORD_LOW: {
        Granularity.DAY: {"output": 21, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.BENCHMARKS: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.HOLD_STEADY: {
        Granularity.DAY: {"output": 45, "min": 30},
        Granularity.WEEK: {"output": 16, "min": 10},
        Granularity.MONTH: {"output": 9, "min": 6},
    },
    StoryType.NEW_STRONGEST_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.NEW_WEAKEST_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.NEW_LARGEST_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.NEW_SMALLEST_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Performance Stories
    StoryType.ON_TRACK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.OFF_TRACK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.IMPROVING_STATUS: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.WORSENING_STATUS: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.REQUIRED_PERFORMANCE: {
        Granularity.DAY: {"output": 45, "min": 30},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Likely Status Stories
    StoryType.FORECASTED_ON_TRACK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.FORECASTED_OFF_TRACK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.PACING_ON_TRACK: {
        Granularity.DAY: {"output": 21, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.PACING_OFF_TRACK: {
        Granularity.DAY: {"output": 21, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Segment Stories
    StoryType.TOP_4_SEGMENTS: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.BOTTOM_4_SEGMENTS: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.SEGMENT_COMPARISONS: {
        Granularity.DAY: {"output": 21, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Seasonal Pattern Stories
    StoryType.SEASONAL_PATTERN_MATCH: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.SEASONAL_PATTERN_BREAK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Root Causes - Segment Drift Stories
    StoryType.GROWING_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.SHRINKING_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.IMPROVING_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.WORSENING_SEGMENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Root Causes - Component Drift Stories
    StoryType.IMPROVING_COMPONENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.WORSENING_COMPONENT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Root Causes - Influence Drift Stories
    StoryType.STRONGER_INFLUENCE: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.WEAKER_INFLUENCE: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.IMPROVING_INFLUENCE: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.WORSENING_INFLUENCE: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Root Causes - Summary Stories
    StoryType.PRIMARY_ROOT_CAUSE_FACTOR: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Headwinds/Tailwinds - Leading Indicators Stories
    StoryType.UNFAVORABLE_DRIVER_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.FAVORABLE_DRIVER_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    # Headwinds/Tailwinds - Seasonality Stories
    StoryType.UNFAVORABLE_SEASONAL_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 12},
        Granularity.MONTH: {"output": 12, "min": 8},
    },
    StoryType.FAVORABLE_SEASONAL_TREND: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 12},
        Granularity.MONTH: {"output": 12, "min": 8},
    },
    # Headwinds/Tailwinds - Risk Stories
    StoryType.VOLATILITY_ALERT: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 12},
        Granularity.MONTH: {"output": 12, "min": 8},
    },
    StoryType.CONCENTRATION_RISK: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 16, "min": 12},
        Granularity.MONTH: {"output": 12, "min": 8},
    },
    # Portfolio Stories
    StoryType.PORTFOLIO_STATUS_OVERVIEW: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
    StoryType.PORTFOLIO_PERFORMANCE_OVERVIEW: {
        Granularity.DAY: {"output": 30, "min": 14},
        Granularity.WEEK: {"output": 12, "min": 8},
        Granularity.MONTH: {"output": 6, "min": 4},
    },
}

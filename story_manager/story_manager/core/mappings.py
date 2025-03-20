from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGroup,
    StoryType,
)

# FILTER_MAPPING defines the relationship between digest types, sections, and their corresponding story filters
FILTER_MAPPING = {
    # Portfolio Digest Sections
    (Digest.PORTFOLIO, Section.OVERVIEW): {"story_groups": [StoryGroup.GOAL_VS_ACTUAL, StoryGroup.LONG_RANGE]},
    (Digest.PORTFOLIO, Section.STATUS_CHANGES): {"story_groups": [StoryGroup.STATUS_CHANGE]},
    (Digest.PORTFOLIO, Section.LIKELY_MISSES): {"story_types": [StoryType.LIKELY_OFF_TRACK]},
    (Digest.PORTFOLIO, Section.BIG_MOVES): {"story_groups": [StoryGroup.RECORD_VALUES, StoryGroup.TREND_EXCEPTIONS]},
    # Promising Trends section includes improving performance, accelerating growth, and new upward trends
    (Digest.PORTFOLIO, Section.PROMISING_TRENDS): {
        "story_types": [
            StoryType.IMPROVING_PERFORMANCE,
            StoryType.ACCELERATING_GROWTH,
            StoryType.NEW_UPWARD_TREND,
        ]
    },
    # Concerning Trends section includes worsening performance, slowing growth, new downward trends, and plateaus
    (Digest.PORTFOLIO, Section.CONCERNING_TRENDS): {
        "story_types": [
            StoryType.WORSENING_PERFORMANCE,
            StoryType.SLOWING_GROWTH,
            StoryType.NEW_DOWNWARD_TREND,
            StoryType.PERFORMANCE_PLATEAU,
        ]
    },
    # Metric Digest Sections
    # What Is Happening section includes various story groups and genres
    (Digest.METRIC, Section.WHAT_IS_HAPPENING): {
        "story_groups": [
            StoryGroup.GOAL_VS_ACTUAL,
            StoryGroup.STATUS_CHANGE,
            StoryGroup.LIKELY_STATUS,
            StoryGroup.GROWTH_RATES,
            StoryGroup.RECORD_VALUES,
            StoryGroup.REQUIRED_PERFORMANCE,
            StoryGroup.TREND_CHANGES,
            StoryGroup.TREND_EXCEPTIONS,
            StoryGroup.SIGNIFICANT_SEGMENTS,
            StoryGroup.LONG_RANGE,
        ],
    },
    # Why Is It Happening section focuses on segment and component drift
    (Digest.METRIC, Section.WHY_IS_IT_HAPPENING): {
        "story_groups": [StoryGroup.SEGMENT_DRIFT, StoryGroup.COMPONENT_DRIFT, StoryGroup.INFLUENCE_DRIFT]
    },
    # What Happens Next section deals with likely status
    (Digest.METRIC, Section.WHAT_HAPPENS_NEXT): {"story_groups": [StoryGroup.LIKELY_STATUS]},
}

# This dictionary maps each StoryType to its corresponding heuristic expressions for different granularity.
# The heuristic expressions are used to evaluate the salience of a story based on the provided variables.
# Each heuristic_expression is a Jinja2 template string that will be rendered with the actual values of the variables.

STORY_TYPE_HEURISTIC_MAPPING: dict[str, Any] = {
    StoryType.LIKELY_OFF_TRACK: {
        Granularity.DAY: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
    },
    StoryType.LIKELY_ON_TRACK: {
        Granularity.DAY: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{deviation}} >= 5", "cool_off_duration": None},
    },
    StoryType.REQUIRED_PERFORMANCE: {
        Granularity.DAY: {"salient_expression": "{{required_growth}} >= 1", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{required_growth}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{required_growth}} >= 5", "cool_off_duration": None},
    },
    StoryType.HOLD_STEADY: {
        Granularity.DAY: {"salient_expression": "{{req_duration}} >= 6", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{req_duration}} >= 3", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{req_duration}} >= 3", "cool_off_duration": None},
    },
    StoryType.ACCELERATING_GROWTH: {
        Granularity.DAY: {"salient_expression": "({{current_growth}} - {{avg_growth}}) > 2", "cool_off_duration": 3},
        Granularity.WEEK: {
            "salient_expression": "({{current_growth}} - {{avg_growth}}) > 5",
            "cool_off_duration": None,
        },
        Granularity.MONTH: {
            "salient_expression": "({{current_growth}} - {{avg_growth}}) > 5",
            "cool_off_duration": None,
        },
    },
    StoryType.SLOWING_GROWTH: {
        Granularity.DAY: {"salient_expression": "({{avg_growth}} - {{current_growth}}) > 2", "cool_off_duration": 3},
        Granularity.WEEK: {
            "salient_expression": "({{avg_growth}} - {{current_growth}}) > 5",
            "cool_off_duration": None,
        },
        Granularity.MONTH: {
            "salient_expression": "({{avg_growth}} - {{current_growth}}) > 5",
            "cool_off_duration": None,
        },
    },
    StoryType.IMPROVING_PERFORMANCE: {
        Granularity.DAY: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 7,
        },
        Granularity.WEEK: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 2,
        },
    },
    StoryType.WORSENING_PERFORMANCE: {
        Granularity.DAY: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 7,
        },
        Granularity.WEEK: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
            "cool_off_duration": 2,
        },
    },
    StoryType.STABLE_TREND: {
        Granularity.DAY: {"salient_expression": "{{trend_duration}} > 14", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{trend_duration}} > 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{trend_duration}} > 5", "cool_off_duration": 2},
    },
    StoryType.PERFORMANCE_PLATEAU: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 2},
    },
    StoryType.GROWING_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": None},
    },
    StoryType.SHRINKING_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{slice_share_change_percentage}} >= 5", "cool_off_duration": None},
    },
    StoryType.IMPROVING_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
    },
    StoryType.WORSENING_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{pressure_change}} >= 5", "cool_off_duration": None},
    },
    StoryType.IMPROVING_COMPONENT: {
        Granularity.DAY: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
    },
    StoryType.WORSENING_COMPONENT: {
        Granularity.DAY: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{contribution}} >= 5", "cool_off_duration": None},
    },
    StoryType.STRONGER_INFLUENCE: {
        Granularity.DAY: {
            "salient_expression": "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
        Granularity.WEEK: {
            "salient_expression": "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
        Granularity.MONTH: {
            "salient_expression": "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
    },
    StoryType.WEAKER_INFLUENCE: {
        Granularity.DAY: {
            "salient_expression": "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
        Granularity.WEEK: {
            "salient_expression": "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
        Granularity.MONTH: {
            "salient_expression": "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
            "cool_off_duration": None,
        },
    },
    StoryType.IMPROVING_INFLUENCE: {
        Granularity.DAY: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
    },
    StoryType.WORSENING_INFLUENCE: {
        Granularity.DAY: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
        Granularity.WEEK: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": "{{output_deviation}} >= 10", "cool_off_duration": None},
    },
    StoryType.RECORD_HIGH: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": None},
    },
    StoryType.RECORD_LOW: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": None},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": None},
    },
}

from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType

STORY_TYPE_HEURISTIC_MAPPING: dict[str, Any] = {
    StoryType.LIKELY_OFF_TRACK: {
        Granularity.DAY: "{{deviation}} >= 5",
        Granularity.WEEK: "{{deviation}} >= 5",
        Granularity.MONTH: "{{deviation}} >= 5",
    },
    StoryType.LIKELY_ON_TRACK: {
        Granularity.DAY: "{{deviation}} >= 5",
        Granularity.WEEK: "{{deviation}} >= 5",
        Granularity.MONTH: "{{deviation}} >= 5",
    },
    StoryType.REQUIRED_PERFORMANCE: {
        Granularity.DAY: "{{required_growth}} >= 1",
        Granularity.WEEK: "{{required_growth}} >= 5",
        Granularity.MONTH: "{{required_growth}} >= 5",
    },
    StoryType.HOLD_STEADY: {
        Granularity.DAY: "{{req_duration}} >= 6",
        Granularity.WEEK: "{{req_duration}} >= 3",
        Granularity.MONTH: "{{req_duration}} >= 3",
    },
    StoryType.ACCELERATING_GROWTH: {
        Granularity.DAY: "({{current_growth}} - {{avg_growth}}) > 2",
        Granularity.WEEK: "({{current_growth} - {{avg_growth}}) > 5",
        Granularity.MONTH: "({{current_growth}} - {{avg_growth}}) > 5",
    },
    StoryType.SLOWING_GROWTH: {
        Granularity.DAY: "({{avg_growth}} - {{current_growth}}) > 2",
        Granularity.WEEK: "({{avg_growth}} - {{current_growth}}) > 5",
        Granularity.MONTH: "({{avg_growth}} - {{current_growth}}) > 5",
    },
    StoryType.IMPROVING_PERFORMANCE: {
        Granularity.DAY: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
        Granularity.WEEK: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
        Granularity.MONTH: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
    },
    StoryType.WORSENING_PERFORMANCE: {
        Granularity.DAY: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
        Granularity.WEEK: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
        Granularity.MONTH: "({{avg_growth}} > 2) and ({{overall_growth}} > 5)",
    },
    StoryType.STABLE_TREND: {
        Granularity.DAY: "{{trend_duration}} > 14",
        Granularity.WEEK: "{{trend_duration}} > 5",
        Granularity.MONTH: "{{trend_duration}} > 5",
    },
    StoryType.GROWING_SEGMENT: {
        Granularity.DAY: "{{slice_share_change_percentage}} >= 5",
        Granularity.WEEK: "{{slice_share_change_percentage}} >= 5",
        Granularity.MONTH: "{{slice_share_change_percentage}} >= 5",
    },
    StoryType.SHRINKING_SEGMENT: {
        Granularity.DAY: "{{slice_share_change_percentage}} >= 5",
        Granularity.WEEK: "{{slice_share_change_percentage}} >= 5",
        Granularity.MONTH: "{{slice_share_change_percentage}} >= 5",
    },
    StoryType.IMPROVING_SEGMENT: {
        Granularity.DAY: "{{pressure_change}} >= 5",
        Granularity.WEEK: "{{pressure_change}} >= 5",
        Granularity.MONTH: "{{pressure_change}} >= 5",
    },
    StoryType.WORSENING_SEGMENT: {
        Granularity.DAY: "{{pressure_change}} >= 5",
        Granularity.WEEK: "{{pressure_change}} >= 5",
        Granularity.MONTH: "{{pressure_change}} >= 5",
    },
    StoryType.IMPROVING_COMPONENT: {
        Granularity.DAY: "{{contribution}} >= 5",
        Granularity.WEEK: "{{contribution}} >= 5",
        Granularity.MONTH: "{{contribution}} >= 5",
    },
    StoryType.WORSENING_COMPONENT: {
        Granularity.DAY: "{{contribution}} >= 5",
        Granularity.WEEK: "{{contribution}} >= 5",
        Granularity.MONTH: "{{contribution}} >= 5",
    },
    StoryType.STRONGER_INFLUENCE: {
        Granularity.DAY: "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
        Granularity.WEEK: "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
        Granularity.MONTH: "({{output_deviation}} - {{prev_output_deviation}}) >= 5",
    },
    StoryType.WEAKER_INFLUENCE: {
        Granularity.DAY: "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
        Granularity.WEEK: "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
        Granularity.MONTH: "({{prev_output_deviation}} - {{output_deviation}}) >= 5",
    },
    StoryType.IMPROVING_INFLUENCE: {
        Granularity.DAY: "{{output_deviation}} >= 10",
        Granularity.WEEK: "{{output_deviation}} >= 10",
        Granularity.MONTH: "{{output_deviation}} >= 10",
    },
    StoryType.WORSENING_INFLUENCE: {
        Granularity.DAY: "{{output_deviation}} >= 10",
        Granularity.WEEK: "{{output_deviation}} >= 10",
        Granularity.MONTH: "{{output_deviation}} >= 10",
    },
}

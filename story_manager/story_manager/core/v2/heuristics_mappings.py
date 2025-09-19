# STORY_TYPE_HEURISTIC_MAPPING_V2 for v2 stories
# This is the mapping of story types to heuristics for each grain
from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType

STORY_TYPE_HEURISTIC_MAPPING_V2: dict[str, Any] = {
    # Goal vs Actual - On Track
    StoryType.ON_TRACK: {
        Granularity.DAY: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{performance_percent}} >= 5", "cool_off_duration": 1},
    },
    # Goal vs Actual - Off Track
    StoryType.OFF_TRACK: {
        Granularity.DAY: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{gap_percent}} >= 5", "cool_off_duration": 1},
    },
    # Status Change - Improving Status
    StoryType.IMPROVING_STATUS: {
        Granularity.DAY: {
            "salient_expression": "({{performance_percent}} >= 3) and ({{old_status_duration}} >= 3)",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{performance_percent}} >= 5) and ({{old_status_duration}} >= 2)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{performance_percent}} >= 5) and ({{old_status_duration}} >= 1)",
            "cool_off_duration": 1,
        },
    },
    # Status Change - Worsening Status
    StoryType.WORSENING_STATUS: {
        Granularity.DAY: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{old_status_duration}} >= 3)",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{gap_percent}} >= 5) and ({{old_status_duration}} >= 2)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{gap_percent}} >= 5) and ({{old_status_duration}} >= 1)",
            "cool_off_duration": 1,
        },
    },
    # Likely Status - Hold Steady
    StoryType.HOLD_STEADY: {
        Granularity.DAY: {"salient_expression": "{{time_to_maintain}} >= 6", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{time_to_maintain}} >= 4", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{time_to_maintain}} >= 2", "cool_off_duration": 2},
    },
    # Significant Segments - Top 4
    StoryType.TOP_4_SEGMENTS: {
        Granularity.DAY: {
            "salient_expression": "({{max_diff_percent}} >= 2) and ({{total_share_percent}} >= 3)",
            "cool_off_duration": 2,
        },
        Granularity.WEEK: {
            "salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
            "cool_off_duration": 1,
        },
    },
    # Significant Segments - Bottom 4 Segments
    StoryType.BOTTOM_4_SEGMENTS: {
        Granularity.DAY: {
            "salient_expression": "({{max_diff_percent}} >= 2) and ({{total_share_percent}} >= 3)",
            "cool_off_duration": 2,
        },
        Granularity.WEEK: {
            "salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{max_diff_percent}} >= 3) and ({{total_share_percent}} >= 5)",
            "cool_off_duration": 1,
        },
    },
    # Significant Segments - Segment Comparison
    StoryType.SEGMENT_COMPARISONS: {
        Granularity.DAY: {"salient_expression": "{{performance_diff_percent}} >= 3", "cool_off_duration": 2},
        Granularity.WEEK: {"salient_expression": "{{performance_diff_percent}} >= 3", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{performance_diff_percent}} >= 3", "cool_off_duration": 1},
    },
    # Growth Rates - Accelerating Growth
    StoryType.ACCELERATING_GROWTH: {
        Granularity.DAY: {
            "salient_expression": "({{current_growth}} - {{average_growth}}) >= 2",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{current_growth}} - {{average_growth}}) >= 5",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{current_growth}} - {{average_growth}}) >= 8",
            "cool_off_duration": 1,
        },
    },
    # Growth Rates - Slowing Growth
    StoryType.SLOWING_GROWTH: {
        Granularity.DAY: {
            "salient_expression": "({{average_growth}} - {{current_growth}}) >= 2",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{average_growth}} - {{current_growth}}) >= 5",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{average_growth}} - {{current_growth}}) >= 8",
            "cool_off_duration": 1,
        },
    },
    # Long-Range - Improving Performance
    StoryType.IMPROVING_PERFORMANCE: {
        Granularity.DAY: {
            "salient_expression": "({{avg_growth}} >= 2) and ({{overall_growth}} >= 5)",
            "cool_off_duration": 7,
        },
        Granularity.WEEK: {
            "salient_expression": "({{avg_growth}} >= 2) and ({{overall_growth}} >= 5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{avg_growth}} >= 2) and ({{overall_growth}} >= 5)",
            "cool_off_duration": 2,
        },
    },
    # Long-Range - Worsening Performance
    StoryType.WORSENING_PERFORMANCE: {
        Granularity.DAY: {
            "salient_expression": "({{avg_growth}} <= -2) and ({{overall_growth}} <= -5)",
            "cool_off_duration": 7,
        },
        Granularity.WEEK: {
            "salient_expression": "({{avg_growth}} <= -2) and ({{overall_growth}} <= -5)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{avg_growth}} <= -2) and ({{overall_growth}} <= -5)",
            "cool_off_duration": 2,
        },
    },
    # Trend Changes - Stable Trend
    StoryType.STABLE_TREND: {
        Granularity.DAY: {"salient_expression": "{{trend_duration}} >= 14", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{trend_duration}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{trend_duration}} >= 2", "cool_off_duration": 2},
    },
    # Trend Changes - New Upward Trend
    StoryType.NEW_UPWARD_TREND: {
        Granularity.DAY: {"salient_expression": "{{trend_avg_growth}} >= 2", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{trend_avg_growth}} >= 3", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{trend_avg_growth}} >= 5", "cool_off_duration": 1},
    },
    # Trend Changes - New Downward Trend
    StoryType.NEW_DOWNWARD_TREND: {
        Granularity.DAY: {"salient_expression": "{{trend_avg_growth}} <= -2", "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": "{{trend_avg_growth}} <= -3", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{trend_avg_growth}} <= -5", "cool_off_duration": 1},
    },
    # Trend Changes - Performance Plateau
    StoryType.PERFORMANCE_PLATEAU: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 7},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1},
    },
    # Trend Exceptions - Spike
    StoryType.SPIKE: {
        Granularity.DAY: {"salient_expression": "{{deviation_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{deviation_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{deviation_percent}} >= 5", "cool_off_duration": 1},
    },
    # Trend Exceptions - Drop
    StoryType.DROP: {
        Granularity.DAY: {"salient_expression": "{{deviation_percent}} <= -5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{deviation_percent}} <= -5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{deviation_percent}} <= -5", "cool_off_duration": 1},
    },
    # Segment Changes - New Strongest Segment
    StoryType.NEW_STRONGEST_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 1},
    },
    # Segment Changes - New Weakest Segment
    StoryType.NEW_WEAKEST_SEGMENT: {
        Granularity.DAY: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{diff_from_avg_percent}} >= 5", "cool_off_duration": 1},
    },
    # Segment Changes - New Largest Segment
    StoryType.NEW_LARGEST_SEGMENT: {
        Granularity.DAY: {
            "salient_expression": "({{current_share_percent}} - {{prior_share_percent}}) >= 2",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{current_share_percent}} - {{prior_share_percent}}) >= 5",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{current_share_percent}} - {{prior_share_percent}}) >= 5",
            "cool_off_duration": 1,
        },
    },
    # Segment Changes - New Smallest Segment
    StoryType.NEW_SMALLEST_SEGMENT: {
        Granularity.DAY: {
            "salient_expression": "({{prior_share_percent}} - {{current_share_percent}}) >= 2",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "({{prior_share_percent}} - {{current_share_percent}}) >= 5",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{prior_share_percent}} - {{current_share_percent}}) >= 5",
            "cool_off_duration": 1,
        },
    },
    # Record Values - Record High
    StoryType.RECORD_HIGH: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1},
    },
    # Record Values - Record Low
    StoryType.RECORD_LOW: {
        Granularity.DAY: {"salient_expression": None, "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": None, "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": None, "cool_off_duration": 1},
    },
    # Benchmark Comparisons
    StoryType.BENCHMARKS: {
        Granularity.DAY: {"salient_expression": "{{benchmark.deviation_percent}} >= 5", "cool_off_duration": 3},
        Granularity.WEEK: {"salient_expression": "{{benchmark.deviation_percent}} >= 5", "cool_off_duration": 2},
        Granularity.MONTH: {"salient_expression": "{{benchmark.deviation_percent}} >= 5", "cool_off_duration": 1},
    },
    StoryType.REQUIRED_PERFORMANCE: {
        Granularity.DAY: {
            "salient_expression": "{{required_growth}} >= 1",
            "cool_off_duration": 3,  # 3 days
        },
        Granularity.WEEK: {
            "salient_expression": "{{required_growth}} >= 2",
            "cool_off_duration": 2,  # 2 weeks
        },
        Granularity.MONTH: {
            "salient_expression": "{{required_growth}} >= 5",
            "cool_off_duration": 2,  # 2 months
        },
    },
    StoryType.FORECASTED_ON_TRACK: {
        Granularity.DAY: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 3,  # 3 days
        },
        Granularity.WEEK: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 2,  # 2 weeks
        },
        Granularity.MONTH: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 1,  # 1 month
        },
    },
    StoryType.FORECASTED_OFF_TRACK: {
        Granularity.DAY: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 3,
        },
        Granularity.WEEK: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "{{gap_percent}} >= 5",
            "cool_off_duration": 1,
        },
    },
    StoryType.PACING_ON_TRACK: {
        Granularity.DAY: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 2,
        },
        Granularity.WEEK: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 1,
        },
    },
    StoryType.PACING_OFF_TRACK: {
        Granularity.DAY: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 2,
        },
        Granularity.WEEK: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 2,
        },
        Granularity.MONTH: {
            "salient_expression": "({{gap_percent}} >= 3) and ({{percent_elapsed}} >= 20)",
            "cool_off_duration": 1,
        },
    },
}

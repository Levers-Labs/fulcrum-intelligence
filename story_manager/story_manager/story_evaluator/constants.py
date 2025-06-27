"""
Constants for story evaluators including templates for different story types.
"""

from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup, StoryType

# Templates for each story type
STORY_TEMPLATES = {
    StoryType.ON_TRACK: {
        "title": "{{ metric.label }} is on track",
        "detail": "{{ metric.label }} is at {{ current_value|format_number }}, "
        "{{ trend_direction }} {{ change_percent|format_percent }}% {{ pop }} "
        "and beating its target of {{ target_value|format_number }} "
        "by {{ performance_percent|format_percent }}%. This continues a "
        "{{ streak_length }}-{{ grain_label }} streak of exceeding target, "
        "with performance {{ performance_trend }} by "
        "{{ change_percent|format_percent }}% over this period.",
    },
    StoryType.OFF_TRACK: {
        "title": "{{ metric.label }} is off track",
        "detail": "{{ metric.label }} is at {{ current_value|format_number }}, "
        "{{ trend_direction }} {{ change_percent|format_percent }}% {{ pop }} "
        "and missing its target of {{ target_value|format_number }} "
        "by {{ gap_percent|format_percent }}%. At the current trajectory, "
        "this gap {{ gap_trend }} by approximately "
        "{{ change_percent|format_percent }}% per {{ grain_label }}.",
    },
    StoryType.IMPROVING_STATUS: {
        "title": "{{ metric.label }} status has improved to on track",
        "detail": "{{ metric.label }} is now On-Track and beating target by "
        "{{ performance_percent|format_percent }}% after previously being "
        "Off-Track for {{ old_status_duration }} {{ grain_label }}s",
    },
    StoryType.WORSENING_STATUS: {
        "title": "{{ metric.label }} status has worsened to off track",
        "detail": "{{ metric.label }} is now Off-Track and missing target by "
        "{{ gap_percent|format_percent }}% after previously being "
        "On-Track for {{ old_status_duration }} {{ grain_label }}s",
    },
    StoryType.HOLD_STEADY: {
        "title": "{{ metric.label }} needs to hold steady",
        "detail": "{{ metric.label }} is already performing at its target level "
        "for the end of the {{ grain_label }} and needs to maintain this lead "
        "for the next {{ time_to_maintain }} {{ grain_label }}s to stay On Track.",
    },
    StoryType.SLOWING_GROWTH: {
        "title": "{{ pop }} Growth is slowing down",
        "detail": "{{ metric.label }} growth has slowed to {{ current_growth|format_percent }}% {{ pop }}, down from "
        "{{ average_growth|format_percent }}% average over past {{ num_periods_slowing }} {{ grain_label }}s",
    },
    StoryType.ACCELERATING_GROWTH: {
        "title": "{{ pop }} growth is speeding up",
        "detail": "{{ metric.label }} growth has accelerated to {{ current_growth|format_percent }}% {{ pop }}, "
        "up from {{ average_growth|format_percent }}% average over past {{ num_periods_accelerating }} {{ "
        "grain_label }}s",
    },
    StoryType.STABLE_TREND: {
        "title": "Following a stable trend",
        "detail": "{{ metric.label }} continues to follow the trend line it has followed for the past {{ "
        "trend_duration }} {{ grain_label }}s, averaging a {{ trend_avg_growth|format_percent }}% {{ pop }} "
        "{{ trend_direction }}.",
    },
    StoryType.NEW_UPWARD_TREND: {
        "title": "New upward trend",
        "detail": "Since {{ trend_start_date }}, {{ metric.label }} has been following a new, upward trend line that "
        "averages {{ trend_avg_growth|format_percent }}% {{ pop }} growth. The prior trend for this metric "
        "lasted {{ prev_trend_duration }} {{ grain_label }}s and averaged {{ "
        "prev_trend_avg_growth|format_percent }}% {{ pop }} growth.",
    },
    StoryType.NEW_DOWNWARD_TREND: {
        "title": "New downward trend",
        "detail": "Since {{ trend_start_date }}, {{ metric.label }} has been following a new, downward trend line "
        "that averages {{ trend_avg_growth|format_percent }}% {{ pop }} decline. The prior trend for this "
        "metric lasted {{ prev_trend_duration }} {{ grain_label }}s and averaged {{ "
        "prev_trend_avg_growth|format_percent }}% {{ pop }} growth.",
    },
    StoryType.PERFORMANCE_PLATEAU: {
        "title": "Performance has leveled off",
        "detail": "Since {{ trend_start_date }}, {{ metric.label }} growth has steadied into a new normal, hovering "
        "around a {{ grain_label }} average of {{ trend_avg_growth|format_percent }}%.",
    },
    StoryType.SPIKE: {
        "title": "Performance spike above trend",
        "detail": "{{ metric.label }} is currently performing at {{ deviation_percent|format_percent }}% above its "
        "normal range. This may indicate an anomaly, a data issue, or a fundamental change in how this "
        "metric performs.",
    },
    StoryType.DROP: {
        "title": "Performance drop below trend",
        "detail": "{{ metric.label }} is currently performing at {{ deviation_percent|format_percent }}% below its "
        "normal range. This may indicate an anomaly, a data issue, or a fundamental change in how this "
        "metric performs.",
    },
    StoryType.IMPROVING_PERFORMANCE: {
        "title": "Improved performance over the past {{ trend_duration }} {{ grain_label }}s",
        "detail": "Over the past {{ trend_duration }} {{ grain_label }}s, {{ metric.label }} has been averaging {{ "
        "trend_avg_growth|format_percent }}% {{ pop }} growth and has grown since {{ trend_start_date }}.",
    },
    StoryType.WORSENING_PERFORMANCE: {
        "title": "Worsening performance over the past {{ trend_duration }} {{ grain_label }}s",
        "detail": "Over the past {{ trend_duration }} {{ grain_label }}s, {{ metric.label }} has been declining {{ "
        "trend_avg_growth|format_percent }}% {{ pop }} and has fallen since {{ trend_start_date }}.",
    },
    StoryType.SEASONAL_PATTERN_MATCH: {
        "title": "Expected seasonal behavior",
        "detail": "{{ metric.label }} is following its usual seasonal pattern, aligning within {{ "
        "seasonal_deviation|format_percent }}% of historical norms for this {{ grain_label }}. No "
        "unexpected variation is evident.",
    },
    StoryType.SEASONAL_PATTERN_BREAK: {
        "title": "Unexpected seasonal deviation",
        "detail": "{{ metric.label }} is diverging from its usual seasonal trend by {{ "
        "seasonal_deviation|format_percent }}%. Historically, we'd expect a seasonal {{ expected_direction "
        "}} of {{ expected_change|format_percent }}% this period, but we're currently seeing a seasonal {{ "
        "actual_direction }} of {{ actual_change|format_percent }}%.",
    },
    StoryType.RECORD_HIGH: {
        "title": "{{ high_rank|format_ordinal }} highest {{ grain_label }} value over the past {{ high_duration }} {{ "
        "grain_label }}s",
        "detail": "The {{ grain_label }} value for {{ metric.label }} of {{ high_value|format_number }} is now the {{ "
        "high_rank|format_ordinal }} highest value in {{ high_duration }} {{ grain_label }}s.",
    },
    StoryType.RECORD_LOW: {
        "title": "{{ low_rank|format_ordinal }} lowest {{ grain_label }} value over the past {{ low_duration }} {{ "
        "grain_label }}s",
        "detail": "The {{ grain_label }} value for {{ metric.label }} of {{ low_value|format_number }} is now the {{ "
        "low_rank|format_ordinal }} lowest value in {{ low_duration }} {{ grain_label }}s.",
    },
    StoryType.BENCHMARKS: {
        "title": "{{ metric.label }} • Performance Against Historical Benchmarks",
        "detail": "This {{ grain_label }} marks the {{ high_rank|format_ordinal }} highest-performing "
        "{{ grain_label }} in the past {{ high_duration }} {{ grain_label }}s, with the {{ benchmark.current_period }} "
        "performance of {{ metric.label }} at {{ benchmark.current_value|format_number }} coming in "
        "{% for summary in benchmark.comparison_summaries %}{{ summary }}{% if not loop.last %}"
        "{% if loop.index == benchmark.comparison_summaries|length - 1 %} and {% else %}, {% endif %}{% endif %}{% endfor %}.",  # noqa: E501
    },
    StoryType.TOP_4_SEGMENTS: {
        "title": "Strongest segments",
        "detail": "The 4 strongest-performing segments for {{ metric.label }} are {{ top_segments }}, "
        "which collectively outperforming the average of {{ min_diff_percent|format_percent }}% to {{ "
        "max_diff_percent|format_percent }}%. These segments represent {{ "
        "total_share_percent|format_percent }}% of total volume.",
    },
    StoryType.BOTTOM_4_SEGMENTS: {
        "title": "Weakest segments",
        "detail": "The 4 weakest-performing segments for {{ metric.label }} are {{ bottom_segments }}, "
        "which collectively underperforming the average of {{ min_diff_percent|format_percent }}% to {{ "
        "max_diff_percent|format_percent }}%. These segments represent {{ "
        "total_share_percent|format_percent }}% of total volume.",
    },
    StoryType.SEGMENT_COMPARISONS: {
        "title": "{{ segment_a }} vs. {{ segment_b }}",
        "detail": "{{ segment_a }} outperforms {{ segment_b }} by {{ performance_diff_percent|format_percent }}%. "
        "Over the past {{ grain_label }}, this gap has {{ gap_trend }} by {{ "
        "gap_change_percent|format_percent }}%.",
    },
    StoryType.NEW_STRONGEST_SEGMENT: {
        "title": "{{ segment_name }} is now the best-performing segment",
        "detail": "{{ segment_name }} is now the strongest performing segment with an average {{ metric.label }} "
        "value of {{ current_value|format_number }} -- {{ diff_from_avg_percent|format_percent }}% higher "
        "than the average across all segments of {{ avg_value|format_number }}. Meanwhile, the previously "
        "strongest segment {{ previous_segment }} is {{ trend_direction }} {{ change_percent|format_percent "
        "}}% to an average {{ metric.label }} value of {{ previous_value|format_number }}.",
    },
    StoryType.NEW_WEAKEST_SEGMENT: {
        "title": "{{ segment_name }} is now the worst-performing segment",
        "detail": "{{ segment_name }} is now the weakest performing segment with an average {{ metric.label }} value "
        "of {{ current_value|format_number }} -- {{ diff_from_avg_percent|format_percent }}% lower than the "
        "average across all segments of {{ avg_value|format_number }}. Meanwhile, the previously weakest "
        "segment {{ previous_segment }} is {{ trend_direction }} {{ change_percent|format_percent }}% to an "
        "average {{ metric.label }} value of {{ previous_value|format_number }}.",
    },
    StoryType.NEW_LARGEST_SEGMENT: {
        "title": "{{ segment_name }} is now the most represented segment",
        "detail": "{{ segment_name }} now comprises the largest share of {{ dimension_name }}, "
        "at {{ current_share_percent|format_percent }}%, up from {{ prior_share_percent|format_percent }}% "
        "the prior {{ grain_label }}. This surpasses the previously most represented segment {{ "
        "previous_segment }} which now comprises {{ previous_share_percent|format_percent }}% of {{ "
        "dimension_name }}.",
    },
    StoryType.NEW_SMALLEST_SEGMENT: {
        "title": "{{ segment_name }} is now the least represented segment",
        "detail": "{{ segment_name }} now comprises the smallest share of {{ dimension_name }}, "
        "at {{ current_share_percent|format_percent }}%, down from {{ prior_share_percent|format_percent "
        "}}% in the prior {{ grain_label }}. The previously least represented segment {{ previous_segment "
        "}} now comprises {{ previous_share_percent|format_percent }}% of {{ dimension_name }}, up from {{ "
        "previous_prior_share_percent|format_percent }}%.",
    },
    StoryType.FORECASTED_ON_TRACK: {
        "title": "Forecasted to beat end of {{ period_type }} target",
        "detail": "{{ metric.label }} is forecasted to end the {{ period_type }} at {{ forecasted_value|format_number "
        "}}"
        "and beat its target of {{ target_value|format_number }} by {{ gap_percent|format_percent }}%",
    },
    StoryType.FORECASTED_OFF_TRACK: {
        "title": "Forecasted to miss end of {{ period_type }} target by {{ gap_percent|format_percent }}%",
        "detail": "{{ metric.label }} is forecasted to end the {{ period_type }} at {{ forecasted_value|format_number "
        "}}"
        "and miss its target of {{ target_value|format_number }} by {{ gap_percent|format_percent }}%",
    },
    StoryType.PACING_ON_TRACK: {
        "title": "Pacing to beat end of {{ period_type }} target",
        "detail": "{{ percent_elapsed|format_percent }}% through the {{ period_type }}, {{ metric.label }} is pacing "
        "to end this {{ period_type }}"
        "at {{ projected_value|format_number }}, beating the target of {{ target_value|format_number }} "
        "by {{ gap_percent|format_percent }}% if the current trajectory holds.",
    },
    StoryType.PACING_OFF_TRACK: {
        "title": "Pacing to miss end of {{ period_type }} target by {{ gap_percent|format_percent }}%",
        "detail": "{{ percent_elapsed|format_percent }}% through the {{ period_type }}, {{ metric.label }} is pacing "
        "to end this {{ period_type }}"
        "at {{ projected_value|format_number }}, missing the target of {{ target_value|format_number }} "
        "by {{ gap_percent|format_percent }}% if the current trajectory holds.",
    },
    StoryType.REQUIRED_PERFORMANCE: {
        "title": "Must grow {{ required_growth|format_percent }}% {{ pop }} to meet end of {{ period_type }} target",
        "detail": "{{ metric.label }} must average a {{ required_growth|format_percent }}% {{ pop }} growth rate over "
        "the next"
        "{{ remaining_periods }} {{ grain_label }}s to meet its end of {{ period_type }} target of {{ "
        "target_value|format_number }}."
        "This is a {{ delta_growth|format_percent }}% {{ trend_direction }} over the {{ past_growth|format_percent "
        "}}% {{ pop }}"
        "growth over the past {{ past_periods }} {{ grain_label }}s.",
    },
}

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
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.ACCELERATING_GROWTH: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.STABLE_TREND: {
        Granularity.DAY: {"output": 30},
        Granularity.WEEK: {"output": 10},
        Granularity.MONTH: {"output": 6},
    },
    StoryType.NEW_UPWARD_TREND: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.NEW_DOWNWARD_TREND: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.PERFORMANCE_PLATEAU: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.SPIKE: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.DROP: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.IMPROVING_PERFORMANCE: {
        Granularity.DAY: {"output": 30},
        Granularity.WEEK: {"output": 10},
        Granularity.MONTH: {"output": 6},
    },
    StoryType.WORSENING_PERFORMANCE: {
        Granularity.DAY: {"output": 30},
        Granularity.WEEK: {"output": 10},
        Granularity.MONTH: {"output": 6},
    },
    StoryType.RECORD_HIGH: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.RECORD_LOW: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.BENCHMARKS: {
        Granularity.DAY: {"output": 14},
        Granularity.WEEK: {"output": 8},
        Granularity.MONTH: {"output": 4},
    },
    # TODO: Need to confirm the values with abhi for below
    StoryType.ON_TRACK: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.OFF_TRACK: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.IMPROVING_STATUS: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
    StoryType.WORSENING_STATUS: {
        Granularity.DAY: {"output": 7},
        Granularity.WEEK: {"output": 5},
        Granularity.MONTH: {"output": 4},
    },
}

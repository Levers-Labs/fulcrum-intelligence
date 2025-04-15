"""
Constants for story evaluators including templates for different story types.
"""

from story_manager.core.enums import StoryType

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
        "grain_label }}s. This acceleration began {{ trend_duration }} {{ grain_label }}s ago.",
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
        "title": "{{ high_rank }} highest value over the past {{ high_duration }} {{ grain_label }}s",
        "detail": "The {{ grain_label }} value for {{ metric.label }} of {{ high_value|format_number }} is now the {{ "
        "high_rank }} highest value in {{ high_duration }} {{ grain_label }}s.",
    },
    StoryType.RECORD_LOW: {
        "title": "{{ low_rank }} lowest value over the past {{ low_duration }} {{ grain_label }}s",
        "detail": "The {{ grain_label }} value for {{ metric.label }} of {{ low_value|format_number }} is now the {{ "
        "low_rank }} lowest value in {{ low_duration }} {{ grain_label }}s.",
    },
    StoryType.BENCHMARKS: {
        "title": "Performance Against Historical Benchmarks",
        "detail": "This day marks the {{ high_rank }}th highest-performing {{ grain_label }} in the past {{ "
        "high_duration }} {{ grain_label }}s, with the current period's performance of {{ metric.label }} "
        "at {{ high_value|format_number }} coming in {{ prior_change_percent|format_percent }}% {{ "
        "prior_direction }} than this time last {{ prior_period }} and {{ older_change_percent }} {{ "
        "older_direction }} than this time last {{ older_period }}",
    },
}

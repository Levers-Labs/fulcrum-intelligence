"""
Constants for story evaluators including templates for different story types.
"""

from commons.models.enums import Granularity
from story_manager.core.enums import StoryType

# Grain to period/interval mapping
GRAIN_MAPPING = {
    Granularity.DAY: {"label": "day", "pop": "d/d"},
    Granularity.WEEK: {"label": "week", "pop": "w/w"},
    Granularity.MONTH: {"label": "month", "pop": "m/m"},
    Granularity.QUARTER: {"label": "quarter", "pop": "q/q"},
    Granularity.YEAR: {"label": "year", "pop": "y/y"},
}

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
}

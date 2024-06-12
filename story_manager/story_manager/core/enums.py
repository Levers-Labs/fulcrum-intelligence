from enum import Enum


class StoryGenre(str, Enum):
    """
    Defines the genre of the story
    """

    GROWTH = "GROWTH"
    TRENDS = "TRENDS"
    PERFORMANCE = "PERFORMANCE"
    BIG_MOVES = "BIG_MOVES"
    ROOT_CAUSES = "ROOT_CAUSES"


class StoryType(str, Enum):
    """
    Defines the type of the story for each genre
    """

    # Growth Stories
    SLOWING_GROWTH = "SLOWING_GROWTH"
    ACCELERATING_GROWTH = "ACCELERATING_GROWTH"
    # Trend Stories
    STABLE_TREND = "STABLE_TREND"
    NEW_UPWARD_TREND = "NEW_UPWARD_TREND"
    NEW_DOWNWARD_TREND = "NEW_DOWNWARD_TREND"
    PERFORMANCE_PLATEAU = "PERFORMANCE_PLATEAU"
    SPIKE = "SPIKE"
    DROP = "DROP"
    IMPROVING_PERFORMANCE = "IMPROVING_PERFORMANCE"
    WORSENING_PERFORMANCE = "WORSENING_PERFORMANCE"
    # Performance Stories
    ON_TRACK = "ON_TRACK"
    OFF_TRACK = "OFF_TRACK"
    IMPROVING_STATUS = "IMPROVING_STATUS"
    WORSENING_STATUS = "WORSENING_STATUS"
    # Big Moves Stories
    RECORD_HIGH = "RECORD_HIGH"
    RECORD_LOW = "RECORD_LOW"
    # Likely status
    LIKELY_ON_TRACK = "LIKELY_ON_TRACK"
    LIKELY_OFF_TRACK = "LIKELY_OFF_TRACK"
    # Segment Drift Stories
    GROWING_SEGMENT = "GROWING_SEGMENT"
    SHRINKING_SEGMENT = "SHRINKING_SEGMENT"
    IMPROVING_SEGMENT = "IMPROVING_SEGMENT"
    WORSENING_SEGMENT = "WORSENING_SEGMENT"


class StoryGroup(str, Enum):
    """
    Defines the group of the story
    """

    GROWTH_RATES = "GROWTH_RATES"

    TREND_CHANGES = "TREND_CHANGES"
    TREND_EXCEPTIONS = "TREND_EXCEPTIONS"
    LONG_RANGE = "LONG_RANGE"
    GOAL_VS_ACTUAL = "GOAL_VS_ACTUAL"
    LIKELY_STATUS = "LIKELY_STATUS"
    RECORD_VALUES = "RECORD_VALUES"
    STATUS_CHANGE = "STATUS_CHANGE"
    SEGMENT_DRIFT = "SEGMENT_DRIFT"


class Position(str, Enum):
    """
    Defines the position of the value
    """

    ABOVE = "above"
    BELOW = "below"


class Movement(str, Enum):
    """
    Defines the movement of the numeric value
    """

    INCREASE = "increase"
    DECREASE = "decrease"


class Direction(str, Enum):
    """
    Defines the direction of the value
    """

    UP = "up"
    DOWN = "down"


class Pressure(str, Enum):
    """
    Defines the pressure of a slice
    """

    UPWARD = "upward"
    DOWNWARD = "downward"
    UNCHANGED = "unchanged"


GROUP_TO_STORY_TYPE_MAPPING = {
    StoryGroup.GROWTH_RATES: [
        StoryType.SLOWING_GROWTH,
        StoryType.ACCELERATING_GROWTH,
    ],
    StoryGroup.TREND_CHANGES: [
        StoryType.STABLE_TREND,
        StoryType.NEW_UPWARD_TREND,
        StoryType.NEW_DOWNWARD_TREND,
        StoryType.PERFORMANCE_PLATEAU,
    ],
    StoryGroup.GOAL_VS_ACTUAL: [
        StoryType.ON_TRACK,
        StoryType.OFF_TRACK,
    ],
    StoryGroup.RECORD_VALUES: [
        StoryType.RECORD_HIGH,
        StoryType.RECORD_LOW,
    ],
    StoryGroup.TREND_EXCEPTIONS: [
        StoryType.SPIKE,
        StoryType.DROP,
    ],
    StoryGroup.LONG_RANGE: [
        StoryType.IMPROVING_PERFORMANCE,
        StoryType.WORSENING_PERFORMANCE,
    ],
    StoryGroup.STATUS_CHANGE: [
        StoryType.IMPROVING_STATUS,
        StoryType.WORSENING_STATUS,
    ],
    StoryGroup.LIKELY_STATUS: [
        StoryType.LIKELY_ON_TRACK,
        StoryType.LIKELY_OFF_TRACK,
    ],
    StoryGroup.SEGMENT_DRIFT: [
        StoryType.GROWING_SEGMENT,
        StoryType.SHRINKING_SEGMENT,
        StoryType.IMPROVING_SEGMENT,
        StoryType.WORSENING_SEGMENT,
    ],
}

# Story type meta-information
STORY_TYPES_META: dict[str, dict[str, str]] = {
    StoryType.SLOWING_GROWTH: {
        "title": "{{pop}} growth is slowing down",
        # e.g., The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% average
        # over the past 5 days.
        "detail": "The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently "
        "{{current_growth}}% and down from the {{avg_growth}}% average over the past {{duration}} {{grain}}s.",
    },
    StoryType.ACCELERATING_GROWTH: {
        "title": "{{pop}} growth is speeding up",
        # e.g., The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% average
        # over the past 11 days.
        "detail": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently "
        "{{current_growth}}% and up from the {{avg_growth}}% average over the past {{duration}} {{grain}}s.",
    },
    StoryType.STABLE_TREND: {
        "title": "Following a stable trend",
        # e.g., NewBizDeals continues to follow the trend line it has followed for the past 30 days, averaging a 10%
        # d/d increase.
        "detail": "{{metric.label}} continues to follow the trend line it has followed for the "
        "past {{trend_duration}} {{grain}}s, averaging a {{avg_growth}}% {{pop}} {{movement}}.",
    },
    StoryType.NEW_UPWARD_TREND: {
        "title": "New upward trend",
        # E.g., Since Mar 15, 2024, NewBizDeals has been following a new, upward trend line that averages 15% d/d
        # growth. The prior trend for this metric lasted 30 days and averaged 10% d/d growth.
        "detail": "Since {{trend_start_date}}, {{metric.label}} has been following a new, upward trend line that "
        "averages {{current_avg_growth}}% {{pop}} growth. The prior trend for this metric "
        "lasted {{previous_trend_duration}} {{grain}}s and averaged {{previous_avg_growth}}% {{pop}} growth.",
    },
    StoryType.NEW_DOWNWARD_TREND: {
        "title": "New downward trend",
        # E.g., Since Mar 15, 2024, NewBizDeals has been following a new, downward trend line that averages 5% d/d
        # growth. The prior trend for this metric lasted 30 days and averaged 10% d/d growth.
        "detail": "Since {{trend_start_date}}, {{metric.label}} has been following a new, downward trend line that "
        "averages {{current_avg_growth}}% {{pop}} growth. The prior trend for this metric "
        "lasted {{previous_trend_duration}} {{grain}}s and averaged {{previous_avg_growth}}% {{pop}} growth.",
    },
    StoryType.PERFORMANCE_PLATEAU: {
        "title": "Performance has leveled off",
        # e.g., Since Mar 15, 2024, NewBizDeals growth has steadied into a new normal, hovering around a 30day
        # average of 8%.
        "detail": "Since {{trend_start_date}}, {{metric.label}} growth has steadied into a new normal, hovering "
        "around a {{grain}} average of {{avg_value}}.",
    },
    StoryType.SPIKE: {
        "title": "Performance spike above {{grain}} trend",
        # e.g.,  NewBizDeals is currently performing at 12% above its normal range. This may indicate an anomaly,
        # a data issue, or a fundamental change in how this metric performs.
        "detail": "{{metric.label}} is currently performing at {{deviation}}% {{position}} its normal "
        "range. This may indicate an anomaly, a data issue, or a fundamental change in how this metric "
        "performs.",
    },
    StoryType.DROP: {
        "title": "Performance drop below {{grain}} trend",
        # e.g.,  NewBizDeals is currently performing at -25% below its normal range. This may indicate an anomaly,
        # a data issue, or a fundamental change in how this metric performs.
        "detail": "{{metric.label}} is currently performing at {{deviation}}% {{position}} its normal "
        "range. This may indicate an anomaly, a data issue, or a fundamental change in how this metric "
        "performs.",
    },
    StoryType.IMPROVING_PERFORMANCE: {
        "title": "Performance has improved over the past {{duration}} {{grain}}s",
        # e.g.,  Over the past 30 days, NewBizDeals has been averaging 10% d/d growth and has grown 5% overall
        # since Mar 15, 2024.
        "detail": "Over the past {{duration}} {{grain}}s, {{metric.label}} has been averaging {{avg_growth}}% {{pop}} "
        "growth and has grown {{overall_growth}}% overall since {{start_date}}.",
    },
    StoryType.WORSENING_PERFORMANCE: {
        "title": "Performance has worsened over the past {{duration}} {{grain}}s",
        # e.g.,  Over the past 30 days, NewBizDeals has been declining 10% d/d growth and has fallen 5% overall
        # since Mar 15, 2024.
        "detail": "Over the past {{duration}} {{grain}}s, {{metric.label}} has been declining {{avg_growth}}% {{pop}} "
        "growth and has fallen {{overall_growth}}% overall since {{start_date}}.",
    },
    StoryType.ON_TRACK: {
        "title": "Beats {{duration}} {{grain}}s target",
        # e.g.,  As of EOD, NewBizDeals was at 85, up 5% d/d and beating its target of 80 by 6.25%.
        "detail": "As of {{eoi}}, {{metric.label}} was at {{current_value}}, {{direction}} {{current_growth}}% {{"
        "pop}} and beating its target of {{target}} by {{deviation}}%.",
    },
    StoryType.OFF_TRACK: {
        "title": "Missed {{duration}} {{grain}}s target",
        # e.g.,  As of EOD, NewBizDeals was at 75, down 2% d/d and missing its target 80 by 6.25%.
        "detail": "As of {{eoi}}, {{metric.label}} was at {{current_value}}, {{direction}} {{current_growth}}% {{"
        "pop}} and missing its target of {{target}} by {{deviation}}%.",
    },
    StoryType.RECORD_HIGH: {
        "title": "{% if is_second_rank %}Second Highest{% else %}Highest{% endif %} value over the past {{duration}} "
        "{{grain}}s",
        # e.g.,  On Mar 15, 2024, the 20 days value for NewBizDeals hit 90 -- the second highest across the past 20
        # days.
        "detail": "On {{record_date}}, the {{duration}} {{grain}}s value for {{metric.label}} hit {{value}} -- the {% "
        "if is_second_rank %}Second Highest{% else %}Highest{% endif %} across the past {{duration}} {{"
        "grain}}s.{% if not is_second_rank %} This represents a {{deviation}}% increase over the prior "
        "high of {{prior_value}} on {{prior_date}}. {% endif %}",
    },
    StoryType.RECORD_LOW: {
        "title": "{% if is_second_rank %}Second lowest{% else %}Lowest{% endif %} {{grain}} value over the past {{"
        "duration}} {{grain}}s",
        # e.g., On Mar 15, 2024, the 20-day value for NewBizDeals hit -10 -- the lowest across the past quarter.
        "detail": "On {{record_date}}, the {{duration}} {{grain}}s value for {{metric.label}} hit {{value}} -- the {% "
        "if is_second_rank %}Second lowest{% else %}Lowest{% endif %} across the past {{duration}} {{"
        "grain}}s.{% if not is_second_rank  %} This represents a {{deviation}}% decrease over the "
        "prior low of {{prior_value}} on {{prior_date}}. {% endif %}",
    },
    StoryType.IMPROVING_STATUS: {
        "title": "Newly beating target",
        # e.g., NewBizDeals is now On-Track and beating target by 100% after previously being Off-Track for 2 weeks.
        "detail": "{{metric.label}} is now On-Track and beating target by {{deviation}}% after previously being "
        "Off-Track for {{prev_duration}} {{grain}}s.",
    },
    StoryType.WORSENING_STATUS: {
        "title": "Newly missing target",
        # e.g., NewBizDeals is now Off-Track and missing target by 10% after previously being On-Track for 2 weeks.
        "detail": "{{metric.label}} is now Off-Track and missing target by {{deviation}}% after previously being "
        "On-Track for {{prev_duration}} {{grain}}s.",
    },
    StoryType.LIKELY_ON_TRACK: {
        "title": "Pacing to beat end of {{interval}} target by {{deviation}}%",
        # e.g., SQORate is forecasted to end the day at 90 and beat its target of 80 by 12.5%.
        "detail": "{{metric.label}} is forecasted to end the {{interval}} at {{forecasted_value}} and beat its target "
        "of {{target}} by {{deviation}}%.",
    },
    StoryType.LIKELY_OFF_TRACK: {
        "title": "Pacing to miss end of {{interval}} target by {{deviation}}%",
        # e.g., SQORate is forecasted to end the day at 70 and miss its target of 80 by 12.5%.
        "detail": "{{metric.label}} is forecasted to end the {{interval}} at {{forecasted_value}} and miss its target "
        "of {{target}} by {{deviation}}%.",
    },
    StoryType.GROWING_SEGMENT: {
        "title": "Key Driver: Growing {{slice | default('null')}} share of {{dimension}}",
        "detail": "The share of {{dimension}} that is {{slice | default('null')}} increased from "
        "{{previous_share}}% to {{current_share}}% over the past {{grain}}. This increase "
        "contributed {{slice_share_change_percentage}}% {{pressure_direction}} pressure on {{metric.label}}.",
    },
    StoryType.SHRINKING_SEGMENT: {
        "title": "Key Driver: Falling {{slice | default('null')}} share of {{dimension}}",
        "detail": "For {{metric.label}}, the share of {{dimension}} that is {{slice | default('null')}} has decreased "
        "from {{previous_share}}% to {{current_share}}% over the past {{grain}}. This "
        "decrease contributed {{slice_share_change_percentage}}% {{pressure_direction}} pressure on "
        "{{metric.label}}.",
    },
    StoryType.IMPROVING_SEGMENT: {
        "title": "Key Driver: Stronger {{slice | default('null')}} segment",
        "detail": "Over the past {{grain}}, when {{dimension}} is {{slice | default('null')}}, {{metric.label}} is "
        "{{current_value}}. This is an increase of {{deviation}}% relative to "
        "the prior {{grain}}, and this increase contributed {{pressure_change}}% {{pressure_direction}} "
        "pressure on {{metric.label}}.",
    },
    StoryType.WORSENING_SEGMENT: {
        "title": "Key Driver: Weaker {{slice}} segment",
        "detail": "Over the past {{grain}}, when {{dimension}} is {{slice | default('null')}}, {{metric.label}} is "
        "{{current_value}}. This is a decrease of {{deviation}}% relative to "
        "the prior {{grain}}, and this decrease contributed {{pressure_change}}% {{pressure_direction}} "
        "pressure on {{metric.label}}.",
    },
}

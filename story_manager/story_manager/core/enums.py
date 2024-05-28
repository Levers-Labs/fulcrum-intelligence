from enum import Enum


class StoryGenre(str, Enum):
    """
    Defines the genre of the story
    """

    GROWTH = "GROWTH"
    TRENDS = "TRENDS"
    PERFORMANCE = "PERFORMANCE"


class StoryType(str, Enum):
    """
    Defines the type of the story for each genre
    """

    # growth stories
    SLOWING_GROWTH = "SLOWING_GROWTH"
    ACCELERATING_GROWTH = "ACCELERATING_GROWTH"
    # trend stories
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


class StoryGroup(str, Enum):
    """
    Defines the group of the story
    """

    GROWTH_RATES = "GROWTH_RATES"
    TREND_CHANGES = "TREND_CHANGES"
    TREND_EXCEPTIONS = "TREND_EXCEPTIONS"
    LONG_RANGE = "LONG_RANGE"
    GOAL_VS_ACTUAL = "GOAL_VS_ACTUAL"


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
    StoryGroup.TREND_EXCEPTIONS: [
        StoryType.SPIKE,
        StoryType.DROP,
    ],
    StoryGroup.LONG_RANGE: [
        StoryType.IMPROVING_PERFORMANCE,
        StoryType.WORSENING_PERFORMANCE,
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
}

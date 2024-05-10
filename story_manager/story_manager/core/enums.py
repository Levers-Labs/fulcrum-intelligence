from commons.models.enums import StrEnum


class StoryGenre(StrEnum):
    """
    Defines the genre of the story
    """

    GROWTH = "GROWTH"
    TRENDS = "TRENDS"


class StoryType(StrEnum):
    """
    Defines the type of the story for each genre
    """

    # growth stories
    SLOWING_GROWTH = "SLOWING_GROWTH"
    ACCELERATING_GROWTH = "ACCELERATING_GROWTH"
    # trend stories
    IMPROVING_PERFORMANCE = "IMPROVING_PERFORMANCE"
    WORSENING_PERFORMANCE = "WORSENING_PERFORMANCE"
    STABLE_TREND = "STABLE_TREND"
    NEW_UPWARD_TREND = "NEW_UPWARD_TREND"
    NEW_DOWNWARD_TREND = "NEW_DOWNWARD_TREND"
    PERFORMANCE_PLATEAU = "PERFORMANCE_PLATEAU"
    SPIKE = "SPIKE"
    DROP = "DROP"


class StoryGroup(StrEnum):
    """
    Defines the group of the story
    """

    GROWTH_RATES = "GROWTH_RATES"
    LONG_RANGE = "LONG_RANGE"
    TREND_CHANGES = "TREND_CHANGES"
    TREND_EXCEPTIONS = "TREND_EXCEPTIONS"
    RECORD_VALUES = "RECORD_VALUES"
    SEGMENT_TRENDS = "SEGMENT_TRENDS"


GROUP_TO_STORY_TYPE_MAPPING = {
    StoryGroup.GROWTH_RATES: [
        StoryType.SLOWING_GROWTH,
        StoryType.ACCELERATING_GROWTH,
    ],
    StoryGroup.LONG_RANGE: [
        StoryType.IMPROVING_PERFORMANCE,
        StoryType.WORSENING_PERFORMANCE,
    ],
    StoryGroup.TREND_CHANGES: [
        StoryType.STABLE_TREND,
        StoryType.NEW_UPWARD_TREND,
        StoryType.NEW_DOWNWARD_TREND,
        StoryType.PERFORMANCE_PLATEAU,
    ],
    StoryGroup.TREND_EXCEPTIONS: [
        StoryType.SPIKE,
        StoryType.DROP,
    ],
}

# Story type meta-information
STORY_TYPES_META: dict[str, dict[str, str]] = {
    StoryType.SLOWING_GROWTH: {
        "title": "{{pop}} growth is slowing down",
        # e.g., The d/d growth rate for NewBizDeals is slowing down. It is currently 10% and down from the 15% average
        # over the past 5 days.
        "template": "The {{pop}} growth rate for {{metric.label}} is slowing down. It is currently {{"
        "current_growth}}% and down from the {{reference_growth}}% average over the past {{"
        "reference_period_days}} {{grain}}s.",
    },
    StoryType.ACCELERATING_GROWTH: {
        "title": "{{pop}} growth is speeding up",
        # e.g., The d/d growth rate for NewBizDeals is speeding up. It is currently 15% and up from the 10% average
        # over the past 11 days.
        "template": "The {{pop}} growth rate for {{metric.label}} is speeding up. It is currently {{current_growth}}% "
        "and up from the {{reference_growth}}% average over the past {{reference_period_days}} {{"
        "days}}s.",
    },
    StoryType.IMPROVING_PERFORMANCE: {
        "title": "Performance has improved over the past {{reference_period_days}} {{grain}}s",
        # e.g., Over the past 30 days, NewBizDeals has been averaging 15% d/d growth and has grown 25% overall since
        # 08-04-2024.
        "template": "Over the past {{reference_period_days}} {{grain}}s, {{metric.label}} has been "
        "averaging {{current_growth}}% {{pop}} growth and has grown {{overall_growth}}% overall since {{"
        "start_date}}.",
    },
    StoryType.WORSENING_PERFORMANCE: {
        "title": "Performance has worsened over the past {{reference_period_days}} {{grain}}s",
        # e.g., Over the past 30 days, NewBizDeals has been declining 5% d/d and has fallen 15% overall since
        # 08-04-2024.
        "template": "Over the past {{reference_period_days}} {{grain}}s, {{metric.label}} has been "
        "declining {{current_growth}}% {{pop}} and has fallen {{overall_growth}}% overall since {{"
        "start_date}}.",
    },
    StoryType.STABLE_TREND: {
        "title": "Following a stable trend",
        # e.g., NewBizDeals continues to follow the trend line it has followed for the past 30 days, averaging a 10%
        # d/d increase.
        "template": "{{metric.label}} continues to follow the trend line it has followed for the past {{"
        "trend_duration_days}} {{grain}}s, averaging a {{current_growth}}% {{pop}} {{"
        "movement}}.",
    },
    StoryType.NEW_UPWARD_TREND: {
        "title": "New upward trend",
        # e.g., Since 07-04-2024, NewBizDeals has been following a new, upward trend line that averages 15% d/d
        # growth. The prior trend for this metric lasted 30 days and averaged 10% d/d growth.
        "template": "Since {{trend_start_date}}, {{metric.label}} has been following a new, upward trend line that "
        "averages {{current_growth}}% {{pop}} growth. The prior trend for this metric lasted {{"
        "prior_trend_days}} days and averaged {{prior_trend_growth}}% {{pop}} growth.",
    },
    StoryType.NEW_DOWNWARD_TREND: {
        "title": "New downward trend",
        # e.g.,  Since 07-04-2024, NewBizDeals has been following a new, downward trend line that averages 5% d/d
        # growth. The prior trend for this metric lasted 30 days and averaged 10% d/d growth.
        "template": "Since {{trend_start_date}}, {{metric.label}} has been following a new, downward trend line that "
        "averages {{current_growth}}% {{pop}} growth. The prior trend for this metric lasted {{"
        "prior_trend_days}} days and averaged {{prior_trend_growth}}% {{pop}} growth.",
    },
    StoryType.PERFORMANCE_PLATEAU: {
        "title": "Performance has leveled off",
        # e.g.,  Since 07-04-2024, NewBizDeals growth has steadied into a new normal, hovering around a 30day
        # average of 8%.
        "template": "Since {{trend_start_date}}, {{metric.label}} growth has steadied into a new normal, hovering "
        "around a {{grain}} average of {{current_growth}}%.",
    },
    StoryType.SPIKE: {
        "title": "Performance spike above {{grain}} trend",
        # e.g.,  NewBizDeals is currently performing at 12% above its normal range. This may indicate an anomaly,
        # a data issue, or a fundamental change in how this metric performs.
        "template": "{{metric.label}} is currently performing at {{current_growth}}% {{position}} its normal "
        "range. This may indicate an anomaly, a data issue, or a fundamental change in how this metric "
        "performs.",
    },
    StoryType.DROP: {
        "title": "Performance drop below {{grain}} trend",
        # e.g.,  NewBizDeals is currently performing at -25% below its normal range. This may indicate an anomaly,
        # a data issue, or a fundamental change in how this metric performs.
        "template": "{{metric.label}} is currently performing at {{current_growth}}% {{position}} its normal "
        "range. This may indicate an anomaly, a data issue, or a fundamental change in how this metric "
        "performs.",
    },
}

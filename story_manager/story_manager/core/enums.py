from commons.models.enums import StrEnum


class StoryGenre(StrEnum):
    """
    Defines the genre of the story
    """

    GROWTH = "GROWTH"
    TRENDS = "TRENDS"
    SWINGS = "SWINGS"
    PERFORMANCE = "PERFORMANCE"
    MIX_SHIFT = "MIX_SHIFT"
    SEGMENT_IMPORTANCE = "SEGMENT_IMPORTANCE"
    INFLUENCE = "INFLUENCE"
    ROOT_CAUSE = "ROOT_CAUSE"


class StoryType(StrEnum):
    """
    Defines the type of the story for each genre
    """

    # growth stories
    SLOWING_GROWTH = "SLOWING_GROWTH"
    ACCELERATING_GROWTH = "ACCELERATING_GROWTH"
    # trend stories
    NEW_UPWARD_TREND = "NEW_UPWARD_TREND"
    NEW_DOWNWARD_TREND = "NEW_DOWNWARD_TREND"
    STICKY_DOWNWARD_TREND = "STICKY_DOWNWARD_TREND"
    NEW_NORMAL = "NEW_NORMAL"
    # swing stories
    RECORD_HIGH = "RECORD_HIGH"
    RECORD_LOW = "RECORD_LOW"
    METRIC_SPIKE = "METRIC_SPIKE"
    METRIC_DROP = "METRIC_DROP"
    SEGMENT_SPIKE = "SEGMENT_SPIKE"
    SEGMENT_DROP = "SEGMENT_DROP"
    # performance stories
    LIKELY_RED = "LIKELY_RED"
    LIKELY_YELLOW = "LIKELY_YELLOW"
    LIKELY_GREEN = "LIKELY_GREEN"
    REQUIRED_PERFORMANCE = "REQUIRED_PERFORMANCE"
    # mix shift stories
    GROWING_SEGMENT = "GROWING_SEGMENT"
    SHRINKING_SEGMENT = "SHRINKING_SEGMENT"
    # segment importance stories
    IMPROVING_SEGMENT = "IMPROVING_SEGMENT"
    WORSENING_SEGMENT = "WORSENING_SEGMENT"
    TOP3_SEGMENT = "TOP3_SEGMENT"
    BOTTOM3_SEGMENT = "BOTTOM3_SEGMENT"
    # influence stories
    STRONGER_INFLUENCE = "STRONGER_INFLUENCE"
    WEAKER_INFLUENCE = "WEAKER_INFLUENCE"
    # root cause stories
    SEASONAL_DRIFT = "SEASONAL_DRIFT"
    SEGMENT_DRIFT = "SEGMENT_DRIFT"
    INFLUENCE_DRIFT = "INFLUENCE_DRIFT"
    COMPONENT_DRIFT = "COMPONENT_DRIFT"


GENRE_TO_STORY_TYPE_MAPPING: dict[str, list[str]] = {
    StoryGenre.GROWTH: [
        StoryType.SLOWING_GROWTH,
        StoryType.ACCELERATING_GROWTH,
    ],
    StoryGenre.TRENDS: [
        StoryType.NEW_UPWARD_TREND,
        StoryType.NEW_DOWNWARD_TREND,
        StoryType.STICKY_DOWNWARD_TREND,
        StoryType.NEW_NORMAL,
    ],
    StoryGenre.SWINGS: [
        StoryType.RECORD_HIGH,
        StoryType.RECORD_LOW,
        StoryType.METRIC_SPIKE,
        StoryType.METRIC_DROP,
        StoryType.SEGMENT_SPIKE,
        StoryType.SEGMENT_DROP,
    ],
    StoryGenre.PERFORMANCE: [
        StoryType.LIKELY_RED,
        StoryType.LIKELY_YELLOW,
        StoryType.LIKELY_GREEN,
        StoryType.REQUIRED_PERFORMANCE,
    ],
    StoryGenre.MIX_SHIFT: [
        StoryType.GROWING_SEGMENT,
        StoryType.SHRINKING_SEGMENT,
    ],
    StoryGenre.SEGMENT_IMPORTANCE: [
        StoryType.IMPROVING_SEGMENT,
        StoryType.WORSENING_SEGMENT,
        StoryType.TOP3_SEGMENT,
        StoryType.BOTTOM3_SEGMENT,
    ],
    StoryGenre.INFLUENCE: [
        StoryType.STRONGER_INFLUENCE,
        StoryType.WEAKER_INFLUENCE,
    ],
    StoryGenre.ROOT_CAUSE: [
        StoryType.SEASONAL_DRIFT,
        StoryType.SEGMENT_DRIFT,
        StoryType.INFLUENCE_DRIFT,
        StoryType.COMPONENT_DRIFT,
    ],
}

# Story type meta-information
STORY_TYPES_META: dict[str, dict[str, str]] = {
    StoryType.SLOWING_GROWTH: {
        "label": "Slowing Growth",
        "description": "Indicates a decrease in the rate of growth for this metric, indicating a deceleration in "
        "progress or expansion.",
        # e.g., The d/d growth rate of revenue is slowing and has dropped from 10% last month to 5% now.
        "template": "The {{pop}} growth rate of {{metric.label}} is slowing and has dropped from {{current_growth}}% "
        "last {{current_growth}} to {{reference_growth}}% now.",
    },
    StoryType.ACCELERATING_GROWTH: {
        "label": "Accelerating Growth",
        "description": "Indicates an increase in the rate of growth for this metric, indicating a quickening pace of "
        "improvement or success.",
        "template": "The {{pop}} growth rate of {{metric.label}} is speeding up and has increased "
        "from {{reference_growth}}% last {{reference_period}} to {{current_growth}}% now.",
    },
    StoryType.NEW_UPWARD_TREND: {
        "label": "New Upward Trend",
        "description": "Indicates a recent, significant, and sustained upward shift in this metric, indicating a "
        "pattern expected to continue into the future.",
    },
    StoryType.NEW_DOWNWARD_TREND: {
        "label": "New Downward Trend",
        "description": "Indicates a recent, significant, and sustained downward shift in this metric, indicating a "
        "pattern expected to continue into the future.",
    },
    StoryType.STICKY_DOWNWARD_TREND: {
        "label": "Sticky Downward Trend",
        "description": "Indicates that a negative trend is a persistent trend.",
    },
    StoryType.NEW_NORMAL: {
        "label": "New Normal",
        "description": "Identifies a shift in the baseline or average performance of this metric, suggesting a change "
        "in standard operations or expectations.",
    },
    StoryType.RECORD_HIGH: {
        "label": "Record High",
        "description": "Indicates the highest value ever reached by this specific metric in its recorded history.",
    },
    StoryType.RECORD_LOW: {
        "label": "Record Low",
        "description": "Indicates the lowest value ever reached by this specific metric in its recorded history.",
    },
    StoryType.METRIC_SPIKE: {
        "label": "Metric Spike",
        "description": "Indicates a sudden and sharp increase in this metric over a short period, suggesting an "
        "anomaly or unexpected event.",
    },
    StoryType.METRIC_DROP: {
        "label": "Metric Drop",
        "description": "Indicates a sudden and sharp decrease in this metric over a short period, suggesting an "
        "anomaly or unexpected event.",
    },
    StoryType.SEGMENT_SPIKE: {
        "label": "Segment Spike",
        "description": "Indicates a sudden, sharp increase in a single segment of a specific dimension "
        "for this metric.",
    },
    StoryType.SEGMENT_DROP: {
        "label": "Segment Drop",
        "description": "Indicates a sudden, sharp decrease in a single segment of a specific dimension "
        "for this metric.",
    },
    StoryType.LIKELY_RED: {
        "label": "Likely Red",
        "description": "Indicates a high probability of this metric becoming 'Red' against target.",
    },
    StoryType.LIKELY_YELLOW: {
        "label": "Likely Yellow",
        "description": "Indicates a high probability of this metric becoming 'Yellow'' against target.",
    },
    StoryType.LIKELY_GREEN: {
        "label": "Likely Green",
        "description": "Indicates a high probability of this metric getting to 'Green' against target.",
    },
    StoryType.REQUIRED_PERFORMANCE: {
        "label": "Required Performance",
        "description": "Indicates the required metric performance needed to return to target.",
    },
    StoryType.GROWING_SEGMENT: {
        "label": "Growing Segment",
        "description": "Indicates that a given dimensional slice is more represented than it has been in the past.",
    },
    StoryType.SHRINKING_SEGMENT: {
        "label": "Shrinking Segment",
        "description": "TIndicates that a given dimensional slice is less represented than it has been in the past.",
    },
    StoryType.IMPROVING_SEGMENT: {
        "label": "Improving Segment",
        "description": "Indicates that the average metric value for a given segment is improving.",
    },
    StoryType.WORSENING_SEGMENT: {
        "label": "Worsening Segment",
        "description": "Indicates that the average metric value for a given segment is worsening.",
    },
    StoryType.TOP3_SEGMENT: {
        "label": "Top 3 Segment",
        "description": "Indicates that this segment is associated with one of the three highest average metric values "
        "across all segments.",
    },
    StoryType.BOTTOM3_SEGMENT: {
        "label": "Bottom 3 Segment",
        "description": "Indicates that this segment is associated with one of the three lowest average metric values "
        "across all segments.",
    },
    StoryType.STRONGER_INFLUENCE: {
        "label": "Stronger Influence",
        "description": "Indicates that the relationship between an influence and its output metric has grown stronger.",
    },
    StoryType.WEAKER_INFLUENCE: {
        "label": "Weaker Influence",
        "description": "Indicates that the relationship between an influence and its output metric has become weaker.",
    },
    StoryType.SEASONAL_DRIFT: {
        "label": "Seasonal Drift",
        "description": "Highlights movements in this metric attributable to seasonality.",
    },
    StoryType.SEGMENT_DRIFT: {
        "label": "Segment Drift",
        "description": "Highlights movements in this metric attributable to changes in a single segment of a specific "
        "dimension.",
    },
    StoryType.INFLUENCE_DRIFT: {
        "label": "Influence Drift",
        "description": "Highlights movements in this metric attributable to changes in other metrics correlated with "
        "this one.",
    },
    StoryType.COMPONENT_DRIFT: {
        "label": "Component Drift",
        "description": "Highlights movements in this metric attributable to changes in specific components or "
        "elements of this metric.",
    },
}

from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)

FILTER_MAPPING = {
    (Digest.PORTFOLIO, Section.OVERVIEW): {
        "story_groups": [StoryGroup.GOAL_VS_ACTUAL.value, StoryGroup.LONG_RANGE.value]
    },
    (Digest.PORTFOLIO, Section.STATUS_CHANGES): {"story_groups": [StoryGroup.STATUS_CHANGE.value]},
    (Digest.PORTFOLIO, Section.LIKELY_MISSES): {"story_types": [StoryType.LIKELY_OFF_TRACK.value]},
    (Digest.PORTFOLIO, Section.BIG_MOVES): {
        "story_groups": [StoryGroup.RECORD_VALUES.value, StoryGroup.TREND_EXCEPTIONS.value]
    },
    (Digest.PORTFOLIO, Section.PROMISING_TRENDS): {
        "story_types": [
            StoryType.IMPROVING_PERFORMANCE.value,
            StoryType.ACCELERATING_GROWTH.value,
            StoryType.NEW_UPWARD_TREND.value,
        ]
    },
    (Digest.PORTFOLIO, Section.CONCERNING_TRENDS): {
        "story_types": [
            StoryType.WORSENING_PERFORMANCE.value,
            StoryType.SLOWING_GROWTH.value,
            StoryType.NEW_DOWNWARD_TREND.value,
            StoryType.PERFORMANCE_PLATEAU.value,
        ]
    },
    (Digest.METRIC, Section.WHAT_IS_HAPPENING): {
        "story_types": [StoryType.REQUIRED_PERFORMANCE.value],
        "story_groups": [
            StoryGroup.GOAL_VS_ACTUAL.value,
            StoryGroup.STATUS_CHANGE.value,
            StoryGroup.LIKELY_STATUS.value,
            StoryGroup.GROWTH_RATES.value,
            StoryGroup.RECORD_VALUES.value,
        ],
        "genres": StoryGenre.TRENDS.value,
    },
    (Digest.METRIC, Section.WHY_IS_IT_HAPPENING): {"story_types": []},
    (Digest.METRIC, Section.WHAT_HAPPENS_NEXT): {"story_groups": [StoryGroup.LIKELY_STATUS.value]},
}

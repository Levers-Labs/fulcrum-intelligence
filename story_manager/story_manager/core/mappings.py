from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)

# Mapping of predefined filters for various digest and section combinations. Each key in the dictionary is a tuple
# consisting of a Digest type and a Section type. The value is another dictionary specifying the applicable story
# filters. All the mapping is from the file:
# https://docs.google.com/spreadsheets/d/1qf2P5wUkfMeGGc7aG0ffNRLaIEOauBJEMWLo-IJPbA0/edit#gid=259135405

FILTER_MAPPING = {
    (Digest.PORTFOLIO, Section.OVERVIEW): {"story_groups": [StoryGroup.GOAL_VS_ACTUAL, StoryGroup.LONG_RANGE]},
    (Digest.PORTFOLIO, Section.STATUS_CHANGES): {"story_groups": [StoryGroup.STATUS_CHANGE]},
    (Digest.PORTFOLIO, Section.LIKELY_MISSES): {"story_types": [StoryType.LIKELY_OFF_TRACK]},
    (Digest.PORTFOLIO, Section.BIG_MOVES): {"story_groups": [StoryGroup.RECORD_VALUES, StoryGroup.TREND_EXCEPTIONS]},
    (Digest.PORTFOLIO, Section.PROMISING_TRENDS): {
        "story_types": [
            StoryType.IMPROVING_PERFORMANCE,
            StoryType.ACCELERATING_GROWTH,
            StoryType.NEW_UPWARD_TREND,
        ]
    },
    (Digest.PORTFOLIO, Section.CONCERNING_TRENDS): {
        "story_types": [
            StoryType.WORSENING_PERFORMANCE,
            StoryType.SLOWING_GROWTH,
            StoryType.NEW_DOWNWARD_TREND,
            StoryType.PERFORMANCE_PLATEAU,
        ]
    },
    (Digest.METRIC, Section.WHAT_IS_HAPPENING): {
        "story_types": [StoryType.REQUIRED_PERFORMANCE],
        "story_groups": [
            StoryGroup.GOAL_VS_ACTUAL,
            StoryGroup.STATUS_CHANGE,
            StoryGroup.LIKELY_STATUS,
            StoryGroup.GROWTH_RATES,
            StoryGroup.RECORD_VALUES,
        ],
        "genres": [StoryGenre.TRENDS],
    },
    (Digest.METRIC, Section.WHY_IS_IT_HAPPENING): {"story_types": []},
    (Digest.METRIC, Section.WHAT_HAPPENS_NEXT): {"story_groups": [StoryGroup.LIKELY_STATUS]},
}

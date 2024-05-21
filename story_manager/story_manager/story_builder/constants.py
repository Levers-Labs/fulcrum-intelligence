from typing import Any

from commons.models.enums import Granularity
from story_manager.core.enums import StoryGroup

GRAIN_META: dict[str, Any] = {
    Granularity.DAY: {"pop": "d/d", "delta": {"days": 1}, "eoi": "EOD", "interval": "daily"},
    Granularity.WEEK: {"pop": "w/w", "delta": {"weeks": 1}, "eoi": "EOW", "interval": "weekly"},
    Granularity.MONTH: {"pop": "m/m", "delta": {"months": 1}, "eoi": "EOM", "interval": "monthly"},
    Granularity.QUARTER: {"pop": "q/q", "delta": {"months": 3}, "eoi": "EOQ", "interval": "quarterly"},
    Granularity.YEAR: {"pop": "y/y", "delta": {"years": 1}, "eoi": "EOY", "interval": "yearly"},
}

STORY_GROUP_TIME_DURATIONS: dict[str, Any] = {
    StoryGroup.TREND_CHANGES: {
        Granularity.DAY: {
            # minimum number of data points required for analysis
            "min": 30,
            # period which will be used for analysis and story calculation
            "input": 90,
            # period which will be used for story output and visualization
            "output": 20,
        },
        Granularity.WEEK: {"min": 20, "input": 104, "output": 20},
        Granularity.MONTH: {"min": 20, "input": 60, "output": 10},
    },
    StoryGroup.TREND_EXCEPTIONS: {
        Granularity.DAY: {"min": 30, "input": 90, "output": 20},
        Granularity.WEEK: {"min": 20, "input": 104, "output": 20},
        Granularity.MONTH: {"min": 20, "input": 60, "output": 10},
    },
    StoryGroup.LONG_RANGE: {
        Granularity.DAY: {"min": 7, "input": 30, "output": 30},
        Granularity.WEEK: {"min": 4, "input": 14, "output": 14},
        Granularity.MONTH: {"min": 3, "input": 4, "output": 4},
    },
    StoryGroup.GROWTH_RATES: {
        Granularity.DAY: {"min": 8, "input": 30, "output": 30},
        Granularity.WEEK: {"min": 8, "input": 14, "output": 14},
        Granularity.MONTH: {"min": 8, "input": 12, "output": 12},
    },
}

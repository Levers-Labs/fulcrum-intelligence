"""
Store v2 enums for story evaluators.
"""

from enum import Enum


class StoryGenre(str, Enum):
    """
    Defines the genre of the story
    """

    PERFORMANCE = "performance", "Performance"


class StoryType(str, Enum):
    """
    Defines the type of the story
    """

    ON_TRACK = "on_track", "On Track"
    OFF_TRACK = "off_track", "Off Track"
    IMPROVING_STATUS = "improving_status", "Improving Status"
    WORSENING_STATUS = "worsening_status", "Worsening Status"
    HOLD_STEADY = "hold_steady", "Hold Steady"


class StoryGroup(str, Enum):
    """
    Defines the group of the story
    """

    GOAL_VS_ACTUAL = "goal_vs_actual", "Goal vs Actual"
    STATUS_CHANGE = "status_change", "Status Change"
    LIKELY_STATUS = "likely_status", "Likely Status"

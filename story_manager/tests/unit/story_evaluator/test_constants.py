"""
Tests for the story evaluator constants module.
"""

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.constants import STORY_TEMPLATES


def test_story_templates():
    """Test STORY_TEMPLATES constant."""
    # Test ON_TRACK template
    on_track = STORY_TEMPLATES[StoryType.ON_TRACK]
    assert "is on track" in on_track["title"]
    assert "is at" in on_track["detail"]
    assert "beating its target" in on_track["detail"]
    assert "streak of exceeding target" in on_track["detail"]

    # Test OFF_TRACK template
    off_track = STORY_TEMPLATES[StoryType.OFF_TRACK]
    assert "is off track" in off_track["title"]
    assert "is at" in off_track["detail"]
    assert "missing its target" in off_track["detail"]
    assert "gap" in off_track["detail"]

    # Test IMPROVING_STATUS template
    improving = STORY_TEMPLATES[StoryType.IMPROVING_STATUS]
    assert "status has improved" in improving["title"]
    assert "now On-Track" in improving["detail"]
    assert "previously being Off-Track" in improving["detail"]

    # Test WORSENING_STATUS template
    worsening = STORY_TEMPLATES[StoryType.WORSENING_STATUS]
    assert "status has worsened" in worsening["title"]
    assert "now Off-Track" in worsening["detail"]
    assert "previously being On-Track" in worsening["detail"]

    # Test HOLD_STEADY template
    hold_steady = STORY_TEMPLATES[StoryType.HOLD_STEADY]
    assert "needs to hold steady" in hold_steady["title"]
    assert "already performing at its target level" in hold_steady["detail"]
    assert "needs to maintain this lead" in hold_steady["detail"]

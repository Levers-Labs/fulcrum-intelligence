from datetime import datetime

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.schemas import StoryDetail


def test_story_detail_for_story_group():
    """Test digest and section population for GOAL_VS_ACTUAL story group"""
    story = StoryDetail(
        id=1,
        genre=StoryGenre.PERFORMANCE,
        story_group=StoryGroup.GOAL_VS_ACTUAL,
        story_type=StoryType.IMPROVING_STATUS,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="test title",
        title_template="test template",
        detail="test detail",
        detail_template="test template",
        variables={},
        series=[],
        story_date=datetime.now(),
        is_salient=True,
        in_cool_off=False,
        is_heuristic=True,
    )

    assert set(story.digest) == {Digest.PORTFOLIO, Digest.METRIC}
    assert set(story.section) == {Section.OVERVIEW, Section.WHAT_IS_HAPPENING}


def test_story_detail_multiple_matches():
    """Test when a story matches multiple digest/section combinations"""
    story = StoryDetail(
        id=1,
        genre=StoryGenre.PERFORMANCE,
        story_group=StoryGroup.LIKELY_STATUS,
        story_type=StoryType.LIKELY_OFF_TRACK,
        grain=Granularity.DAY,
        metric_id="test_metric",
        title="test title",
        title_template="test template",
        detail="test detail",
        detail_template="test template",
        variables={},
        series=[],
        story_date=datetime.now(),
        is_salient=True,
        in_cool_off=False,
        is_heuristic=True,
    )

    assert set(story.digest) == {Digest.PORTFOLIO, Digest.METRIC}
    assert Section.LIKELY_MISSES in story.section
    assert Section.WHAT_HAPPENS_NEXT in story.section

from datetime import datetime

import pytest
from sqlalchemy import select

from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.filters import StoryFilter
from story_manager.core.models import Story


@pytest.fixture
def base_query():
    return select(Story)


def test_basic_filters(base_query):
    filters = {
        "metric_ids": ["metric1", "metric2"],
        "story_date_start": datetime(2023, 1, 1),
        "story_date_end": datetime(2023, 1, 31),
        "genres": [StoryGenre.TRENDS],
        "story_types": [StoryType.STABLE_TREND],
        "story_groups": [StoryGroup.TREND_CHANGES],
        "grains": [Granularity.DAY],
    }

    query = StoryFilter.apply_filters(base_query, filters)
    sql = str(query.compile(compile_kwargs={"literal_binds": True}))

    assert "WHERE" in sql
    assert "story.metric_id IN ('metric1', 'metric2')" in sql
    assert "story.story_date >= '2023-01-01 00:00:00'" in sql
    assert "story.story_date <= '2023-01-31 00:00:00'" in sql
    assert "story.genre IN ('TRENDS')" in sql
    assert "story.story_type IN ('STABLE_TREND')" in sql
    assert "story.story_group IN ('TREND_CHANGES')" in sql
    assert "story.grain IN ('DAY')" in sql


def test_digest_section_filters(base_query):
    filters = {
        "digest": Digest.METRIC,
        "section": Section.WHY_IS_IT_HAPPENING,
    }

    query = StoryFilter.apply_filters(base_query, filters)
    sql = str(query.compile(compile_kwargs={"literal_binds": True}))
    assert "WHERE" in sql
    assert "story.story_group IN ('SEGMENT_DRIFT', 'COMPONENT_DRIFT')" in sql


def test_combined_filters(base_query):
    filters = {
        "metric_ids": ["metric1"],
        "story_date_start": datetime(2023, 1, 1),
        "digest": Digest.PORTFOLIO,
        "section": Section.LIKELY_MISSES,
        "genres": [StoryGenre.PERFORMANCE],
    }

    query = StoryFilter.apply_filters(base_query, filters)
    sql = str(query.compile(compile_kwargs={"literal_binds": True}))

    assert "WHERE" in sql
    assert "story.metric_id IN ('metric1')" in sql
    assert "story.story_date >= '2023-01-01 00:00:00'" in sql
    assert "story.genre IN ('PERFORMANCE')" in sql
    assert "story.story_type IN ('LIKELY_OFF_TRACK')" in sql


def test_empty_filters(base_query):
    filters = {}

    query = StoryFilter.apply_filters(base_query, filters)
    sql = str(query.compile(compile_kwargs={"literal_binds": True}))

    assert "WHERE" not in sql


def test_null_values(base_query):
    filters = {
        "metric_ids": None,
        "story_date_start": None,
        "genres": None,
    }

    query = StoryFilter.apply_filters(base_query, filters)
    sql = str(query.compile(compile_kwargs={"literal_binds": True}))

    assert "WHERE" not in sql

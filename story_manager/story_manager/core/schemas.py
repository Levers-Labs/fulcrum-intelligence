from datetime import datetime

from commons.models import BaseModel
from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)


class StoryTypeSchema(BaseModel):
    type: StoryType
    label: str
    description: str


class StoryGenreSchema(BaseModel):
    genre: StoryGenre
    label: str
    types: list[StoryTypeSchema]


class StoryGroupMeta(BaseModel):
    group: StoryGroup
    grains: list[Granularity]


class StoryDetail(BaseModel):
    id: int
    genre: StoryGenre
    story_group: StoryGroup
    story_type: StoryType
    grain: Granularity
    metric_id: str
    title: str
    title_template: str
    detail: str
    detail_template: str
    variables: dict
    series: list
    story_date: datetime
    is_salient: bool
    in_cool_off: bool
    is_heuristic: bool

    digest: Digest | None = None
    section: Section | None = None

from datetime import datetime

from pydantic import model_validator

from commons.models import BaseModel
from commons.models.enums import Granularity
from story_manager.core.enums import (
    Digest,
    Section,
    StoryGenre,
    StoryGroup,
    StoryType,
)
from story_manager.core.mappings import FILTER_MAPPING


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
    version: int
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

    digest: list[Digest] | None = None
    section: list[Section] | None = None

    @model_validator(mode="after")
    def populate_digest_and_section(self):
        matching_digests = set()
        matching_sections = set()

        for (digest, section), filters in FILTER_MAPPING.items():
            # Check if story_group matches
            if "story_groups" in filters and self.story_group in filters["story_groups"]:
                matching_digests.add(digest)
                matching_sections.add(section)

            # Check if story_type matches
            if "story_types" in filters and self.story_type in filters["story_types"]:
                matching_digests.add(digest)
                matching_sections.add(section)

        # Only update if matches were found
        if matching_digests:
            self.digest = list(matching_digests)
        if matching_sections:
            self.section = list(matching_sections)

        return self


class StoryStatsResponse(BaseModel):
    """Story stats response model"""

    story_date: datetime
    count: int

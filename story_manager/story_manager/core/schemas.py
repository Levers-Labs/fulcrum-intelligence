from commons.models import BaseModel
from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType


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

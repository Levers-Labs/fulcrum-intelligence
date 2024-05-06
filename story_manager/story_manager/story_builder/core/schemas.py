from commons.models import BaseModel
from story_manager.core.enums import StoryGenre, StoryType


class StoryTypeSchema(BaseModel):
    type: StoryType
    label: str
    description: str


class StoryGenreSchema(BaseModel):
    genre: StoryGenre
    label: str
    types: list[StoryTypeSchema]

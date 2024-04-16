from typing import Any

from pydantic import model_validator
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from commons.db.models import BaseTimeStampedModel
from story_manager.core.enums import GENRE_TO_STORY_TYPE_MAPPING, StoryGenre, StoryType


class StorySchemaMixin:
    __table_args__ = {"schema": "story_store"}


class Story(StorySchemaMixin, BaseTimeStampedModel, table=True):
    """
    Story model
    """

    genre: StoryGenre = Field(max_length=64, index=True)
    story_type: StoryType = Field(max_length=64, index=True)
    metric_id: str = Field(max_length=255, index=True)
    description: str = Field(sa_type=Text)
    template: str = Field(sa_type=Text)
    text: str = Field(sa_type=Text)
    variables: dict = Field(default_factory=dict, sa_type=JSONB)
    series: dict = Field(default_factory=dict, sa_type=JSONB)

    @model_validator(mode="before")
    @classmethod
    def check_valid_type_genre_combination(cls, data: Any) -> Any:
        """
        Check if the story type is valid for the genre
        """
        genre = data.get("genre")
        story_type = data.get("type")

        genre_story_types = GENRE_TO_STORY_TYPE_MAPPING.get(genre)

        if genre_story_types is None:
            raise ValueError(f"Invalid genre '{genre}'")

        if story_type not in genre_story_types:
            raise ValueError(f"Invalid type '{story_type}' for genre '{genre}'")

        return data

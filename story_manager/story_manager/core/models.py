from datetime import datetime
from typing import Any

from pydantic import model_validator
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from commons.db.models import BaseTimeStampedModel
from commons.models.enums import Granularity
from story_manager.core.enums import (
    GROUP_TO_STORY_TYPE_MAPPING,
    StoryGenre,
    StoryGroup,
    StoryType,
)


class StorySchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "story_store"}


class Story(StorySchemaBaseModel, table=True):  # type: ignore
    """
    Story model
    """

    genre: StoryGenre = Field(sa_column=Column(Enum(StoryGenre, name="storygenre", inherit_schema=True), index=True))
    story_group: StoryGroup = Field(
        sa_column=Column(Enum(StoryGroup, name="storygroup", inherit_schema=True), index=True)
    )
    story_type: StoryType = Field(sa_column=Column(Enum(StoryType, name="storytype", inherit_schema=True), index=True))
    grain: Granularity = Field(sa_column=Column(Enum(Granularity, name="grain", inherit_schema=True), index=True))

    metric_id: str = Field(max_length=255, index=True)
    title: str = Field(sa_type=Text)
    title_template: str = Field(sa_type=Text)
    detail: str = Field(sa_type=Text)
    detail_template: str = Field(sa_type=Text)
    variables: dict = Field(default_factory=dict, sa_type=JSONB)
    series: list = Field(default_factory=list, sa_type=JSONB)
    story_date: datetime = Field(sa_column=Column(DateTime, nullable=False, index=True))
    is_salient: bool = Field(default=False, sa_column=Column(Boolean, default=False))

    @model_validator(mode="before")
    @classmethod
    def check_valid_type_group_combination(cls, data: Any) -> Any:
        """
        Check if the story type is valid for the group
        """
        group = data.get("story_group")
        story_type = data.get("story_type")

        group_story_types = GROUP_TO_STORY_TYPE_MAPPING.get(group)

        if group_story_types is None:
            raise ValueError(f"Invalid group '{group}'")

        if story_type not in group_story_types:
            raise ValueError(f"Invalid type '{story_type}' for group '{group}'")

        return data

    async def set_salience(self) -> None:
        """
        Automatically update the salience of the story based on the heuristic expressions.
        """
        from story_manager.story_builder.salience import SalienceEvaluator

        evaluator = SalienceEvaluator(self.story_type, self.grain, self.variables)
        self.is_salient = await evaluator.evaluate_salience()


class HeuristicExpression(StorySchemaBaseModel, table=True):
    story_type: StoryType = Field(sa_column=Column(String(255), nullable=False))
    grain: Granularity = Field(sa_column=Column(String(255), nullable=False))
    expression: str | None = Field(sa_column=Column(String(255), nullable=True))  # type: ignore

    __table_args__ = (
        UniqueConstraint("story_type", "grain", name="uix_story_type_grain"),  # type: ignore
        {"schema": "story_store"},
    )

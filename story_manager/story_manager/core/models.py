from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.models import BaseTimeStampedTenantModel
from commons.models.enums import Granularity
from story_manager.core.enums import StoryGenre, StoryGroup, StoryType


class StorySchemaBaseModel(BaseTimeStampedTenantModel):
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
    is_salient: bool = Field(default=True, sa_column=Column(Boolean, default=True))
    in_cool_off: bool = Field(default=False, sa_column=Column(Boolean, default=False))
    is_heuristic: bool = Field(default=True, sa_column=Column(Boolean, default=True))
    # for new stories, we need to set the version to 2
    version: int = Field(default=1, sa_column=Column(Integer, default=1, server_default="1"))
    # For version 2, we need to add the pattern_run_id
    pattern_run_id: int | None = Field(default=None, sa_column=Column(Integer, nullable=True, index=True))
    # Dimension name for proper story isolation (e.g., "customer_success_manager_region", "reseller_partner")
    dimension_name: str | None = Field(default=None, max_length=255, index=True)
    # Pattern name that generated this story (e.g., "historical_performance", "dimension_analysis")
    evaluation_pattern: str | None = Field(default=None, max_length=255, index=True)

    # Composite index for common query patterns
    __table_args__ = (  # type: ignore
        Index(
            "ix_story_tenant_version_date_metric_grain_type",
            "tenant_id",
            "version",
            desc("story_date"),
            "metric_id",
            "grain",
            "story_type",
        ),
        {"schema": "story_store"},
    )

    async def set_heuristics(self, session: AsyncSession) -> None:
        """
        Automatically update the salience of the story based on the heuristic expressions.

        This method uses the StoryHeuristicEvaluator class to determine if the story is salient.
        It fetches the heuristic expression from the database, renders it with the provided variables,
        and evaluates the rendered expression to update the 'is_salient', 'in_cool_off',
        and 'is_heuristic' attributes of the story.

        :param session: The database session used to fetch heuristic expressions and evaluate the story.
        """
        from story_manager.core.heuristics import StoryHeuristicEvaluator

        # Create an instance of StoryHeuristicEvaluator with the story's type, grain, and session
        evaluator = StoryHeuristicEvaluator(
            story_type=self.story_type,
            grain=self.grain,
            session=session,
            story_date=self.story_date,
            metric_id=self.metric_id,
            tenant_id=self.tenant_id,
            version=self.version,
        )

        # Evaluate the salience of the story and update the 'is_salient', 'in_cool_off', and 'is_heuristic' attributes
        self.is_salient, self.in_cool_off, self.is_heuristic = await evaluator.evaluate(self.variables)


class StoryConfig(StorySchemaBaseModel, table=True):
    """
    StoryConfig model
    """

    story_type: StoryType = Field(sa_column=Column(String(255), nullable=False))
    grain: Granularity = Field(sa_column=Column(String(255), nullable=False))
    heuristic_expression: str | None = Field(sa_column=Column(String(255), nullable=True))  # type: ignore
    cool_off_duration: int | None = Field(nullable=True)  # type: ignore
    version: int = Field(default=1, sa_column=Column(Integer, default=1, server_default="1"))

    __table_args__ = (
        UniqueConstraint("story_type", "grain", "tenant_id", "version", name="uix_story_type_grain_tenant_id_version"),  # type: ignore
        {"schema": "story_store"},
    )

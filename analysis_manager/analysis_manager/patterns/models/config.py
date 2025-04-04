"""Models for pattern configuration storage."""

from typing import Any

from sqlmodel import JSON, Column, Field

from analysis_manager.patterns.models.base import AnalysisSchemaBaseModel
from levers.models import AnalysisWindowConfig, DataSource, PatternConfig as PatternConfigModel


class PatternConfig(AnalysisSchemaBaseModel, table=True):  # type: ignore
    """Database model for pattern configurations."""

    __tablename__ = "pattern_configs"

    pattern_name: str = Field(index=True, unique=True)
    version: str
    description: str | None = None
    data_sources: list[DataSource] = Field(default_factory=list, sa_column=Column(JSON))
    analysis_window: AnalysisWindowConfig = Field(sa_column=Column(JSON))
    settings: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    meta: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))

    def to_pydantic(self) -> PatternConfigModel:
        """Convert the database model to a Pydantic model."""

        # Convert nested JSON objects
        data_sources = []
        for ds in self.data_sources:
            data_sources.append(DataSource.model_validate(ds))

        analysis_window = AnalysisWindowConfig.model_validate(self.analysis_window)

        return PatternConfigModel(
            pattern_name=self.pattern_name,
            version=self.version,
            description=self.description,
            data_sources=data_sources,
            analysis_window=analysis_window,
            settings=self.settings,
            meta=self.meta,
        )

    @classmethod
    def from_pydantic(cls, config: PatternConfigModel) -> "PatternConfig":
        """Create a database model from a Pydantic model."""
        return cls(
            pattern_name=config.pattern_name,
            version=config.version,
            description=config.description,
            data_sources=config.data_sources,
            analysis_window=config.analysis_window,
            settings=config.settings,
            meta=config.meta,
        )

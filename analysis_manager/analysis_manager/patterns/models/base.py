"""Base models for pattern storage."""

from datetime import date, datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Column, Field

from commons.db.models import BaseTimeStampedTenantModel
from levers.models.common import AnalysisWindow

T = TypeVar("T", bound=BaseModel)


class AnalysisSchemaBaseModel(BaseTimeStampedTenantModel):
    __table_args__ = {"schema": "analysis_store"}


class BasePatternResult(AnalysisSchemaBaseModel, Generic[T]):
    """Base class for all pattern results."""

    metric_id: str
    pattern: str
    version: str = "1.0.0"
    analysis_window: AnalysisWindow = Field(sa_column=Column(JSONB))
    # Date for which the pattern was evaluated
    analysis_date: date = Field(default_factory=date.today)
    # Time the pattern was evaluated
    evaluation_time: datetime = Field(default_factory=datetime.now)

    # Error information if pattern analysis fails
    # todo: add error model
    error: dict[str, Any] | None = Field(default=None, sa_column=Column(JSONB, nullable=True))

    # Define table arguments including additional indexes for common query patterns
    __table_args__ = (  # type: ignore
        # Composite indexes for common query patterns
        Index("idx_pattern_result_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_pattern_result_metric_tenant_pattern", "metric_id", "tenant_id", "pattern"),
        # unique constraint
        UniqueConstraint(
            "metric_id",
            "tenant_id",
            "pattern",
            "analysis_date",
        ),
        Index(
            "idx_pattern_result_metric_tenant_pattern_date",
            "metric_id",
            "tenant_id",
            "pattern",
            "analysis_date",
            postgresql_ops={"analysis_date": "DESC"},
        ),
        # Maintain schema definition from parent class
        {"schema": "analysis_store"},
    )

    def to_pattern_model(self) -> T:
        """Convert the database model to the pattern model."""
        # Convert the DB model to a dict and let model_validate ignore extra fields
        model_data = self.model_dump()
        return self.pattern_model_class.model_validate(model_data, from_attributes=True)

    @property
    def pattern_model_class(self) -> type[T]:
        """Get the pattern model class."""
        raise NotImplementedError("Subclasses must implement pattern_model_class")

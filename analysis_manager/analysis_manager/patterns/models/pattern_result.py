"""Pattern result model for storing analysis results."""

from datetime import date
from typing import Any

from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from commons.db.models import BaseTimeStampedTenantModel
from commons.models.enums import Granularity
from levers import Levers
from levers.models import AnalysisWindow


class AnalysisSchemaBaseModel(BaseTimeStampedTenantModel):
    """Base model for analysis schema."""

    __table_args__ = {"schema": "analysis_store"}


class PatternResult(AnalysisSchemaBaseModel, table=True):
    """Generic model for storing any pattern result as JSON."""

    __tablename__ = "pattern_results"

    metric_id: str = Field(index=True)
    pattern: str = Field(index=True)
    version: str = Field(default="1.0", index=True)
    grain: Granularity = Field(index=True)
    analysis_date: date = Field(default_factory=date.today, index=True)
    # Dimension name is only set for dimension analysis patterns
    dimension_name: str | None = Field(default=None, index=True)
    analysis_window: AnalysisWindow = Field(default=None, sa_type=JSONB)
    error: dict[str, Any] | None = Field(default=None, sa_type=JSONB, nullable=True)
    run_result: dict[str, Any] = Field(default_factory=dict, sa_type=JSONB)

    # Define table arguments including additional indexes for common query patterns
    __table_args__ = (  # type: ignore
        # Composite indexes for common query patterns
        Index("idx_pattern_result_metric_tenant", "metric_id", "tenant_id"),
        Index("idx_pattern_result_metric_tenant_pattern", "metric_id", "tenant_id", "pattern"),
        Index("idx_pattern_result_metric_tenant_grain", "metric_id", "tenant_id", "grain"),
        Index("idx_pattern_result_metric_tenant_grain_dimension", "metric_id", "tenant_id", "grain", "dimension_name"),
        # unique constraint
        UniqueConstraint(
            "metric_id",
            "tenant_id",
            "pattern",
            "version",
            "analysis_date",
            "grain",
            "dimension_name",
        ),
        # Maintain schema definition from parent class
        {"schema": "analysis_store"},
    )

    def to_pattern_model(self) -> Any:
        """
        Convert JSON data to the appropriate Pydantic model using the Levers API.

        Returns:
            The pattern result as a Pydantic model
        """

        # Add pattern field to the data if it's not there
        run_data = self.run_result.copy()
        if "pattern" not in run_data:
            run_data["pattern"] = self.pattern

        # Add pattern_run_id from database primary key
        run_data["pattern_run_id"] = self.id

        return Levers.load_pattern_model(run_data)

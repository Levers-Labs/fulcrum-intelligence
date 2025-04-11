"""
Common models used across patterns.
"""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel as PydanticBase, ConfigDict, Field

from levers.models import Granularity


class BaseModel(PydanticBase):
    """Base model for all models"""

    model_config = ConfigDict(
        use_enum_values=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        },
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert the pattern to a dictionary."""
        return self.model_dump(mode="json")


class AnalysisWindow(BaseModel):
    """Time window for analysis"""

    start_date: str  # ISO format date string 'YYYY-MM-DD'
    end_date: str  # ISO format date string 'YYYY-MM-DD'
    grain: Granularity = Granularity.DAY


class BasePattern(BaseModel):
    """Base model for all pattern outputs"""

    # Will be used when a loading pattern runs from a database
    pattern_run_id: int | None = Field(alias="id", default=None)

    pattern: str
    version: str = "1.0.0"
    metric_id: str
    analysis_window: AnalysisWindow
    num_periods: int = Field(default=0)
    analysis_date: date = Field(default_factory=date.today)
    evaluation_time: datetime = Field(default_factory=datetime.now)
    # Error information if pattern analysis fails
    error: dict[str, Any] | None = None

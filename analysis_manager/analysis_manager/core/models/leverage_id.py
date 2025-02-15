from __future__ import annotations

from datetime import date

from pydantic import Field

from commons.models import BaseModel
from commons.models.enums import Granularity


class LeverageRequest(BaseModel):
    metric_id: str
    start_date: date
    end_date: date
    grain: Granularity


class LeverageDetails(BaseModel):
    pct_diff: float = Field(description="Percentage difference w.r.t. the parent")
    pct_diff_root: float = Field(description="Percentage difference w.r.t. the root_metric_id")


class LeverageResponse(BaseModel):
    metric_id: str | None = None
    leverage: LeverageDetails | None = None
    components: list[LeverageResponse] | None = None

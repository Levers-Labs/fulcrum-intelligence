from __future__ import annotations

from datetime import date

from pydantic import Field

from commons.models import BaseModel


class ComponentDriftRequest(BaseModel):
    metric_id: str
    evaluation_start_date: date
    evaluation_end_date: date
    comparison_start_date: date
    comparison_end_date: date


class ComponentDrift(BaseModel):
    absolute_drift: float = Field(description="Absolute change in the evaluation value w.r.t. the comparison value")
    percentage_drift: float = Field(description="Percentage change in the evaluation value w.r.t. the comparison value")
    relative_impact: float = Field(description="Relative impact w.r.t. the parent component")
    marginal_contribution: float = Field(description="Marginal contribution w.r.t. the parent component")
    relative_impact_root: float = Field(description="Relative impact w.r.t. the root component")
    marginal_contribution_root: float = Field(description="Marginal contribution w.r.t. the root component")


class Component(BaseModel):
    metric_id: str
    evaluation_value: float
    comparison_value: float
    drift: ComponentDrift
    components: list[Component] | None = None

from __future__ import annotations

from datetime import date

from pydantic import Field
from sqlmodel import Relationship

from analysis_manager.core.models.base_model import AnalysisSchemaBaseModel


class ComponentDriftRequest(AnalysisSchemaBaseModel, table=True):  # type: ignore
    metric_id: str
    evaluation_start_date: date
    evaluation_end_date: date
    comparison_start_date: date
    comparison_end_date: date


class ComponentDrift(AnalysisSchemaBaseModel, table=True):  # type: ignore
    absolute_drift: float = Field(description="Absolute change in the evaluation value w.r.t. the comparison value")
    percentage_drift: float = Field(description="Percentage change in the evaluation value w.r.t. the comparison value")
    relative_impact: float = Field(description="Relative impact w.r.t. the parent component")
    marginal_contribution: float = Field(description="Marginal contribution w.r.t. the parent component")
    relative_impact_root: float = Field(description="Relative impact w.r.t. the root component")
    marginal_contribution_root: float = Field(description="Marginal contribution w.r.t. the root component")

    components: list[Component] = Relationship(back_populates="drift")


class Component(AnalysisSchemaBaseModel, table=True):  # type: ignore
    metric_id: str
    evaluation_value: float
    comparison_value: float
    drift: ComponentDrift | None = Relationship(back_populates="components")
    components: list[Component] | None = Relationship(back_populates="parent_component")

    class Config:
        orm_mode = True

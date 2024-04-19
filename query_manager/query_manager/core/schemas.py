from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import Extra, Field

from commons.models import BaseModel
from query_manager.core.enums import TargetAim


class Dimension(BaseModel):
    id: str
    label: str
    reference: str


class DimensionDetail(Dimension):
    definition: str
    members: list[str] | None = None


class MetricBase(BaseModel):
    id: str
    label: str
    abbreviation: str
    definition: str
    unit_of_measure: str
    unit: str
    complexity: str
    components: list[str] | None = None
    terms: list[str] | None = None
    metric_expression: MetricExpression | None = None
    grain_aggregation: str | None = None


class MetricList(MetricBase):
    pass


class MetricListResponse(BaseModel):
    results: list[MetricList]


class MetricExpression(BaseModel):
    type: Literal["metric"] = "metric"
    metric_id: str
    period: int = Field(0, description="Period for the metric, 0 denotes the current period")
    expression_str: str = Field(None, description="Expression string for the metric")
    expression: Expression | None = Field(None, description="Expression for the metric")


class Expression(BaseModel):
    type: Literal["expression"] = "expression"
    operator: str
    operands: list[MetricExpression | Expression]


class MetricDetail(MetricBase):
    output_of: str | None = None
    input_to: list[str] | None = None
    influences: list[str] | None = None
    influenced_by: list[str] | None = None
    periods: list[str] | None = None
    aggregations: list[str] | None = None
    owned_by_team: list[str] | None = None
    dimensions: list[Dimension] | None = None


class MetricValue(BaseModel):
    metric_id: str
    value: int | float


class MetricTimeSeriesValue(BaseModel, extra=Extra.allow):  # type: ignore
    metric_id: str | None = None
    value: int
    date: date


class MetricValuesResponse(BaseModel):
    data: list[MetricTimeSeriesValue] | None = Field(
        default=None,
        example=[  # type: ignore
            {
                "metric_id": "CAC",
                "value": 203,
                "date": "2022-09-01",
                "customer_segment": "Enterprise",
                "channel": "Online",
                "region": "Asia",
            },
        ],
    )
    url: str | None = Field(None, description="URL to the Parquet file")


class Target(BaseModel):
    id: str
    target_date: str
    aim: TargetAim
    target_value: int
    target_upper_bound: int | None = None
    target_lower_bound: int | None = None
    yellow_buffer: int | None = None
    red_buffer: int | None = None

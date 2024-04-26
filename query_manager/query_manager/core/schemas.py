from __future__ import annotations

import datetime
from typing import Literal

from pydantic import Extra, Field

from commons.models import BaseModel
from query_manager.core.enums import SemanticMemberType, TargetAim


class SemanticMetaTimeDimension(BaseModel):
    cube: str
    member: str


class SemanticMetaBase(BaseModel):
    cube: str
    member: str


class SemanticMetaDimension(SemanticMetaBase):
    member_type: Literal[SemanticMemberType.DIMENSION] = SemanticMemberType.DIMENSION


class SemanticMetaMetric(SemanticMetaBase):
    member_type: Literal[SemanticMemberType.MEASURE] = SemanticMemberType.MEASURE
    time_dimension: SemanticMetaTimeDimension


class MetricMetadata(BaseModel):
    semantic_meta: SemanticMetaMetric


class DimensionMetadata(BaseModel):
    semantic_meta: SemanticMetaDimension


class Dimension(BaseModel):
    id: str
    label: str
    reference: str
    metadata: DimensionMetadata


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
    metadata: MetricMetadata


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
    output_of: list[str] | None = None
    input_to: list[str] | None = None
    influences: list[str] | None = None
    influenced_by: list[str] | None = None
    periods: list[str] | None = None
    aggregations: list[str] | None = None
    owned_by_team: list[str] | None = None
    dimensions: list[Dimension] | None = None

    def get_dimension(self, dimension_id: str) -> Dimension | None:
        if self.dimensions is None:
            return None
        return next((dimension for dimension in self.dimensions if dimension.id == dimension_id), None)


class MetricValue(BaseModel, extra=Extra.allow):  # type: ignore
    metric_id: str | None = None
    value: int | float
    date: datetime.date | None = None


class MetricValuesResponse(BaseModel):
    data: list[MetricValue] | None = Field(
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

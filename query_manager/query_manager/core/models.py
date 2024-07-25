from typing import Literal, Union, List, Optional

from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, Relationship

from commons.db.models import BaseTimeStampedModel
from commons.models import BaseModel
from commons.models.enums import Granularity
from query_manager.core.enums import Complexity, SemanticMemberType


class MetricExpression(BaseModel):
    type: Literal["metric"] = "metric"
    metric_id: str
    coefficient: int | float = Field(1, description="Coefficient for the metric")
    period: int = Field(0, description="Period for the metric, 0 denotes the current period")
    expression_str: str = Field(None, description="Expression string for the metric")
    expression: Union["Expression", None] = Field(None, description="Expression for the metric")
    power: int | float = Field(1, description="Power for the metric")


class ConstantExpression(BaseModel):
    type: Literal["constant"] = "constant"
    value: int | float


class Expression(BaseModel):
    type: Literal["expression"] = "expression"
    operator: str
    operands: list[Union["MetricExpression", "Expression", "ConstantExpression"]]


class SemanticMetaBase(BaseModel):
    cube: str
    member: str


class SemanticMetaTimeDimension(BaseModel):
    cube: str
    member: str


class SemanticMetaMetric(SemanticMetaBase):
    member_type: Literal[SemanticMemberType.MEASURE] = SemanticMemberType.MEASURE
    time_dimension: SemanticMetaTimeDimension


class MetricMetadata(BaseModel):
    semantic_meta: SemanticMetaMetric


class SemanticMetaDimension(SemanticMetaBase):
    member_type: Literal[SemanticMemberType.DIMENSION] = SemanticMemberType.DIMENSION


class DimensionMetadata(BaseModel):
    semantic_meta: SemanticMetaDimension


class QuerySchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "query_store"}


class Dimensions(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Dimensions model
    """

    dimension_id: str = Field(max_length=50, index=True, primary_key=True, unique=True)
    label: str = Field(sa_type=Text, max_length=255)
    reference: str = Field(sa_type=Text, max_length=50, nullable=True)
    definition: str = Field(sa_type=Text, nullable=True)
    meta_data: DimensionMetadata = Field(sa_type=JSONB)


class Metric(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Metric model
    """

    metric_id: str = Field(max_length=255, index=True, primary_key=True, unique=True)
    label: str = Field(sa_type=Text)
    abbreviation: Optional[str] = Field(sa_type=Text, nullable=True)
    definition: Optional[str] = Field(sa_type=Text, nullable=True)
    unit_of_measure: Optional[str] = Field(sa_type=Text, nullable=True)
    unit: Optional[str] = Field(sa_type=Text, nullable=True)

    terms: List = Field(default_factory=list, sa_type=JSONB)
    complexity: Complexity = Field(sa_column=Column(Enum(Complexity, inherit_schema=True)))
    metric_expression: Optional[MetricExpression] = Field(sa_type=JSONB, nullable=True)
    periods: Optional[List[Granularity]] = Field(default_factory=list, sa_type=JSONB)
    grain_aggregation: Granularity = Field(
        sa_column=Column(Enum(Granularity, name="grain_aggregation", inherit_schema=True))
    )
    aggregations: List = Field(default_factory=list, sa_type=JSONB)
    owned_by_team: List = Field(default_factory=list, sa_type=JSONB)
    meta_data: MetricMetadata = Field(default_factory=dict, sa_type=JSONB)


class MetricDimensions(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Model for metric-dimensions many-to-many relationship
    """

    metric_id: str = Field(sa_column=Column(ForeignKey(Metric.metric_id), primary_key=True))
    dimension_id: str = Field(sa_column=Column(ForeignKey(Dimensions.dimension_id), primary_key=True))


# Add the dimensions relationship to Metric after MetricDimensions is defined
Metric.update_forward_refs()
Metric.dimensions = Relationship(link_model=MetricDimensions)
Metric.components = Relationship(
    sa_relationship_kwargs={"remote_side": "Metric.metric_id"},
    back_populates="components",
)
Metric.output_of = Relationship(
    sa_relationship_kwargs={"remote_side": "Metric.metric_id"},
    back_populates="input_to",
)
Metric.input_to = Relationship(
    sa_relationship_kwargs={"remote_side": "Metric.metric_id"},
    back_populates="output_of",
)
Metric.influences = Relationship(
    sa_relationship_kwargs={"remote_side": "Metric.metric_id"},
    back_populates="influenced_by",
)
Metric.influenced_by = Relationship(
    sa_relationship_kwargs={"remote_side": "Metric.metric_id"},
    back_populates="influences",
)


# Resolve forward references for other models
MetricExpression.update_forward_refs()
Expression.update_forward_refs()

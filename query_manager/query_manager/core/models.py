from typing import Literal, Union

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


class MetricComponent(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Model for metric components relationship
    """

    metric_id: str = Field(sa_column=Column(ForeignKey("metric.metric_id"), primary_key=True))
    component_id: str = Field(sa_column=Column(ForeignKey("metric.metric_id"), primary_key=True))


class Metric(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Metric model
    """

    metric_id: str = Field(max_length=255, index=True, primary_key=True, unique=True)
    label: str = Field(sa_type=Text)
    abbreviation: str = Field(sa_type=Text, nullable=True)
    definition: str = Field(sa_type=Text, nullable=True)
    unit_of_measure: str = Field(sa_type=Text, nullable=True)
    unit: str = Field(sa_type=Text, nullable=True)

    # Define self-referential relationships
    components: list["Metric"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Metric.metric_id == foreign(Metric.metric_id)"},
        back_populates="components",
    )
    terms: list = Field(default_factory=list, sa_type=JSONB)
    complexity: Complexity = Field(sa_column=Column(Enum(Complexity, inherit_schema=True)))
    metric_expression: MetricExpression = Field(sa_type=JSONB, nullable=True)
    output_of: list["Metric"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Metric.metric_id == foreign(Metric.metric_id)"},
        back_populates="output_of",
    )
    input_to: list = Field(default_factory=list, sa_type=JSONB)
    influences: list["Metric"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Metric.metric_id == foreign(Metric.metric_id)"},
        back_populates="influences",
    )
    influenced_by: list["Metric"] = Relationship(
        sa_relationship_kwargs={"primaryjoin": "Metric.metric_id == foreign(Metric.metric_id)"},
        back_populates="influenced_by",
    )

    periods: list[Granularity] | None = Field(default_factory=list, sa_type=JSONB)
    grain_aggregation: Granularity = Field(
        sa_column=Column(Enum(Granularity, name="grain_aggregation", inherit_schema=True))
    )

    aggregations: list = Field(default_factory=list, sa_type=JSONB)
    owned_by_team: list = Field(default_factory=list, sa_type=JSONB)
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

# Resolve forward references for other models
MetricExpression.update_forward_refs()
Expression.update_forward_refs()

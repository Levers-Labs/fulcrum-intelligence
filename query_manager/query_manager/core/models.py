from typing import (
    Any,
    Literal,
    Optional,
    Union,
)

from pydantic import TypeAdapter
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Float,
    String,
    Text,
    UniqueConstraint,
    event,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlmodel import Field, Relationship, SQLModel

from commons.db.models import BaseSQLModel, BaseTimeStampedTenantModel
from commons.models import BaseModel
from commons.models.enums import Granularity
from commons.models.slack import SlackChannel
from query_manager.core.enums import Complexity, SemanticMemberType


# Expression models
class MetricExpression(BaseModel):
    type: Literal["metric"] = "metric"
    metric_id: str
    coefficient: int | float = Field(1, description="Coefficient for the metric")
    period: int = Field(0, description="Period for the metric, 0 denotes the current period")
    expression_str: str | None = Field(None, description="Expression string for the metric")
    expression: Optional["Expression"] = Field(None, description="Expression for the metric")
    power: int | float = Field(1, description="Power for the metric")


class ConstantExpression(BaseModel):
    type: Literal["constant"] = "constant"
    value: int | float


class Expression(BaseModel):
    type: Literal["expression"] = "expression"
    operator: str
    operands: list[Union["MetricExpression", "Expression", "ConstantExpression"]]


# Metadata models
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


# Base DB model
class QuerySchemaBaseModel(BaseTimeStampedTenantModel):
    __table_args__ = {"schema": "query_store"}


# Association tables
class MetricDimension(SQLModel, table=True):
    """
    Association table for the many-to-many relationship between Metric and Dimensions.
    """

    __table_args__ = {"schema": "query_store"}
    metric_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)
    dimension_id: int = Field(foreign_key="query_store.dimension.id", primary_key=True)


class MetricInfluence(SQLModel, table=True):
    """Association table for metric influences"""

    __table_args__ = {"schema": "query_store"}
    influencer_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)
    influenced_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)


class MetricComponent(SQLModel, table=True):
    """Association table for metric components"""

    __table_args__ = {"schema": "query_store"}
    parent_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)
    component_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)


class MetricInput(SQLModel, table=True):
    """Association table for metric inputs"""

    __table_args__ = {"schema": "query_store"}
    metric_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)
    input_id: int = Field(foreign_key="query_store.metric.id", primary_key=True)


# Dimension models
class DimensionBase(BaseSQLModel):
    dimension_id: str = Field(sa_column=Column(String(255), index=True))
    label: str = Field(sa_column=Column(String(255)))
    reference: str = Field(sa_column=Column(String(255), nullable=True))
    definition: str = Field(sa_column=Column(Text, nullable=True))
    meta_data: DimensionMetadata = Field(sa_type=JSONB)


class Dimension(DimensionBase, QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Dimensions model
    """

    __table_args__ = (
        # Unique constraint for dimension_id and tenant_id
        UniqueConstraint("dimension_id", "tenant_id", name="uq_dimension_id_tenant_id"),  # type: ignore
        {"schema": "query_store"},
    )

    metrics: list["Metric"] = Relationship(back_populates="dimensions", link_model=MetricDimension)

    @classmethod
    def __declare_last__(cls):
        @event.listens_for(cls, "load", propagate=True)
        def receive_load(target: Dimension, context: Any):
            if isinstance(target.meta_data, dict):  # type: ignore
                target.meta_data = TypeAdapter(DimensionMetadata).validate_python(  # type: ignore[unreachable]
                    target.meta_data
                )


class MetricBase(BaseSQLModel):
    metric_id: str = Field(sa_column=Column(String(255), index=True))
    label: str = Field(sa_column=Column(String(255)))
    abbreviation: str | None = Field(sa_column=Column(String(255), nullable=True))
    definition: str | None = Field(sa_column=Column(Text, nullable=True))
    unit_of_measure: str | None = Field(sa_column=Column(String(255), nullable=True))
    unit: str | None = Field(sa_column=Column(String(255), nullable=True))
    terms: list[str] = Field(
        sa_column=Column(
            ARRAY(String),
            nullable=True,
        )
    )
    complexity: Complexity = Field(sa_column=Column(Enum(Complexity, name="metric_complexity", inherit_schema=True)))
    metric_expression: MetricExpression | None = Field(sa_type=JSONB, nullable=True)
    periods: list[Granularity] | None = Field(sa_column=Column(ARRAY(String), nullable=True, default=list))
    grain_aggregation: str = Field(sa_column=Column(String(255), default="sum"))
    aggregations: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True, default=list))
    owned_by_team: list[str] = Field(sa_column=Column(ARRAY(String), nullable=True, default=list))
    meta_data: MetricMetadata = Field(sa_type=JSONB, default_factory=dict)
    hypothetical_max: float | None = Field(sa_column=Column(Float, nullable=True), default=None)


class Metric(MetricBase, QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Metric model
    """

    __table_args__ = (
        # Unique constraint for metric_id and tenant_id
        UniqueConstraint("metric_id", "tenant_id", name="uq_metric_id_tenant_id"),  # type: ignore
        {"schema": "query_store"},
    )

    dimensions: list["Dimension"] = Relationship(back_populates="metrics", link_model=MetricDimension)

    influences: list["Metric"] = Relationship(
        back_populates="influencers",
        link_model=MetricInfluence,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricInfluence.influencer_id",
            "secondaryjoin": "Metric.id == MetricInfluence.influenced_id",
        },
    )
    influencers: list["Metric"] = Relationship(
        back_populates="influences",
        link_model=MetricInfluence,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricInfluence.influenced_id",
            "secondaryjoin": "Metric.id == MetricInfluence.influencer_id",
        },
    )

    components: list["Metric"] = Relationship(
        back_populates="parent_metrics",
        link_model=MetricComponent,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricComponent.parent_id",
            "secondaryjoin": "Metric.id == MetricComponent.component_id",
        },
    )
    parent_metrics: list["Metric"] = Relationship(
        back_populates="components",
        link_model=MetricComponent,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricComponent.component_id",
            "secondaryjoin": "Metric.id == MetricComponent.parent_id",
        },
    )

    inputs: list["Metric"] = Relationship(
        back_populates="outputs",
        link_model=MetricInput,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricInput.metric_id",
            "secondaryjoin": "Metric.id == MetricInput.input_id",
        },
    )
    outputs: list["Metric"] = Relationship(
        back_populates="inputs",
        link_model=MetricInput,
        sa_relationship_kwargs={
            "primaryjoin": "Metric.id == MetricInput.input_id",
            "secondaryjoin": "Metric.id == MetricInput.metric_id",
        },
    )

    def get_dimension(self, dimension_id: str) -> Dimension | None:
        return next((dimension for dimension in self.dimensions if dimension.dimension_id == dimension_id), None)

    @classmethod
    def __declare_last__(cls):
        @event.listens_for(cls, "load", propagate=True)
        def receive_load(target: Metric, context):
            if isinstance(target.meta_data, dict):  # type: ignore
                target.meta_data = TypeAdapter(MetricMetadata).validate_python(target.meta_data)  # type: ignore


class MetricNotifications(QuerySchemaBaseModel, table=True):
    """
    Represents metric notifications settings in the database.

    Attributes:
        metric_id (int): The foreign key referencing the metric.id in the query_store schema.
        slack_enabled (bool): Indicates if Slack notifications are enabled for the metric.
        slack_channels (dict): A dictionary containing Slack channel IDs and their corresponding names.
    """

    __table_args__ = (
        # Unique constraint for metric_id and tenant_id
        UniqueConstraint("metric_id", "tenant_id", name="uq_metric_id_tenant_id_notify"),  # type: ignore
        {"schema": "query_store"},
    )

    metric_id: int = Field(foreign_key="query_store.metric.id")
    slack_enabled: bool = Field(default=False, sa_column=Column(Boolean, default=False))
    slack_channels: list[SlackChannel] = Field(default_factory=dict, sa_type=JSONB)

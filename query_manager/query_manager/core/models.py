from sqlalchemy import Column, Enum, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from commons.db.models import BaseTimeStampedModel
from commons.models.enums import Granularity
from query_manager.core.enums import Complexity, Unit, UnitOfMeasure
from query_manager.core.schemas import DimensionMetadata, MetricExpression, MetricMetadata


class QuerySchemaBaseModel(BaseTimeStampedModel):
    __table_args__ = {"schema": "query_store"}


class Dimensions(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Dimensions model
    """

    dimension_id: str = Field(max_length=50, index=True)
    label: str = Field(sa_type=Text, max_length=255)
    reference: str = Field(sa_type=Text, max_length=50)
    definition: str = Field(sa_type=Text)
    meta_data: DimensionMetadata = Field(sa_type=JSONB)


class Metric(QuerySchemaBaseModel, table=True):  # type: ignore
    """
    Metric model
    """

    __table_args__ = {"schema": "query_store"}

    metric_id: str = Field(max_length=255, index=True)
    label: str = Field(sa_type=Text)
    abbreviation: str = Field(sa_type=Text)
    definition: str = Field(sa_type=Text)
    unit_of_measure: UnitOfMeasure = Field(sa_column=Column(Enum(UnitOfMeasure, inherit_schema=True)))
    unit: Unit = Field(sa_column=Column(Enum(Unit, inherit_schema=True)))
    components: list = Field(default_factory=list, sa_type=JSONB)
    terms: list = Field(default_factory=list, sa_type=JSONB)
    complexity: Complexity = Field(sa_column=Column(Enum(Complexity, inherit_schema=True)))
    metric_expression: MetricExpression = Field(sa_type=JSONB)
    output_of: list = Field(default_factory=list, sa_type=JSONB)
    input_to: list = Field(default_factory=list, sa_type=JSONB)
    influences: list = Field(default_factory=list, sa_type=JSONB)
    influenced_by: list = Field(default_factory=list, sa_type=JSONB)

    periods: list[Granularity] | None = Field(default_factory=list, sa_type=JSONB)
    grain_aggregation: Granularity = Field(
        sa_column=Column(Enum(Granularity, name="grain_aggregation", inherit_schema=True))
    )

    aggregations: list = Field(default_factory=list, sa_type=JSONB)
    owned_by_team: list = Field(default_factory=list, sa_type=JSONB)
    dimensions: list[Dimensions] = Field(default_factory=list, sa_type=JSONB)
    meta_data: MetricMetadata = Field(default_factory=dict, sa_type=JSONB)

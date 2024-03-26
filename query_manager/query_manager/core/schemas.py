from query_manager.utilities.schema import BaseModel


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
    metric_expression: str | None = None
    grain_aggregation: str | None = None


class MetricList(MetricBase):
    pass


class MetricDetail(MetricBase):
    output_of: str | None = None
    input_to: list[str] | None = None
    influences: list[str] | None = None
    influenced_by: list[str] | None = None
    periods: list[str] | None = None
    aggregations: list[str] | None = None
    owned_by_team: list[str] | None = None
    dimensions: list[Dimension] | None = None


class Target(BaseModel):
    metric_id: str
    target_date: str
    aim: str
    target_value: int
    target_upper_bound: int | None = None
    target_lower_bound: int | None = None
    yellow_buffer: int | None = None
    red_buffer: int | None = None

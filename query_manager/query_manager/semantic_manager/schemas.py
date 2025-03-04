"""
Pydantic schemas for semantic manager API requests and responses.
"""

from pydantic import ConfigDict

from commons.models import BaseModel
from query_manager.semantic_manager.models import MetricDimensionalTimeSeries, MetricTimeSeries


class MetricTimeSeriesResponse(BaseModel):
    """Response schema for a single metric's time series data."""

    results: list[MetricTimeSeries]

    model_config = ConfigDict(from_attributes=True)


class MetricDimensionalTimeSeriesResponse(BaseModel):
    """Response schema for dimensional time series data."""

    results: list[MetricDimensionalTimeSeries]

    model_config = ConfigDict(from_attributes=True)

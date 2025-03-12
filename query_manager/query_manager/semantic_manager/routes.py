"""
API routes for semantic data.

This module provides FastAPI routes for the semantic manager module.
"""

from datetime import date
from typing import Annotated

from fastapi import APIRouter, HTTPException
from fastapi.params import Query, Security
from starlette import status

from commons.auth.scopes import QUERY_MANAGER_ALL
from commons.models.enums import Granularity
from query_manager.core.dependencies import oauth2_auth
from query_manager.semantic_manager.dependencies import SemanticManagerDep
from query_manager.semantic_manager.schemas import MetricDimensionalTimeSeriesResponse, MetricTimeSeriesResponse

router = APIRouter(prefix="/semantic", tags=["semantic"])


@router.get(
    "/metrics/{metric_id}/time-series",
    response_model=MetricTimeSeriesResponse,
    summary="Get time series data for a metric",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_metric_time_series(
    semantic_manager: SemanticManagerDep,
    metric_id: str,
    grain: Granularity,
    start_date: date | None = None,
    end_date: date | None = None,
) -> MetricTimeSeriesResponse:
    """
    Get time series data for a specific metric with optional date range filtering.

    - **metric_id**: ID of the metric to retrieve
    - **grain**: Time granularity (DAILY, WEEKLY, MONTHLY, etc.)
    - **start_date**: Optional start date (inclusive)
    - **end_date**: Optional end date (inclusive)
    """
    results = await semantic_manager.get_metric_time_series(
        metric_id=metric_id,
        grain=grain,
        start_date=start_date,
        end_date=end_date,
    )
    return MetricTimeSeriesResponse(results=results)


@router.get(
    "/metrics/time-series",
    response_model=MetricTimeSeriesResponse,
    summary="Get time series data for multiple metrics",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_multi_metric_time_series(
    semantic_manager: SemanticManagerDep,
    metric_ids: Annotated[list[str], Query(description="List of metric IDs")],
    grain: Granularity,
    start_date: date | None = None,
    end_date: date | None = None,
) -> MetricTimeSeriesResponse:
    """
    Get time series data for multiple metrics with the same granularity.

    Query parameters:
    - **metric_ids**: Comma-separated list of metric IDs to retrieve
    - **grain**: Time granularity (DAILY, WEEKLY, MONTHLY, etc.)
    - **start_date**: Optional start date (inclusive)
    - **end_date**: Optional end date (inclusive)
    """
    if not metric_ids:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="At least one metric_id must be provided"
        )
    results = await semantic_manager.get_multi_metric_time_series(
        metric_ids=metric_ids,
        grain=grain,
        start_date=start_date,
        end_date=end_date,
    )
    return MetricTimeSeriesResponse(results=results)


@router.get(
    "/metrics/{metric_id}/dimensional-time-series",
    response_model=MetricDimensionalTimeSeriesResponse,
    summary="Get dimensional time series data for a metric",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_metric_dimensional_time_series(
    metric_id: str,
    semantic_manager: SemanticManagerDep,
    grain: Granularity,
    start_date: date | None = None,
    end_date: date | None = None,
    dimension_names: Annotated[list[str] | None, Query(description="List of dimension names to filter by")] = None,
) -> MetricDimensionalTimeSeriesResponse:
    """
    Get dimensional time series data for a specific metric.

    - **metric_id**: ID of the metric to retrieve
    - **grain**: Time granularity (DAILY, WEEKLY, MONTHLY, etc.)
    - **start_date**: Optional start date (inclusive)
    - **end_date**: Optional end date (inclusive)
    - **dimension_names**: Optional comma-separated list of dimension names to filter by
    """
    results = await semantic_manager.get_dimensional_time_series(
        metric_id=metric_id,
        grain=grain,
        start_date=start_date,
        end_date=end_date,
        dimension_names=dimension_names,
    )
    return MetricDimensionalTimeSeriesResponse(results=results)

"""
API routes for semantic data.

This module provides FastAPI routes for the semantic manager module.
"""

import logging
from datetime import date
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Response,
)
from fastapi.params import Query, Security
from starlette import status

from commons.auth.scopes import QUERY_MANAGER_ALL
from commons.models.enums import Granularity
from commons.utilities.pagination import Page, PaginationParams
from query_manager.core.dependencies import oauth2_auth
from query_manager.semantic_manager.dependencies import SemanticManagerDep
from query_manager.semantic_manager.filters import TargetFilter
from query_manager.semantic_manager.schemas import (
    MetricDimensionalTimeSeriesResponse,
    MetricTargetOverview,
    MetricTimeSeriesResponse,
    TargetBulkUpsertRequest,
    TargetBulkUpsertResponse,
    TargetCalculationRequest,
    TargetCalculationResponse,
    TargetResponse,
)
from query_manager.semantic_manager.utils.target_calculator import TargetCalculator

router = APIRouter(prefix="/semantic")
logger = logging.getLogger(__name__)


@router.get(
    "/metrics/{metric_id}/time-series",
    response_model=MetricTimeSeriesResponse,
    summary="Get time series data for a metric",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["semantic"],
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
    tags=["semantic"],
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
    tags=["semantic"],
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
    Get dimensional time series data for a specific metric with optional date range and dimension filtering.

    - **metric_id**: ID of the metric to retrieve
    - **grain**: Time granularity (DAILY, WEEKLY, MONTHLY, etc.)
    - **start_date**: Optional start date (inclusive)
    - **end_date**: Optional end date (inclusive)
    - **dimension_names**: Optional list of dimension names to filter by
    """
    results = await semantic_manager.get_dimensional_time_series(
        metric_id=metric_id,
        grain=grain,
        start_date=start_date,
        end_date=end_date,
        dimension_names=dimension_names,
    )
    return MetricDimensionalTimeSeriesResponse(results=results)


# Target Management Routes


@router.get(
    "/metrics/targets/overview",
    response_model=Page[MetricTargetOverview],
    summary="List all metrics with their target status",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
)
async def get_targets_overview(
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    semantic_manager: SemanticManagerDep,
    # metric_label: str | None = None,
) -> Page[MetricTargetOverview]:
    """
    Get targets for metrics with optional filtering.

    - **metric_id**: Optional metric ID to filter by
    """
    # targets_filter = TargetFilter(metric_label=metric_label)
    results, count = await semantic_manager.metric_target.get_metrics_targets_list()
    # filter_params=targets_filter.model_dump(exclude_unset=True))
    return Page.create(items=results, total_count=count, params=params)


@router.get(
    "/metrics/targets",
    response_model=Page[TargetResponse],
    summary="Get targets for metrics",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
)
async def get_targets(
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    semantic_manager: SemanticManagerDep,
    metric_ids: Annotated[list[str] | None, Query(description="List of metric IDs")] = None,
    grain: Granularity | None = None,
    target_date: date | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    metric_label: str | None = None,
) -> Page[TargetResponse]:
    """
    Get targets for metrics with optional filtering.

    - **metric_ids**: Optional list of metric IDs to filter by
    - **grain**: Optional time granularity to filter by (DAILY, WEEKLY, MONTHLY, etc.)
    - **start_date**: Optional start date (inclusive)
    - **end_date**: Optional end date (inclusive)

    Returns targets with additional growth metrics:
    - **growth_percentage**: Total growth percentage from first target to this one
    - **pop_growth_percentage**: Period-on-period growth percentage
    """
    targets_filter = TargetFilter(
        metric_ids=metric_ids,
        grain=grain,
        target_date=target_date,
        target_date_ge=start_date,
        target_date_le=end_date,
        metric_label=metric_label,
    )

    # Get database results
    db_results, count = await semantic_manager.metric_target.paginate(
        filter_params=targets_filter.model_dump(exclude_unset=True), params=params
    )

    # Convert to TargetResponse with growth percentages added
    target_responses = TargetCalculator.add_growth_stats_to_targets(db_results)

    return Page.create(items=target_responses, total_count=count, params=params)


@router.get(
    "/metrics/targets/{target_id}",
    response_model=TargetResponse,
    summary="Get a specific target",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
)
async def get_target(
    target_id: int,
    semantic_manager: SemanticManagerDep,
) -> TargetResponse:
    """
    Get a specific target by its primary key.

    - **target_id**: ID of the target
    """
    target = await semantic_manager.metric_target.get(target_id)
    return TargetResponse.model_validate(target)


@router.post(
    "/metrics/targets/bulk",
    response_model=TargetBulkUpsertResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Bulk upsert targets",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
)
async def bulk_upsert_targets(
    bulk_data: TargetBulkUpsertRequest,
    semantic_manager: SemanticManagerDep,
) -> TargetBulkUpsertResponse:
    """
    Bulk upsert targets for metrics.

    - **bulk_data**: List of targets to upsert
    """
    targets_data = [target.model_dump() for target in bulk_data.targets]

    # Upsert targets
    result = await semantic_manager.metric_target.bulk_upsert_targets(targets_data)

    return TargetBulkUpsertResponse(total=result["total"], processed=result["processed"], failed=result["failed"])


@router.delete(
    "/metrics/{metric_id}/targets",
    summary="Delete targets",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_targets(
    semantic_manager: SemanticManagerDep,
    metric_id: str,
    grain: Granularity,
    target_date: date | None = None,
    target_date_ge: date | None = None,
    target_date_le: date | None = None,
) -> None:
    """
    Delete targets with optional filtering.

    - **metric_id**: Optional metric ID to filter by
    - **grain**: Optional time granularity to filter by (DAILY, WEEKLY, MONTHLY, etc.)
    - **target_date**: Optional target date to filter by
    - **target_date_ge**: Optional start date to filter by
    - **target_date_le**: Optional end date to filter by
    """
    deleted_count = await semantic_manager.metric_target.delete_targets(
        metric_id=metric_id,
        grain=grain,
        target_date=target_date,
        target_date_ge=target_date_ge,
        target_date_le=target_date_le,
    )
    logger.info(
        "Deleted %d targets for metric %s with grain %s and date %s", deleted_count, metric_id, grain, target_date
    )


@router.post(
    "/metrics/targets/calculate",
    response_model=list[TargetCalculationResponse],
    summary="Calculate target values using different methods",
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
    tags=["targets"],
)
async def calculate_target_values(
    calculation_request: TargetCalculationRequest,
) -> list[TargetCalculationResponse]:
    """
    Calculate target values between dates using different calculation methods.

    - **current_value**: Current value on which calculations are based
    - **start_date**: Start date for the target period
    - **end_date**: End date for the target period
    - **grain**: Time granularity to use (DAILY, WEEKLY, MONTHLY, etc.)
    - **calculation_type**: Method used for calculation (value, growth, pop_growth)
    - **target_value**: Target value used for 'value' calculation type
    - **growth_percentage**: Growth percentage used for 'growth' calculation type
    - **pop_growth_percentage**: Period on period growth percentage for 'pop_growth' calculation type

    Returns calculated targets for each date with values for target value, growth %, and PoP growth %.
    """

    # Calculate targets using the utility function
    return TargetCalculator.calculate_targets(**calculation_request.model_dump(exclude_unset=True))

import logging
from datetime import date
from http.client import HTTPException
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Body,
    Depends,
    Query,
    Request,
)
from pydantic import ValidationError

from commons.models.enums import Granularity
from commons.utilities.pagination import PaginationParams
from query_manager.core.dependencies import ParquetServiceDep, QueryClientDep
from query_manager.core.enums import OutputFormat
from query_manager.core.models import Dimensions
from query_manager.core.schemas import (
    DimensionDetail,
    MetricDetail,
    MetricListResponse,
    MetricValuesResponse,
    TargetListResponse,
)
from query_manager.exceptions import MetricNotFoundError
from query_manager.services.s3 import NoSuchKeyError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="")


@router.get("/metrics", response_model=MetricListResponse, tags=["metrics"])
async def list_metrics(
    client: QueryClientDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    metric_ids: Annotated[
        list[str] | None,
        Query(description="List of metric ids"),
    ] = None,  # type: ignore
):
    """
    Retrieve a list of metrics.
    """
    results = await client.list_metrics(metric_ids=metric_ids, params=params)
    print(f"Results: {results}")
    # Validate the results against the MetricListResponse schema
    try:
        validated_results = MetricListResponse(results=results)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")

    return validated_results


@router.get("/metrics/{metric_id}", response_model=MetricDetail, tags=["metrics"])
async def get_metric(metric_id: str, client: QueryClientDep):
    """
    Retrieve a metric by ID.
    """
    return await client.get_metric_details(metric_id)


@router.get("/dimensions", response_model=list[Dimensions], tags=["dimensions"])
async def list_dimensions(
    client: QueryClientDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
):
    """
    Retrieve a list of dimensions.
    """
    return await client.list_dimensions(params=params)


@router.get("/dimensions/{dimension_id}", response_model=DimensionDetail, tags=["dimensions"])
async def get_dimension(dimension_id: str, client: QueryClientDep):
    """
    Retrieve a dimension by ID.
    """
    return await client.get_dimension_details(dimension_id)


@router.get("/dimensions/{dimension_id}/members", response_model=list[Any], tags=["dimensions"])
async def get_dimension_members(dimension_id: str, client: QueryClientDep):
    """
    Retrieve members of a dimension by ID.
    """
    return await client.get_dimension_members(dimension_id)


# Value APIs
@router.post("/metrics/{metric_id}/values", response_model=MetricValuesResponse, tags=["metrics"])
async def get_metric_values(
    request: Request,
    client: QueryClientDep,
    parquet_service: ParquetServiceDep,
    metric_id: str,
    start_date: Annotated[date, Body(description="The start date of the date range.")],
    end_date: Annotated[date, Body(description="The end date of the date range.")],
    grain: Annotated[Granularity | None, Body(description="The granularity of the data.")] = None,
    dimensions: Annotated[
        list[str] | None,
        Body(description="Can be either 'all' or list of dimension ids."),
    ] = None,
    output_format: Annotated[OutputFormat, Body(description="The desired output format.")] = OutputFormat.JSON,
):
    """
    Retrieve values for a metric within a date range.
    """
    # Accessing the request_id from the request's state
    request_id = request.state.request_id
    try:
        res = await client.get_metric_values(metric_id, start_date, end_date, grain=grain, dimensions=dimensions)
    except NoSuchKeyError as e:
        raise MetricNotFoundError(metric_id) from e

    if output_format == OutputFormat.PARQUET:
        parquet_url = await parquet_service.convert_and_upload(res, metric_id, request_id, folder="values")
        return {"url": parquet_url}
    return {"data": res}


@router.get("/metrics/{metric_id}/targets", response_model=TargetListResponse, tags=["metrics"])
async def get_metric_targets(
    request: Request,
    client: QueryClientDep,
    parquet_service: ParquetServiceDep,
    metric_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    grain: Granularity | None = None,
    output_format: Annotated[OutputFormat, Query(...)] = OutputFormat.JSON,
):
    """
    Retrieve targets for a metric within a date range.
    """
    request_id = request.state.request_id
    res = await client.get_metric_targets(metric_id, start_date=start_date, end_date=end_date, grain=grain)
    if output_format == OutputFormat.PARQUET:
        parquet_url = await parquet_service.convert_and_upload(res, metric_id, request_id, folder="targets")
        return {"url": parquet_url}

    return {"results": res}

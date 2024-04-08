import logging
from datetime import date
from typing import Annotated

from fastapi import (
    APIRouter,
    Body,
    Query,
    Request,
)

from query_manager.core.dependencies import ParquetServiceDep, QueryClientDep
from query_manager.core.schemas import (
    Dimension,
    DimensionDetail,
    MetricDetail,
    MetricListResponse,
    MetricValue,
    MetricValuesResponse,
    Target,
)
from query_manager.exceptions import MetricNotFoundError
from query_manager.services.s3 import NoSuchKeyError
from query_manager.utilities.enums import OutputFormat

logger = logging.getLogger(__name__)
router = APIRouter(prefix="")


@router.get("/metrics", response_model=MetricListResponse, tags=["metrics"])
async def list_metrics(
    client: QueryClientDep, metric_ids: Annotated[list[str] | None, Query(description="List of metric IDs")] = None
):
    """
    Retrieve a list of metrics.
    """
    results = await client.list_metrics(metric_ids=metric_ids)
    return {"results": results}


@router.get("/metrics/{metric_id}", response_model=MetricDetail, tags=["metrics"])
async def get_metric(metric_id: str, client: QueryClientDep):
    """
    Retrieve a metric by ID.
    """
    return await client.get_metric_details(metric_id)


@router.get("/dimensions", response_model=list[Dimension], tags=["dimensions"])
async def list_dimensions(client: QueryClientDep):
    """
    Retrieve a list of dimensions.
    """
    return await client.list_dimensions()


@router.get("/dimensions/{dimension_id}", response_model=DimensionDetail, tags=["dimensions"])
async def get_dimension(dimension_id: str, client: QueryClientDep):
    """
    Retrieve a dimension by ID.
    """
    return await client.get_dimension_details(dimension_id)


@router.get("/dimensions/{dimension_id}/members", response_model=list[str], tags=["dimensions"])
async def get_dimension_members(dimension_id: str, client: QueryClientDep):
    """
    Retrieve members of a dimension by ID.
    """
    return await client.get_dimension_members(dimension_id)


# Value APIs
@router.get("/metrics/{metric_id}/value", response_model=MetricValue, tags=["metrics"])
async def get_metric_value(
    client: QueryClientDep,
    metric_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
):
    """
    Retrieve the aggregated value of a metric within a date range.
    """
    try:
        res = await client.get_metric_value(metric_id, start_date, end_date)
    except NoSuchKeyError as e:
        raise MetricNotFoundError(metric_id) from e
    return res


@router.post("/metrics/{metric_id}/values", response_model=MetricValuesResponse, tags=["metrics"])
async def get_metric_values(
    request: Request,
    client: QueryClientDep,
    parquet_service: ParquetServiceDep,
    metric_id: str,
    start_date: Annotated[date, Body(description="The start date of the date range.")],
    end_date: Annotated[date, Body(description="The end date of the date range.")],
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
        res = await client.get_metric_values(metric_id, start_date, end_date, dimensions=dimensions)
    except NoSuchKeyError as e:
        raise MetricNotFoundError(metric_id) from e

    if output_format == OutputFormat.PARQUET:
        parquet_url = await parquet_service.convert_and_upload(res, metric_id, request_id)
        return {"url": parquet_url}
    return {"data": res}


@router.get("/metrics/{metric_id}/targets", response_model=list[Target], tags=["metrics"])
async def get_metric_targets(
    client: QueryClientDep,
    metric_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """
    Retrieve targets for a metric within a date range.
    """
    try:
        res = await client.get_metric_targets(metric_id, start_date=start_date, end_date=end_date)
    except NoSuchKeyError as e:
        raise MetricNotFoundError(metric_id) from e
    return res

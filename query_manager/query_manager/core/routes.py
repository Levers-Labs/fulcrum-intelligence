import logging

from fastapi import APIRouter

from query_manager.core.dependencies import QueryClientDep
from query_manager.core.schemas import (
    Dimension,
    DimensionDetail,
    MetricDetail,
    MetricList,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="")


@router.get("/metrics", response_model=list[MetricList], tags=["metrics"])
async def list_metrics(client: QueryClientDep):
    """
    Retrieve a list of metrics.
    """
    return await client.list_metrics()


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

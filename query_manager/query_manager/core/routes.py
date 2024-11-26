import logging
from datetime import date
from typing import Annotated, Any

from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    Security,
)
from sqlalchemy.exc import IntegrityError

from commons.auth.scopes import QUERY_MANAGER_ALL
from commons.models.enums import Granularity
from commons.models.tenant import CubeConnectionConfig
from commons.utilities.pagination import Page, PaginationParams
from query_manager.core.dependencies import ParquetServiceDep, QueryClientDep, oauth2_auth
from query_manager.core.enums import OutputFormat
from query_manager.core.models import Dimension, Metric
from query_manager.core.schemas import (
    DimensionCompact,
    DimensionCreate,
    DimensionDetail,
    DimensionUpdate,
    MetricCreate,
    MetricDetail,
    MetricList,
    MetricUpdate,
    MetricValuesResponse,
    TargetListResponse,
)
from query_manager.exceptions import DimensionNotFoundError, MetricNotFoundError
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.s3 import NoSuchKeyError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="")


@router.get(
    "/metrics",
    response_model=Page[MetricList],
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def list_metrics(
    client: QueryClientDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    metric_label: str = None,  # type: ignore
    metric_ids: Annotated[
        list[str],
        Query(description="List of metric ids"),
    ] = None,  # type: ignore
):
    """
    Retrieve a list of metrics.
    """
    results, count = await client.list_metrics(metric_ids=metric_ids, metric_label=metric_label, params=params)
    return Page[Metric].create(items=results, total_count=count, params=params)


@router.get(
    "/metrics/{metric_id}",
    response_model=MetricDetail,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_metric(metric_id: str, client: QueryClientDep):
    """
    Retrieve a metric by ID.
    """
    return await client.get_metric_details(metric_id)


@router.post(
    "/metrics",
    response_model=MetricDetail,
    tags=["metrics"],
    status_code=201,
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def create_metric(
    metric_data: MetricCreate,
    client: QueryClientDep,
):
    """
    Create a new metric.
    """
    try:
        created_metric = await client.create_metric(metric_data)
        return created_metric
    except IntegrityError as e:
        raise HTTPException(
            status_code=422,
            detail={
                "loc": ["body", "metric_id"],
                "msg": f"Metric with id '{metric_data.metric_id}' already exists.",
                "type": "already_exists",
            },
        ) from e


@router.patch(
    "/metrics/{metric_id}",
    response_model=MetricDetail,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def update_metric(
    metric_id: str,
    metric_data: MetricUpdate,
    client: QueryClientDep,
):
    """
    Update a metric by ID.
    """
    try:
        updated_metric = await client.update_metric(metric_id, metric_data)
        return updated_metric
    except MetricNotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Metric with ID {metric_id} not found") from e


@router.get(
    "/dimensions",
    response_model=Page[DimensionCompact],
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def list_dimensions(
    client: QueryClientDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
):
    """
    Retrieve a list of dimensions.
    """
    results, count = await client.list_dimensions(params=params)
    return Page[Dimension].create(items=results, total_count=count, params=params)


@router.post(
    "/dimensions",
    response_model=DimensionDetail,
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def create_dimension(dimension: DimensionCreate, client: QueryClientDep):
    """
    Create a new dimension.
    """
    try:
        return await client.create_dimension(dimension)
    except IntegrityError as e:
        raise HTTPException(
            status_code=422,
            detail={
                "loc": ["body", "dimension_id"],
                "msg": f"Dimension with id '{dimension.dimension_id}' already exists.",
                "type": "already_exists",
            },
        ) from e


@router.get(
    "/dimensions/{dimension_id}",
    response_model=DimensionDetail,
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_dimension(dimension_id: str, client: QueryClientDep):
    """
    Retrieve a dimension by ID.
    """
    return await client.get_dimension_details(dimension_id)


@router.put(
    "/dimensions/{dimension_id}",
    response_model=DimensionDetail,
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def update_dimension(dimension_id: str, dimension: DimensionUpdate, client: QueryClientDep):
    """
    Update an existing dimension.
    """
    try:
        return await client.update_dimension(dimension_id, dimension)
    except DimensionNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=f"Dimension with id '{dimension_id}' not found.",
        ) from e


@router.get(
    "/dimensions/{dimension_id}/members",
    response_model=list[Any],
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_dimension_members(dimension_id: str, client: QueryClientDep):
    """
    Retrieve members of a dimension by ID.
    """
    return await client.get_dimension_members(dimension_id)


# Value APIs
@router.post(
    "/metrics/{metric_id}/values",
    response_model=MetricValuesResponse,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
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
    except MetricNotFoundError as MetricErr:
        raise MetricNotFoundError(metric_id) from MetricErr

    if output_format == OutputFormat.PARQUET:
        parquet_url = await parquet_service.convert_and_upload(res, metric_id, request_id, folder="values")
        return {"url": parquet_url}
    return {"data": res}


@router.get(
    "/metrics/{metric_id}/targets",
    response_model=TargetListResponse,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
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


@router.post(
    "/connection/cube/verify",
    tags=["cube"],
    dependencies=[Security(oauth2_auth().verify, scopes=[])],
)
async def verify_cube_connection(config: CubeConnectionConfig):
    """
    This endpoint is used to verify the connection to the Cube API using the provided client ID and secret key.
    """
    try:
        auth_options = (
            {"secret_key": config.cube_auth_secret_key}
            if config.cube_auth_type == CubeJWTAuthType.SECRET_KEY
            else {"token": config.cube_auth_token}
        )

        # Create a new CubeClient instance with the provided API URL and authentication options
        cube_client = CubeClient(
            base_url=config.cube_api_url,
            auth_type=config.cube_auth_type,  # type: ignore
            auth_options=auth_options,  # type: ignore
        )

        # Attempt to load a simple query or check connection to verify the credentials
        await cube_client.load_query_data({"dimensions": ["metric_targets.grain"]})

        # If the connection is successful, return a message indicating the successful connection
        return {"message": "Connection successful"}

    except Exception as e:
        # If an exception is raised during the connection attempt, log the error and raise an HTTPException
        logger.error("Connection failed: %s", str(e))
        raise HTTPException(status_code=400, detail="Invalid credentials") from e

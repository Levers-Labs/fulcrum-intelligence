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
from commons.clients.base import HttpClientError
from commons.db.crud import NotFoundError
from commons.llm.exceptions import LLMError
from commons.models.enums import Granularity
from commons.models.tenant import CubeConnectionConfig
from commons.utilities.pagination import Page, PaginationParams
from query_manager.core.dependencies import (
    CRUDMetricCacheConfigDep,
    CRUDMetricCacheGrainConfigDep,
    ExpressionParserServiceDep,
    ParquetServiceDep,
    QueryClientDep,
    oauth2_auth,
)
from query_manager.core.enums import OutputFormat
from query_manager.core.filters import MetricCacheConfigFilter
from query_manager.core.schemas import (
    BulkGrainConfigUpdate,
    BulkMetricCacheUpdate,
    Cube,
    DeleteResponse,
    Dimension,
    DimensionCompact,
    DimensionCreate,
    DimensionDetail,
    DimensionUpdate,
    ExpressionParseRequest,
    Metric,
    MetricCacheConfigRead,
    MetricCacheConfigUpdate,
    MetricCacheGrainConfigRead,
    MetricCacheGrainConfigUpdate,
    MetricCreate,
    MetricDetail,
    MetricList,
    MetricUpdate,
    MetricValuesResponse,
    TargetListResponse,
)
from query_manager.exceptions import (
    DimensionNotFoundError,
    ErrorCode,
    MetricNotFoundError,
    QueryManagerError,
)
from query_manager.llm.prompts import ParsedExpressionOutput
from query_manager.services.cube import CubeClient, CubeJWTAuthType
from query_manager.services.s3 import NoSuchKeyError
from query_manager.utils.metric_builder import MetricDataBuilder

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
        return await client.get_metric_details(created_metric.metric_id)
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
    dimension_ids: Annotated[
        list[str],
        Query(description="List of dimension ids"),
    ] = None,  # type: ignore
    dimension_label: str | None = None,
):
    """
    Retrieve a list of dimensions.
    """
    results, count = await client.list_dimensions(
        params=params, dimension_ids=dimension_ids, dimension_label=dimension_label
    )
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
    except (NoSuchKeyError, MetricNotFoundError) as e:
        # If the metric is not found, raise a MetricNotFoundError
        raise MetricNotFoundError(metric_id) from e

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


@router.post(
    "/metrics/{metric_id}/expression/parse",
    response_model=ParsedExpressionOutput,
    tags=["expression"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def parse_expression(
    metric_id: str,
    request: ExpressionParseRequest,
    expression_parser_service: ExpressionParserServiceDep,
):
    """
    Parse a string expression and return the parsed JSON.
    """
    logger.info("Parsing expression for metric %s: %s", metric_id, request.expression)
    try:
        return await expression_parser_service.process(request.expression)
    except LLMError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get(
    "/meta/cubes",
    response_model=list[Cube],
    tags=["cube"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def list_cubes(
    client: QueryClientDep,
    cube_name: str | None = None,
):
    """
    List all available cubes.

    Args:
        client: QueryClient dependency
        cube_name: Optional filter to get a specific cube by name
    """
    try:
        cubes = await client.list_cubes(cube_name=cube_name)
        if cube_name:
            cubes = [cube for cube in cubes if cube["name"] == cube_name or cube["title"] == cube_name]
        return cubes
    except HttpClientError as exc:
        logger.error("Failed to fetch cubes from Cube API: %s", exc)
        raise QueryManagerError(500, ErrorCode.MISSING_CONFIGURATION, "Failed to fetch cubes from Cube API") from exc


@router.post(
    "/metrics/preview",
    response_model=MetricCreate,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def preview_metric_from_yaml(
    client: QueryClientDep,  # Dependency for QueryClient
    expression_parser_service: ExpressionParserServiceDep,  # Dependency for ExpressionParserService
    metric_data: str = Body(
        default="""
                        metric_id: test
                        label: test
                        abbreviation: test
                        hypothetical_max: 100
                        definition: test is a metric
                        expression: null
                        aggregation: sum
                        unit_of_measure: quantity
                        unit: n
                        measure: cube.test
                        time_dimension: cube.test""",
        description="Raw Metric Data in YAML format",
        media_type="application/x-yaml",
    ),
):
    """
    Preview a metric from YAML data.
    """
    # Call MetricDataBuilder to construct the metric data structure from the provided YAML content
    return await MetricDataBuilder.build_metric_data(
        metric_data=metric_data,  # The YAML formatted metric data
        client=client,  # The QueryClient dependency
        expression_parser_service=expression_parser_service,  # The ExpressionParserService dependency
    )


@router.delete(
    "/metrics/bulk",
    status_code=200,
    tags=["metrics"],
    response_model=DeleteResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def delete_metrics_bulk(
    metric_ids: Annotated[list[str], Body(description="List of metric IDs to delete")],
    client: QueryClientDep,
):
    """
    Delete multiple metrics and their relationships in bulk.
    """
    failed_deletions = []
    successful_deletions = []

    for metric_id in metric_ids:
        try:
            await client.delete_metric(metric_id)
            successful_deletions.append(metric_id)
        except NotFoundError:
            failed_deletions.append(metric_id)

    if failed_deletions and not successful_deletions:
        # If all deletions failed
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["body", "metric_ids"],
                "msg": f"None of the metrics were found: {failed_deletions}",
                "type": "not_found",
            },
        )

    return DeleteResponse(
        message=(
            f"Successfully deleted {len(successful_deletions)} metrics. "
            + (f"Failed to delete {len(failed_deletions)} metrics: {failed_deletions}" if failed_deletions else "")
        ).strip()
    )


@router.delete(
    "/metrics/{metric_id}",
    status_code=200,
    tags=["metrics"],
    response_model=DeleteResponse,
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def delete_metric(
    metric_id: str,
    client: QueryClientDep,
):
    """
    Delete a metric and its relationships.
    """
    try:
        await client.delete_metric(metric_id)
        return DeleteResponse(message=f"Metric '{metric_id}' and all its relationships have been successfully deleted.")
    except NotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["path", "metric_id"],
                "msg": f"Metric with id '{metric_id}' not found.",
                "type": "not_found",
            },
        ) from e


@router.delete(
    "/dimensions/{dimension_id}",
    status_code=200,
    tags=["dimensions"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def delete_dimension(
    dimension_id: str,
    client: QueryClientDep,
):
    """
    Delete a dimension and its relationships.
    """
    try:
        await client.delete_dimension(dimension_id)
        return None
    except NotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail={
                "loc": ["path", "dimension_id"],
                "msg": f"Dimension with id '{dimension_id}' not found.",
                "type": "not_found",
            },
        ) from e


# Metric Cache Configuration Routes
@router.get(
    "/grains/cache-config",
    response_model=Page[MetricCacheGrainConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def list_grain_configs(
    grain_crud: CRUDMetricCacheGrainConfigDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
):
    """
    Get all grain configurations for metric caching.
    """
    results, count = await grain_crud.paginate(params, {})
    # Convert model objects to response schemas
    response_items = [MetricCacheGrainConfigRead.model_validate(result) for result in results]
    return Page[MetricCacheGrainConfigRead].create(items=response_items, total_count=count, params=params)


@router.get(
    "/grains/{grain}/cache-config",
    response_model=MetricCacheGrainConfigRead,
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def get_grain_config(
    grain: Granularity,
    grain_crud: CRUDMetricCacheGrainConfigDep,
):
    """
    Get configuration for a specific grain level.
    """
    try:
        config = await grain_crud.get_by_grain(grain)
        return MetricCacheGrainConfigRead.model_validate(config)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Grain configuration for '{grain}' not found") from e


@router.put(
    "/grains/{grain}/cache-config",
    response_model=MetricCacheGrainConfigRead,
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def update_grain_config(
    grain: Granularity,
    config_update: MetricCacheGrainConfigUpdate,
    grain_crud: CRUDMetricCacheGrainConfigDep,
):
    """
    Update configuration for a specific grain level.
    """
    try:
        update_data = config_update.model_dump(exclude_unset=True)
        updated_config = await grain_crud.update_grain_config(grain, update_data)
        return MetricCacheGrainConfigRead.model_validate(updated_config)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Grain configuration for '{grain}' not found") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update grain configuration: {str(e)}") from e


@router.post(
    "/grains/cache-config/bulk",
    response_model=list[MetricCacheGrainConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def bulk_update_grain_configs(
    bulk_update: BulkGrainConfigUpdate,
    grain_crud: CRUDMetricCacheGrainConfigDep,
):
    """
    Bulk update multiple grain configurations.
    """
    try:
        updated_configs = await grain_crud.bulk_update_grain_configs(bulk_update.configs)
        # Convert model objects to response schemas
        return [MetricCacheGrainConfigRead.model_validate(config) for config in updated_configs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to bulk update grain configurations: {str(e)}") from e


@router.post(
    "/grains/cache-config/enable-all",
    response_model=list[MetricCacheGrainConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def enable_all_grain_caching(
    grain_crud: CRUDMetricCacheGrainConfigDep,
):
    """
    Enable caching for all grain levels with default configurations.
    Creates default configurations if they don't exist.
    """
    try:
        created_configs = await grain_crud.create_default_grain_configs()
        return [MetricCacheGrainConfigRead.model_validate(config) for config in created_configs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enable caching for all grains: {str(e)}") from e


@router.get(
    "/metrics/{metric_id}/cache-config",
    response_model=MetricCacheConfigRead,
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def get_metric_cache_config(
    metric_id: str,
    cache_crud: CRUDMetricCacheConfigDep,
):
    """
    Get cache configuration for a specific metric.
    """
    try:
        config = await cache_crud.get_by_metric_id(metric_id)
        return MetricCacheConfigRead.model_validate(config)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=f"Cache configuration for metric {metric_id} not found") from e


@router.put(
    "/metrics/{metric_id}/cache-config",
    response_model=MetricCacheConfigRead,
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def update_metric_cache_config(
    metric_id: str,
    config_update: MetricCacheConfigUpdate,
    cache_crud: CRUDMetricCacheConfigDep,
):
    """
    Update cache configuration for a specific metric.
    """
    try:
        updated_config = await cache_crud.create_or_update_metric_config(metric_id, config_update.is_enabled)
        return MetricCacheConfigRead.model_validate(updated_config)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update metric cache configuration: {str(e)}") from e


@router.get(
    "/metrics/cache-config/all",
    response_model=Page[MetricCacheConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def list_metric_cache_configs(
    cache_crud: CRUDMetricCacheConfigDep,
    params: Annotated[PaginationParams, Depends(PaginationParams)],
    metric_ids: Annotated[list[str] | None, Query(description="List of metric ids")] = None,  # type: ignore
    is_enabled: Annotated[bool | None, Query(description="Filter to only enabled metrics")] = None,  # type: ignore
):
    """
    Get cache configurations for all metrics.
    """
    filter_obj = MetricCacheConfigFilter(metric_ids=metric_ids, is_enabled=is_enabled)
    results, count = await cache_crud.paginate(params, filter_obj.model_dump(exclude_unset=True))
    response_items = [MetricCacheConfigRead.model_validate(result) for result in results]
    return Page[MetricCacheConfigRead].create(items=response_items, total_count=count, params=params)


@router.post(
    "/metrics/cache-config/bulk",
    response_model=list[MetricCacheConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def bulk_update_metric_cache_configs(
    bulk_update: BulkMetricCacheUpdate,
    cache_crud: CRUDMetricCacheConfigDep,
):
    """
    Bulk update cache configurations for multiple metrics.
    """
    try:
        updated_configs = await cache_crud.bulk_update_metric_configs(bulk_update.metric_ids, bulk_update.is_enabled)
        return [MetricCacheConfigRead.model_validate(config) for config in updated_configs]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to bulk update metric cache configurations: {str(e)}"
        ) from e


@router.post(
    "/metrics/cache-config/enable-all",
    response_model=list[MetricCacheConfigRead],
    tags=["metric-cache"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],  # type: ignore
)
async def enable_all_metric_caching(
    cache_crud: CRUDMetricCacheConfigDep,
):
    """
    Enable caching for all metrics in the tenant.
    """
    try:
        updated_configs = await cache_crud.enable_all_metrics()
        return [MetricCacheConfigRead.model_validate(config) for config in updated_configs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enable caching for all metrics: {str(e)}") from e

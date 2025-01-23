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
from slack_sdk.errors import SlackApiError
from sqlalchemy.exc import IntegrityError

from commons.auth.scopes import QUERY_MANAGER_ALL
from commons.clients.base import HttpClientError
from commons.db.crud import NotFoundError
from commons.llm.exceptions import LLMError
from commons.models.enums import Granularity
from commons.models.tenant import CubeConnectionConfig
from commons.utilities.pagination import Page, PaginationParams
from query_manager.core.dependencies import (
    CRUDMetricNotificationsDep,
    ExpressionParserServiceDep,
    InsightBackendClientDep,
    ParquetServiceDep,
    QueryClientDep,
    oauth2_auth,
)
from query_manager.core.enums import OutputFormat
from query_manager.core.schemas import (  # SlackChannelIds,; SlackChannelsResponse,
    Cube,
    DeleteResponse,
    Dimension,
    DimensionCompact,
    DimensionCreate,
    DimensionDetail,
    DimensionUpdate,
    ExpressionParseRequest,
    Metric,
    MetricCreate,
    MetricDetail,
    MetricList,
    MetricSlackNotificationRequest,
    MetricSlackNotificationResponse,
    MetricUpdate,
    MetricValuesResponse,
    TargetListResponse,
)
from query_manager.exceptions import (
    DimensionNotFoundError,
    ErrorCode,
    MetricNotFoundError,
    MetricNotificationNotFoundError,
    QueryManagerError,
)
from query_manager.llm.prompts import ParsedExpressionOutput
from query_manager.services.cube import CubeClient
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
    slack_enabled: bool = None,  # type: ignore
):
    """
    Retrieve a list of metrics.
    """
    results, count = await client.list_metrics(
        metric_ids=metric_ids, metric_label=metric_label, slack_enabled=slack_enabled, params=params
    )
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
        auth_options = {"secret_key": config.cube_auth_secret_key}

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
    "/metrics/{metric_id}/notifications/slack",
    response_model=MetricSlackNotificationResponse,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def create_metric_slack_notifications(
    client: QueryClientDep,
    metric_id: str,
    request: MetricSlackNotificationRequest,
    notification_crud: CRUDMetricNotificationsDep,
    insights_client: InsightBackendClientDep,
):
    """
    This endpoint is used to create Slack notifications for a specific metric.
    """
    try:
        # Attempt to get the details of the metric
        metric = await client.get_metric_details(metric_id)
    except (NoSuchKeyError, MetricNotFoundError) as e:
        # If the metric is not found, raise a MetricNotFoundError
        raise MetricNotFoundError(metric_id) from e

    # Validate that channel_ids is not empty if slack_enabled is true
    if request.slack_enabled and not request.channel_ids:
        # If channel_ids is empty and slack_enabled is true, raise an HTTPException
        raise HTTPException(status_code=422, detail="Channel Ids cannot be blank.")

    # Initialize an empty list to store channel details
    channels = []
    for channel_id in request.channel_ids:
        try:
            # Attempt to get the channel name for each channel ID
            channel_details = await insights_client.get_slack_channel_details(channel_id)
        except (SlackApiError, HttpClientError) as Err:
            raise HTTPException(status_code=404, detail=f"Channel not found for {channel_id}") from Err

        # Append the channel details to the channels list
        channels.append(channel_details)

    # Create the Slack notifications using the notification_crud dependency
    return await notification_crud.create_metric_notifications(
        metric_id=metric.id, slack_enabled=request.slack_enabled, slack_channels=channels  # type: ignore
    )


@router.get(
    "/metrics/{metric_id}/notifications/slack",
    response_model=MetricSlackNotificationResponse,
    tags=["metrics"],
    dependencies=[Security(oauth2_auth().verify, scopes=[QUERY_MANAGER_ALL])],
)
async def get_metric_slack_notifications(
    client: QueryClientDep,
    metric_id: str,
    notification_crud: CRUDMetricNotificationsDep,
):
    """
    This endpoint is used to get the Slack notifications for a specific metric.
    """
    try:
        # Attempt to get the details of the metric
        metric = await client.get_metric_details(metric_id)
    except (NoSuchKeyError, MetricNotFoundError) as e:
        # If the metric is not found, raise a MetricNotFoundError
        raise MetricNotFoundError(metric_id) from e

    # Attempt to get the Slack notifications for the metric using the notification_crud dependency
    res = await notification_crud.get_metric_notifications(
        metric_id=metric.id,  # type: ignore
    )

    # If no notifications are found, raise a MetricNotificationNotFoundError
    if not res:
        raise MetricNotificationNotFoundError(metric_id)

    # Return the Slack notifications
    return res


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

from datetime import date
from typing import Any

from commons.db.crud import NotFoundError
from commons.models.enums import Granularity
from commons.utilities.pagination import PaginationParams
from query_manager.core.crud import CRUDDimensions, CRUDMetric
from query_manager.core.models import Dimension, Metric
from query_manager.core.schemas import MetricCreate, MetricUpdate
from query_manager.exceptions import DimensionNotFoundError, MetricNotFoundError
from query_manager.services.cube import CubeClient


class QueryClient:
    """
    Query client to interact with semantic & ontology services
    This will return metadata, values and graph data
    """

    def __init__(self, cube_client: CubeClient, dimensions_crud: CRUDDimensions, metric_crud: CRUDMetric):
        self.cube_client = cube_client
        self.dimensions_crud = dimensions_crud
        self.metric_crud = metric_crud
        self.dimension_model = Dimension
        self.metric_model = Metric

    async def list_metrics(
        self, *, params: PaginationParams, metric_ids: list[str] | None = None
    ) -> tuple[list[Metric], int]:
        """
        Fetches a list of all metrics with their associated dimensions and influences, optionally in a paginated manner.

        :param params: Optional number of items per page for pagination.
        :param metric_ids: Optional list of metric IDs to filter the results by.
        :return: A list of Metric objects with their dimensions and influences.
        """
        filter_params = dict(metric_ids=metric_ids) if metric_ids else dict()
        results, count = await self.metric_crud.paginate(
            params,
            filter_params=filter_params,
        )
        return results, count

    async def get_metric_details(self, metric_id: str) -> Metric:
        """
        Fetches detailed information for a specific metric by its ID.

        :param metric_id: The ID of the metric to fetch details for.
        :return: A dictionary representing the metric details, or None if not found.
        """
        try:
            instance = await self.metric_crud.get_by_metric_id(metric_id)
        except NotFoundError as e:
            raise MetricNotFoundError(metric_id) from e
        return instance

    async def list_dimensions(self, *, params: PaginationParams) -> tuple[list[Dimension], int]:
        """
        Fetches a list of all dimensions with their associated metrics, optionally in a paginated manner.
        """
        results, count = await self.dimensions_crud.paginate(params, filter_params=dict())
        return results, count

    async def get_dimension_details(self, dimension_id: str) -> Dimension | None:
        """
        Fetches detailed information for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch details for.
        :return: A dictionary representing the dimension details, or None if not found.
        """
        try:
            instance = await self.dimensions_crud.get_by_dimension_id(dimension_id)
        except NotFoundError as e:
            raise DimensionNotFoundError(dimension_id) from e
        return instance

    async def get_dimension_members(self, dimension_id: str) -> list[Any]:
        """
        Fetches members for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch members for.
        :return: A list of members for the dimension, or an empty list if not found.
        """
        dimension = await self.get_dimension_details(dimension_id)
        if not dimension:
            return []
        return await self.cube_client.load_dimension_members_from_cube(dimension)

    # Value apis
    async def get_metric_values(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
        grain: Granularity | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetches metric values for a given metric ID within a specified time range and optional dimensions.
        Will return time series data if grain is specified.
        The start_date is inclusive and the end_date is exclusive.
        The start_date and end_date will be used to filter the data.
        If grain is not provided, the data will be aggregated.
        If dimensions are not provided, a single metric value will be returned.
        :param metric_id: The ID of the metric to fetch values for.
        :param grain: The granularity of the time series data, represented as a Grain enum.
        :param start_date: The start date of the period to fetch values for (inclusive).
        :param end_date: The end date of the period to fetch values for (exclusive).
        :param dimensions: Optional, either 'all' or list of dimension names to include in the results.
        :return: A list of dictionaries representing the metric values.
        """
        metric = await self.get_metric_details(metric_id)
        if not metric:
            raise MetricNotFoundError(metric_id)

        # filter out valid dimensions
        valid_dimensions = []
        if dimensions and metric.dimensions:
            valid_dimensions = [
                dimension.dimension_id for dimension in metric.dimensions if dimension.dimension_id in dimensions
            ]
        res = await self.cube_client.load_metric_values_from_cube(metric, grain, start_date, end_date, valid_dimensions)
        return res

    async def get_metric_targets(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
        grain: Granularity | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetches target values for a given metric ID within an optional time range and grain.

        :param metric_id: The ID of the metric to fetch targets for.
        :param start_date: The start date of the period to fetch targets for (inclusive).
        :param end_date: The end date of the period to fetch targets for (exclusive).
        :param grain: The granularity of the target values, represented as a Grain enum.
        :return: A list of dictionaries representing the target values.
        """
        metric = await self.get_metric_details(metric_id)
        if not metric:
            raise MetricNotFoundError(metric_id)
        return await self.cube_client.load_metric_targets_from_cube(
            metric, grain=grain, start_date=start_date, end_date=end_date
        )

    async def update_metric(self, metric_id: str, metric_data: MetricUpdate) -> Metric:
        """
        Updates a metric with the given ID using the provided data.
        """
        metric = await self.metric_crud.get_by_metric_id(metric_id)
        if not metric:
            raise MetricNotFoundError(metric_id)

        updated_metric = await MetricUpdate.update(
            self.metric_crud.session, metric, metric_data.model_dump(exclude_unset=True)
        )
        return updated_metric

    async def create_metric(self, metric_data: MetricCreate) -> Metric:
        """
        Creates a new metric with the given data.
        """
        metric = await MetricCreate.create(self.metric_crud.session, metric_data.model_dump())
        return metric

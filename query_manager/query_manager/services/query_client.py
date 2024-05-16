import json
from datetime import date
from typing import Any

import aiofiles

from commons.models.enums import Granularity
from query_manager.config import get_settings
from query_manager.core.schemas import MetricDetail
from query_manager.exceptions import MetricNotFoundError
from query_manager.services.cube import CubeClient


class QueryClient:
    """
    Query client to interact with semantic & ontology services
    This will return metadata, values and graph data
    """

    METRICS_FILE_PATH = "data/metrics.json"
    DIMENSIONS_FILE_PATH = "data/dimensions.json"

    def __init__(self, cube_client: CubeClient):
        settings = get_settings()
        self.cube_client = cube_client
        self.metric_file_path = str(settings.PATHS.BASE_DIR.joinpath(self.METRICS_FILE_PATH))
        self.dimension_file_path = str(settings.PATHS.BASE_DIR.joinpath(self.DIMENSIONS_FILE_PATH))

    @staticmethod
    async def load_data(file_path: str) -> list[dict[str, Any]]:
        """
        Loads data from a specified JSON file.

        :param file_path: Path to the JSON file to be loaded.
        :return: A list of dictionaries representing the data.
        """

        async with aiofiles.open(file_path, "r") as f:
            contents = await f.read()
        return json.loads(contents)

    async def list_metrics(
        self, page: int | None = None, per_page: int | None = None, metric_ids: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetches a list of all metrics, optionally in a paginated manner.

        :param page: Optional page number for pagination.
        :param per_page: Optional number of items per page for pagination.
        :param metric_ids: Optional list of metric IDs to filter the results by.
        :return: A list of dictionaries representing metrics.
        """
        metrics_data = await self.load_data(self.metric_file_path)
        # Implement pagination logic here if necessary,
        # Implement filtering logic here
        if metric_ids:
            metrics_data = [metric for metric in metrics_data if metric["id"] in metric_ids]
        return metrics_data

    async def get_metric_details(self, metric_id: str) -> dict[str, Any] | None:
        """
        Fetches detailed information for a specific metric by its ID.

        :param metric_id: The ID of the metric to fetch details for.
        :return: A dictionary representing the metric details, or None if not found.
        """
        metrics_data = await self.load_data(self.metric_file_path)
        res = next((metric for metric in metrics_data if metric["id"] == metric_id), None)
        if res:
            return res
        raise MetricNotFoundError(metric_id)

    async def list_dimensions(self) -> list[dict[str, Any]]:
        """
        Fetches a list of all dimensions.

        :return: A list of dictionaries representing dimensions.
        """
        dimensions_data = await self.load_data(self.dimension_file_path)
        return dimensions_data

    async def get_dimension_details(self, dimension_id: str) -> dict[str, Any] | None:
        """
        Fetches detailed information for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch details for.
        :return: A dictionary representing the dimension details, or None if not found.
        """
        dimensions_data = await self.load_data(self.dimension_file_path)
        return next((dimension for dimension in dimensions_data if dimension["id"] == dimension_id), None)

    async def get_dimension_members(self, dimension_id: str) -> list[str]:
        """
        Fetches members for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch members for.
        :return: A list of members for the dimension, or an empty list if not found.
        """
        dimension_detail = await self.get_dimension_details(dimension_id)
        return dimension_detail.get("members", []) if dimension_detail else []

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
        metric_dict = await self.get_metric_details(metric_id)
        if not metric_dict:
            raise MetricNotFoundError(metric_id)

        metric = MetricDetail.parse_obj(metric_dict)
        # filter out valid dimensions
        valid_dimensions = []
        if dimensions and metric.dimensions:
            valid_dimensions = [dimension.id for dimension in metric.dimensions if dimension.id in dimensions]
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
        metric_dict = await self.get_metric_details(metric_id)
        if not metric_dict:
            raise MetricNotFoundError(metric_id)
        metric = MetricDetail.parse_obj(metric_dict)
        return await self.cube_client.load_metric_targets_from_cube(
            metric, grain=grain, start_date=start_date, end_date=end_date
        )

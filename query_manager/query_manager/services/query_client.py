import json
from datetime import date
from typing import Any, cast

import aiofiles
import pandas as pd

from query_manager.config import get_settings
from query_manager.exceptions import MetricNotFoundError, MetricValueNotFoundError
from query_manager.services.s3 import S3Client
from query_manager.utilities.enums import Granularity


class QueryClient:
    """
    Query client to interact with semantic & ontology services
    This will return metadata, values and graph data
    """

    METRICS_FILE_PATH = "data/metrics.json"
    DIMENSIONS_FILE_PATH = "data/dimensions.json"

    def __init__(self, s3_client: S3Client):
        settings = get_settings()
        self.s3_client = s3_client
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
    async def get_metric_value(
        self, metric_id: str, start_date: date | None = None, end_date: date | None = None
    ) -> dict[str, Any]:
        """
        Fetches the value of a metric for a given date range.

        :param metric_id: The ID of the metric to fetch the value for.
        :param start_date: The start date of the period to fetch the value for.
        :param end_date: The end date of the period to fetch the value for.
        :return: A dictionary representing the metric value.

        Example:
        {
            "metric_id": "CAC",
            "value": 0
        }
        """
        # todo: Actual implementation with semantic api
        values = await self.get_metric_values(metric_id, start_date, end_date)
        if values:
            # aggregate the values using a sum
            value = sum([value["value"] for value in values])
            return {"metric_id": metric_id, "value": value}
        raise MetricValueNotFoundError(metric_id)

    async def get_metric_values(
        self,
        metric_id: str,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
        grain: Granularity = Granularity.DAY,
    ) -> list[dict[str, Any]]:
        """
        Fetches time series metric values for a given metric ID within a specified time range and optional dimensions.

        :param metric_id: The ID of the metric to fetch values for.
        :param start_date: The start date of the period to fetch values for (inclusive).
        :param end_date: The end date of the period to fetch values for (exclusive).
        :param dimensions: Optional, either 'all' or list of dimension names to include in the results.
        :param grain: The granularity of the time series data, represented as a Grain enum.
        :return: A list of dictionaries representing the time series metric values.
        """
        # todo: Actual implementation with cube api
        # todo: Implement logic to filter data based on date range, dimensions, and grain
        # Construct the S3 key based on whether dimensions are specified
        if dimensions:
            key = f"mock_data/metric/{metric_id}/values_with_dimensions.json"
        else:
            key = f"mock_data/metric/{metric_id}/values.json"

        # Construct the SQL expression to filter data by date range
        sql_expression = "SELECT * FROM s3object"

        # Use the S3Client to query the data asynchronously
        metric_values = await self.s3_client.query_s3_json(key, sql_expression)

        # load in df
        df = pd.DataFrame(metric_values)
        if not df.empty:
            # convert date strings to date
            df["date"] = pd.to_datetime(df["date"]).dt.date

            # filter by date range
            if start_date:
                df = df[df["date"] >= start_date]
            if end_date:
                df = df[df["date"] < end_date]

        results = cast(list[dict[str, Any]], df.to_dict(orient="records"))
        return results

    async def get_metric_targets(
        self, metric_id: str, start_date: str | None = None, end_date: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetches target values for a given metric ID within an optional time range.

        :param metric_id: The ID of the metric to fetch targets for.
        :param start_date: The start date of the period to fetch targets for (inclusive).
        :param end_date: The end date of the period to fetch targets for (exclusive).
        :return: A list of dictionaries representing the target values.
        """
        # todo: Actual implementation with cube api
        key = f"mock_data/metric/{metric_id}/target.json"

        # Construct the SQL expression
        sql_expression = "SELECT * FROM s3object"

        # Use the S3Client to query the data synchronously
        metric_targets = await self.s3_client.query_s3_json(key, sql_expression)

        return metric_targets

import asyncio
import logging
from datetime import date
from enum import Enum
from typing import Any

from httpx import Auth

from commons.clients.auth import JWTAuth, JWTSecretKeyAuth
from commons.clients.base import AsyncHttpClient, HttpClientError
from commons.models.enums import Granularity
from query_manager.core.models import Dimension, Metric
from query_manager.exceptions import (
    ErrorCode,
    MalformedMetricMetadataError,
    MetricValueNotFoundError,
    QueryManagerError,
)

logger = logging.getLogger(__name__)


class CubeJWTAuthType(str, Enum):
    """
    Enum class for the supported Cube JWT authentication types.
    """

    SECRET_KEY = "SECRET_KEY"  # noqa
    TOKEN = "TOKEN"  # noqa


class CubeClient(AsyncHttpClient):
    CONTINUE_WAIT_MAX_RETRIES = 6
    METRIC_TARGETS_CUBE = "metric_targets"

    def __init__(
        self,
        base_url: str,
        auth_type: CubeJWTAuthType | None = None,
        auth_options: dict[str, str] | None = None,
    ):
        """
        Base class for interacting with the Cube API.
        :param base_url: Base url of the Cube API e.g., https://analytics.cube.dev/cubejs-api/v1
        :param auth_type: The type of authentication to use, defaults to None if no authentication is required.
        :param auth_options: Dictionary containing the authentication options. Required if auth_type is not None.
        For SECRET_KEY auth_type, the dictionary should contain the key "secret_key".
        For TOKEN auth_type, the dictionary should contain the key "token" which is the JWT token.
        """
        self.auth_type = auth_type
        self.auth_options = auth_options
        self._validate_auth_options()
        auth = self._create_auth(auth_type, auth_options)
        super().__init__(base_url, auth=auth)

    def _create_auth(self, auth_type: CubeJWTAuthType | None, auth_options: dict[str, str] | None) -> Auth | None:
        """
        First validates the auth_options &
        Creates an Auth object based on the auth_type and auth_options.

        :param auth_type: The type of authentication to use.
        :param auth_options: Dictionary containing the authentication options.
        :return: An Auth object for the specified authentication type.
        """
        self._validate_auth_options()

        if auth_type is None or auth_options is None:
            return None
        if auth_type == CubeJWTAuthType.SECRET_KEY:
            return JWTSecretKeyAuth(auth_options["secret_key"])
        elif auth_type == CubeJWTAuthType.TOKEN:
            return JWTAuth(auth_options["token"])

    def _validate_auth_options(self):
        if self.auth_type is None:
            return
        if self.auth_type == CubeJWTAuthType.SECRET_KEY:
            if "secret_key" not in self.auth_options:
                raise ValueError("Secret key is required for SECRET_KEY authentication.")
        elif self.auth_type == CubeJWTAuthType.TOKEN:
            if "token" not in self.auth_options:
                raise ValueError("Token is required for TOKEN authentication.")
        else:
            raise ValueError(f"Unsupported authentication type: {self.auth_type}")

    async def load_query_data(self, query: dict[str, Any], retries: int = 0) -> list[dict[str, Any]]:
        """
        Loads the data for a given query from the Cube API.
        If the response contains the error "Continue wait", retries the request with exponential backoff.
        :param query: The query to execute.
        :param retries: The number of retries for the request.
        :return: The response data from the Cube API.
        :raises HttpClientError: If the request fails or the max retries are reached.
        """
        response = await self.post("load", data={"query": query})
        # check if response contains "Continue wait" error
        if response.get("error") == "Continue wait":
            if retries < self.CONTINUE_WAIT_MAX_RETRIES:
                retry_delay = 2**retries
                logger.warning(
                    "Cube returned 'Continue wait' error for query: %s. Retrying in %d seconds...", query, retry_delay
                )
                await asyncio.sleep(retry_delay)
                return await self.load_query_data(query, retries + 1)
            else:
                raise HttpClientError(f"Max retries reached for Cube API request, query: {query}")
        logger.debug("Cube api invoked with query: %s", query)
        return response["data"]

    @staticmethod
    def convert_cube_response_to_metric_values(
        response: list[dict], metric: Metric, grain: Granularity | None = None
    ) -> list[dict[str, Any]]:
        """
        Converts the response from the Cube API to a list of dictionaries representing the metric values.
        e.g. from
        "data": [
            {
                "dim_opportunity.account_name": "Splunk Inc.",
                "dim_opportunity.sqo_date.week": "2020-09-07T00:00:00.000",
                "dim_opportunity.sqo_date": "2020-09-07T00:00:00.000",
                "dim_opportunity.sqo_to_win_rate": "100.000000"
            }
        ]
        to
        [
            {
                "date": "2020-09-07",
                "value": 100.0,
                "account_name": "Splunk Inc."
            }
        ]
        Or like this if not dimension included
        {
            "date": "2020-09-07",
            "value": 100.0
        }
        or
        {
            "value": 100.0
        }
        if no time series present in response.
        Where it will use
        semantic meta from metric and dimension
        e.g.
        {
            "cube": "dim_opportunity",
            "member": "sqo_rate",
            "member_type": "measure",
            "time_dimension": {
              "cube": "dim_opportunity",
              "member": "created_at"
            }
        }
        :param metric:
        :param response: The response from the Cube API.
        :param grain: The granularity of the metric values.
        :return: A list of dictionaries representing the metric values.
        """
        if not response:
            return []
        semantic_meta = metric.meta_data.semantic_meta
        time_dimension = semantic_meta.time_dimension
        time_dimension_member = (
            f"{time_dimension.cube}.{time_dimension.member}.{grain.value}"
            if grain
            else f"{time_dimension.cube}.{time_dimension.member}"
        )
        is_time_series = bool(time_dimension_member in response[0])
        metric_key = f"{semantic_meta.cube}.{semantic_meta.member}"
        dimension_key_map = (
            {
                f"{dimension.meta_data.semantic_meta.cube}.{dimension.meta_data.semantic_meta.member}": dimension.dimension_id  # noqa
                for dimension in metric.dimensions
            }
            if metric.dimensions
            else dict()
        )
        # filter out dimension keys from response
        dimension_keys_present = [key for key in dimension_key_map if key in response[0]]
        metric_values = []
        for data in response:
            value_node = {"metric_id": metric.metric_id, "value": data[metric_key]}
            if is_time_series:
                # convert date to date only
                value_node["date"] = data[time_dimension_member].split("T")[0]
            # add dimension values to the node
            for key in dimension_keys_present:
                value_node[dimension_key_map[key]] = data[key]
            metric_values.append(value_node)

        logger.debug("Converted metric values: %s for metric_id: %s", metric_values, metric.metric_id)
        return metric_values

    @staticmethod
    def generate_query_for_metric(
        metric: Metric,
        grain: Granularity | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Generates a query dictionary for fetching metric values from the Cube API.
        e.g. for semantic meta
            {
            "cube": "dim_opportunity",
            "member": "sqo_rate",
            "member_type": "measure",
            "time_dimension": {
              "cube": "dim_opportunity",
              "member": "created_at"
            }
          }
        it will generate a query like
        {
            "measures": ["dim_opportunity.sqo_rate"],
            "dimensions": [],
            "timeDimensions": [
                {
                    "dimension": "dim_opportunity.created_at",
                    "granularity": "day",
                    "dateRange": ["2021-01-01", "2021-01-02"]
                }
            ],
            "filters": []
        }
        :param metric: The metric to fetch values for.
        :param grain: The granularity of the metric values.
        :param start_date: The start date of the period to fetch values for.
        :param end_date: The end date of the period to fetch values for.
        :param dimensions: The dimensions to include in the query.
        :return: A dictionary representing the query.
        """
        semantic_meta = metric.meta_data.semantic_meta
        query: dict[str, list] = {
            "measures": [],
            "dimensions": [],
            "timeDimensions": [],
            "filters": [],
        }
        # add metric to measures
        query["measures"].append(f"{semantic_meta.cube}.{semantic_meta.member}")
        # generate time series data if grain is provided
        if grain:
            time_series_dict: dict[str, Any] = {
                "dimension": f"{semantic_meta.time_dimension.cube}.{semantic_meta.time_dimension.member}",
                "granularity": grain.value,
            }
            if start_date and end_date:
                time_series_dict["dateRange"] = [start_date, end_date]
            query["timeDimensions"].append(time_series_dict)
        else:
            member = f"{semantic_meta.time_dimension.cube}.{semantic_meta.time_dimension.member}"
            # add start_date and end_date to filters
            if start_date:
                query["filters"].append({"dimension": member, "operator": "gte", "values": [start_date]})
            if end_date:
                query["filters"].append({"dimension": member, "operator": "lt", "values": [end_date]})
        # add dimensions to the query if provided
        if dimensions is not None:
            for dimension_id in dimensions:
                dimension = metric.get_dimension(dimension_id)
                if not dimension or not dimension.meta_data:
                    logger.warning("Dimension meta_data not found for dimension_id: %s", dimension_id)
                    continue
                dimension_semantic_meta = dimension.meta_data.semantic_meta
                query["dimensions"].append(f"{dimension_semantic_meta.cube}.{dimension_semantic_meta.member}")

        logger.debug("Cube query generated: %s for metric_id: %s", query, metric.metric_id)
        return query

    async def load_metric_values_from_cube(
        self,
        metric: Metric,
        grain: Granularity | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        dimensions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Loads metric values from the Cube API for a given metric.
        :param metric: The metric to fetch values for.
        :param grain: The granularity of the metric values.
        :param start_date: The start date of the period to fetch values for.
        :param end_date: The end date of the period to fetch values for.
        :param dimensions: The dimensions to include in the query.
        :return: A list of dictionaries representing the metric values.
        :raises MalformedMetricMetadataError: If the metric meta_data is malformed
                                            or if the Cube API request fails.
        :raises MetricValueNotFoundError: If no values are found for the metric.
        """
        query = self.generate_query_for_metric(metric, grain, start_date, end_date, dimensions)
        try:
            response = await self.load_query_data(query)
        except HttpClientError as exc:
            logger.error("Cube API request failed with error: %s", exc)
            raise MalformedMetricMetadataError(metric.metric_id) from exc
        metric_values = self.convert_cube_response_to_metric_values(response, metric, grain=grain)
        if not metric_values:
            logger.warning("No values found for metric_id: %s", metric.metric_id)
            raise MetricValueNotFoundError(metric.metric_id)
        return metric_values

    async def load_metric_targets_from_cube(
        self,
        metric: Metric,
        grain: Granularity | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """
        Loads target values from the Cube API for a given metric.
        :param metric: The metric to fetch targets for.
        :param grain: The granularity of the target values.
        :param start_date: The start date of the period to fetch targets for.
        :param end_date: The end date of the period to fetch targets for.
        :return: A list of dictionaries representing the target values.
        :raises QueryManagerError: If the Cube API request fails.
        """
        targets_cube = self.METRIC_TARGETS_CUBE
        target_columns: list[str] = [
            "metric_id",
            "grain",
            "target_date",
            "target_value",
            "aim",
            "target_lower_bound",
            "target_upper_bound",
            "yellow_buffer",
            "red_buffer",
        ]
        cube_columns = [f"{targets_cube}.{col}" for col in target_columns]
        query: dict = {
            "dimensions": cube_columns,
            "filters": [{"member": f"{targets_cube}.metric_id", "operator": "equals", "values": [metric.metric_id]}],
            "timeDimensions": [],
        }

        if grain is not None:
            query["filters"].append({"member": f"{targets_cube}.grain", "operator": "equals", "values": [grain.value]})

        if start_date and end_date:
            query["timeDimensions"].append(
                {"dimension": f"{targets_cube}.target_date", "dateRange": [start_date, end_date]}
            )

        try:
            response = await self.load_query_data(query)
        except HttpClientError as exc:
            logger.error("Cube API request failed with error: %s", exc)
            raise QueryManagerError(
                500, ErrorCode.METRIC_TARGET_ERROR, f"Failed to fetch targets for metric_id: {metric.metric_id}"
            ) from exc

        # convert cube response to target values
        target_values = []
        for row in response:
            # remove the cube prefix from the column names
            target_values.append({col.split(".")[1]: row[col] for col in row if col in cube_columns})

        return target_values

    async def load_dimension_members_from_cube(self, dimension: Dimension) -> list[Any]:
        """
        Loads members of a dimension from the Cube API.
        :param dimension: The dimension to fetch members for.
        :return: A list of dimension members.
        """
        key = f"{dimension.meta_data.semantic_meta.cube}.{dimension.meta_data.semantic_meta.member}"
        query = {"dimensions": [key]}
        try:
            response = await self.load_query_data(query)
        except HttpClientError as exc:
            logger.error("Cube API request failed with error: %s", exc)
            return []
        # convert cube response to dimension members (list of values)
        # also remove null values from the list
        members = [row[key] for row in response if row[key] is not None]
        logger.debug("Dimension members: %s for dimension_id: %s", members, dimension.dimension_id)
        return members

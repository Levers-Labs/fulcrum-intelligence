import json
from datetime import date
from typing import Any, cast

import aiofiles
from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession

from commons.db.crud import ModelType, NotFoundError
from commons.models.enums import Granularity
from commons.utilities.pagination import PaginationParams
from query_manager.config import get_settings
from query_manager.core.models import Dimensions, Metric
from query_manager.core.schemas import Dimension, MetricDetail
from query_manager.exceptions import MetricNotFoundError
from query_manager.services.cube import CubeClient


class QueryClient:
    """
    Query client to interact with semantic & ontology services
    This will return metadata, values and graph data
    """

    def __init__(self, cube_client: CubeClient, session: AsyncSession | None = AsyncSession):
        self.cube_client = cube_client
        self.dimension_model = Dimensions
        self.metric_model = Metric
        self.session = session

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
        self, *, params: PaginationParams, metric_ids: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetches a list of all metrics, optionally in a paginated manner.

        :param params: Optional number of items per page for pagination.
        :param metric_ids: Optional list of metric IDs to filter the results by.
        :return: A list of dictionaries representing metrics.
        """
        query = select(self.metric_model).offset(params.offset).limit(params.limit)
        if metric_ids:
            query = query.filter(self.metric_model.metric_id.in_(metric_ids))

        results = await self.session.execute(query)
        records: list[ModelType] = cast(list[ModelType], results.scalars().all())
        records_dicts: list[dict[str, Any]] = [record.dict() for record in records]
        return records_dicts

    async def get_metric_details(self, metric_id: str) -> dict[str, Any] | None:
        """
        Fetches detailed information for a specific metric by its ID.

        :param metric_id: The ID of the metric to fetch details for.
        :return: A dictionary representing the metric details, or None if not found.
        """
        statement = select(self.metric_model).filter_by(metric_id=metric_id)
        results = await self.session.execute(statement=statement)
        instance: ModelType | None = results.scalar_one_or_none()

        if instance is None:
            raise MetricNotFoundError(metric_id)

        instance_dict = instance.dict()  # Convert the instance to a dictionary

        # Remove unwanted fields
        instance_dict.pop("created_at", None)
        instance_dict.pop("updated_at", None)

        # Map metric_id to id
        if "metric_id" in instance_dict:
            instance_dict["id"] = str(instance_dict.pop("metric_id"))

        return instance_dict

    async def list_dimensions(self, *, params: PaginationParams) -> list[dict[str, Any]]:
        results = await self.session.execute(select(self.dimension_model).offset(params.offset).limit(params.limit))
        records: list[ModelType] = cast(list[ModelType], results.scalars().all())
        records_dicts: list[dict[str, Any]] = [record.dict() for record in records]
        return records_dicts

    async def get_dimension_details(self, dimension_id: str) -> dict[str, Any] | None:
        """
        Fetches detailed information for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch details for.
        :return: A dictionary representing the dimension details, or None if not found.
        """
        statement = select(self.dimension_model).filter_by(dimension_id=dimension_id)
        results = await self.session.execute(statement=statement)
        instance: ModelType | None = results.scalar_one_or_none()

        if instance is None:
            raise NotFoundError(id=dimension_id)

        instance_dict = instance.dict()  # Convert the instance to a dictionary

        # Remove unwanted fields
        instance_dict.pop("created_at", None)
        instance_dict.pop("updated_at", None)

        # Map dimension_id to id
        if "dimension_id" in instance_dict:
            instance_dict["id"] = str(instance_dict.pop("dimension_id"))

        # Rename meta_data to metadata
        if "meta_data" in instance_dict:
            instance_dict["metadata"] = instance_dict.pop("meta_data")

        return instance_dict

    async def get_dimension_members(self, dimension_id: str) -> list[Any]:
        """
        Fetches members for a specific dimension by its ID.

        :param dimension_id: The ID of the dimension to fetch members for.
        :return: A list of members for the dimension, or an empty list if not found.
        """
        dimension_detail = await self.get_dimension_details(dimension_id)
        if not dimension_detail:
            return []
        dimension = Dimension.parse_obj(dimension_detail)
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

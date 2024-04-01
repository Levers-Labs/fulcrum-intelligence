import json
from typing import Any

import aiofiles

from query_manager.config import get_settings


class QueryClient:
    """
    Query client to interact with semantic & ontology services
    This will return metadata, values and graph data
    """

    METRICS_FILE_PATH = "data/metrics.json"
    DIMENSIONS_FILE_PATH = "data/dimensions.json"

    def __init__(self):
        settings = get_settings()
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

    async def list_metrics(self, page: int | None = None, per_page: int | None = None) -> list[dict[str, Any]]:
        """
        Fetches a list of all metrics, optionally in a paginated manner.

        :param page: Optional page number for pagination.
        :param per_page: Optional number of items per page for pagination.
        :return: A list of dictionaries representing metrics.
        """
        metrics_data = await self.load_data(self.metric_file_path)
        # Implement pagination logic here if necessary
        return metrics_data

    async def get_metric_details(self, metric_id: str) -> dict[str, Any] | None:
        """
        Fetches detailed information for a specific metric by its ID.

        :param metric_id: The ID of the metric to fetch details for.
        :return: A dictionary representing the metric details, or None if not found.
        """
        metrics_data = await self.load_data(self.metric_file_path)
        return next((metric for metric in metrics_data if metric["id"] == metric_id), None)

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

from typing import Any

from commons.clients.base import AsyncHttpClient


class StoryManagerClient(AsyncHttpClient):
    """Client for interacting with Story Manager API."""

    async def list_stories(self, page: int = 1, size: int = 100, **query_params: Any) -> dict[str, Any]:
        """
        Get paginated list of stories with optional filters.

        Args:
            page: Page number (default: 1)
            size: Items per page (default: 100)
            **query_params: Optional filter parameters including:
                - story_types: Filter by story types
                - story_groups: Filter by story groups
                - genres: Filter by genres
                - metric_ids: Filter by metric IDs
                - grains: Filter by granularity (day, week, month)
                - story_date_start: Filter by story date start
                - story_date_end: Filter by story date end
                - digest: Filter by digest type
                - section: Filter by section
                - is_heuristic: Filter by heuristic flag

        Returns:
            dict: Paginated list of stories with metadata
        """
        # Convert Granularity enums to their values if present
        if "grains" in query_params and query_params["grains"]:
            query_params["grains"] = [grain.value for grain in query_params["grains"]]

        params = {"page": page, "size": size, **query_params}
        return await self.get("/stories/", params=params)

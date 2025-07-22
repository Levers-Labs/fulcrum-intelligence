from typing import Any

from commons.clients.base import AsyncHttpClient, HttpClientError
from commons.exceptions import InvalidTenantError
from commons.utilities.context import get_tenant_id


class InsightBackendClient(AsyncHttpClient):
    async def get_user(self, user_id: int, token: str):
        # headers = {"Authorization": token}
        return await self.get(f"/users/{user_id}")  # , {"headers": headers})

    async def get_tenant_config(self) -> dict:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        try:
            config = await self.get("/tenant/config/internal")
            return config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def get_tenants(self, limit: int = 100, **params) -> dict:
        """
        Get all tenants. This endpoint bypasses tenant context validation.
        """
        params.update({"limit": limit})
        return await self.get("/tenants/all", params=params)

    async def validate_tenant(self):
        """
        Validates the existence of a tenant by attempting to retrieve its configuration.
        This function asynchronously retrieves the Insights Backend Client instance using the provided settings.
        It then attempts to fetch the tenant configuration using the client. If the attempt fails, it logs a warning
        indicating that the tenant ID does not exist and raises an InvalidTenantError.
        :param tenant_id: The ID of the tenant to validate.
        :raises InvalidTenantError: If the tenant ID does not exist.
        """
        # Attempt to fetch the tenant configuration
        try:
            _ = await self.get("/tenant/config")
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def get_slack_channel_details(self, channel_id: str) -> Any:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        response = await self.get(f"slack/channels/{channel_id}")
        return response

    async def get_slack_config(self) -> dict:
        """
        Get tenant configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        try:
            config = await self.get("/tenant/config/internal")
            slack_config = config["slack_connection"]
            return slack_config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def get_snowflake_config(self) -> dict:
        """
        Get Snowflake configuration for tenant from context.
        Raises an InvalidTenant exception if the tenant is not found.
        :return: dict
        """
        try:
            config = await self.get("/tenant/snowflake-config/internal")
            return config
        except HttpClientError as e:
            if e.status_code == 404:
                tenant_id = get_tenant_id()
                raise InvalidTenantError(tenant_id) from e
            raise

    async def list_alerts(
        self,
        page: int = 1,
        size: int = 100,
        grains: list[str] | None = None,
        is_active: bool | None = None,
        is_published: bool | None = None,
        metric_ids: list[str] | None = None,
        story_groups: list[str] | None = None,
    ) -> dict:
        """
        Get paginated list of alerts with optional filters.

        Args:
            page: Page number (default: 1)
            size: Items per page (default: 100)
            grains: Filter by granularity (day, week, month)
            is_active: Filter by active status
            is_published: Filter by published status
            metric_ids: Filter by metric IDs
            story_groups: Filter by story groups

        Returns:
            dict: Paginated list of alerts with metadata
        """
        params: dict[str, Any] = {
            "page": page,
            "size": size,
        }

        # Add optional filters
        if grains is not None:
            params["grains"] = grains
        if is_active is not None:
            params["is_active"] = str(is_active).lower()
        if is_published is not None:
            params["is_published"] = str(is_published).lower()
        if metric_ids:
            params["metric_ids"] = metric_ids
        if story_groups:
            params["story_groups"] = story_groups

        return await self.get("/notification/alerts", params=params)

    async def create_notification_execution(self, execution_data: dict) -> dict:
        """
        Create a new notification execution record.

        Args:
            execution_data: Dictionary containing execution details including:
                - notification_type: Type of notification (ALERT/REPORT)
                - status: Execution status
                - delivery_meta: Delivery results and metadata
                - run_meta: Prefect run information
                - error_info: Error details if any
                - report_meta/alert_meta: Notification-specific metadata
                - alert_id/report_id: ID of the associated notification

        Returns:
            dict: Created execution record
        """
        return await self.post("/notification/executions", data=execution_data)

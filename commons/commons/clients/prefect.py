from typing import Any

from pydantic import BaseModel, Field

from commons.clients.auth import JWTAuth
from commons.clients.base import AsyncHttpClient, HttpClientError


class Schedule(BaseModel):
    cron: str
    timezone: str | None = None
    day_or: bool = True


class PrefectSchedule(BaseModel):
    active: bool = True
    catchup: bool = True
    schedule: Schedule


class PrefectDeployment(BaseModel):
    name: str
    description: str | None = None
    flow_name: str
    work_pool_name: str
    entrypoint: str
    tags: list[str] = Field(default_factory=list)
    parameter_openapi_schema: dict[str, Any] | None = None
    parameters: dict[str, Any] | None = Field(default_factory=dict)
    pull_steps: list[dict[str, Any]] = Field(default_factory=list)
    schedules: list[PrefectSchedule] | None = None

    @classmethod
    def create_with_defaults(
        cls,
        name: str,
        flow_name: str,
        entrypoint: str,
        schedule: str | None = None,  # Cron schedule expression (e.g. "0 0 * * *")
        timezone: str | None = None,  # Timezone for the schedule (e.g. "America/New_York")
        is_active: bool = True,  # New parameter for schedule active status
        parameter_schema: type[BaseModel] | None = None,
        work_pool_name: str = "tasks-manager-ecs-pool",
        pull_steps: list[dict[str, Any]] | None = None,
        parameters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> "PrefectDeployment":
        pull_steps = pull_steps or [
            {"prefect.deployments.steps.set_working_directory": {"directory": "/opt/prefect/tasks_manager"}}
        ]
        return cls(
            name=name,
            flow_name=flow_name,
            entrypoint=entrypoint,
            work_pool_name=work_pool_name,
            parameter_openapi_schema=parameter_schema.model_json_schema() if parameter_schema else None,
            pull_steps=pull_steps,
            parameters=parameters,
            schedules=(
                [PrefectSchedule(active=is_active, schedule=Schedule(cron=schedule, timezone=timezone))]
                if schedule and timezone
                else None
            ),
            **kwargs,
        )


class PrefectDeploymentRead(PrefectDeployment):
    id: str


class PrefectClient(AsyncHttpClient):
    """Client for interacting with Prefect Cloud API."""

    def __init__(self, api_url: str, api_key: str):
        """Initialize Prefect client with API URL and key.

        Args:
            api_url: Prefect Cloud API URL (e.g., https://api.prefect.cloud/api)
            api_key: Prefect Cloud API key
        """
        super().__init__(base_url=api_url, api_version="", auth=JWTAuth(token=api_key))

    async def get_flow_by_name(self, flow_name: str) -> dict[str, Any]:
        """Get flow details by name.

        Args:
            flow_name: Name of the flow to retrieve

        Returns:
            Flow details as a dictionary
        """
        endpoint = f"flows/name/{flow_name}"
        return await self.get(endpoint)

    async def create_flow(self, name: str, tags: list[str] | None = None) -> dict[str, Any]:
        """Create a new flow.

        Args:
            name: Name of the flow
            tags: Optional list of tags to associate with the flow

        Returns:
            Created flow details as a dictionary
        """
        data = {"name": name, "tags": tags or []}
        return await self.post("/flows/", data=data)

    async def get_or_create_flow(self, name: str) -> dict[str, Any]:
        """Get or create a flow.

        Args:
            name: Name of the flow
        """
        try:
            return await self.get_flow_by_name(name)
        except HttpClientError as exc:
            if exc.status_code == 404:
                return await self.create_flow(name)
            raise exc

    async def create_deployment(self, deployment: PrefectDeployment) -> dict[str, Any]:
        """Create a new deployment for a flow.

        Args:
            deployment: Deployment configuration

        Returns:
            Created deployment details
        """
        # First get the flow ID
        flow = await self.get_or_create_flow(deployment.flow_name)
        if not flow:
            raise ValueError(f"Flow {deployment.flow_name} not found")

        flow_id = flow["id"]

        # replace flow_id with id
        deployment_data = deployment.model_dump()
        deployment_data.pop("flow_name")
        deployment_data["flow_id"] = flow_id

        return await self.post("/deployments/", data=deployment_data)

    async def delete_deployment(self, deployment_id: str) -> dict[str, Any]:
        """Delete a deployment by ID.

        Args:
            deployment_id: ID of the deployment to delete

        Returns:
            Deleted deployment details
        """
        return await self.delete(f"/deployments/{deployment_id}")

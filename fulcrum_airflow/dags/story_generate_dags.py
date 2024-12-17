import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.config import (
    ANALYSIS_MANAGER_SERVER_HOST,
    AUTH0_API_AUDIENCE,
    AUTH0_CLIENT_ID,
    AUTH0_CLIENT_SECRET,
    AUTH0_ISSUER,
    DATABASE_URL,
    DOCKER_HOST,
    ECS_CLUSTER_NAME,
    ECS_REGION,
    ECS_SUBNETS,
    ECS_TASK_DEFINITION_NAME,
    ENV,
    INSIGHTS_BACKEND_SERVER_HOST,
    QUERY_MANAGER_SERVER_HOST,
    SECRET_KEY,
    SERVER_HOST,
    STORY_BUILDER_IMAGE,
    STORY_MANAGER_SERVER_HOST,
)
from utils.story_utils import fetch_auth_token, filter_grains

# Initialize logger for the DAG
logger = logging.getLogger(__name__)

# Define the schedule for each story group, running every 30 minutes starting from 12:00 AM
STORY_GROUP_META = {
    "GROWTH_RATES": {"schedule_interval": "0 0 * * *"},  # 12:00 AM
    "TREND_CHANGES": {"schedule_interval": "30 0 * * *"},  # 12:30 AM
    "TREND_EXCEPTIONS": {"schedule_interval": "0 1 * * *"},  # 1:00 AM
    "LONG_RANGE": {"schedule_interval": "30 1 * * *"},  # 1:30 AM
    "GOAL_VS_ACTUAL": {"schedule_interval": "0 2 * * *"},  # 2:00 AM
    "COMPONENT_DRIFT": {"schedule_interval": "30 2 * * *"},  # 2:30 AM
    "RECORD_VALUES": {"schedule_interval": "0 3 * * *"},  # 3:00 AM
    "STATUS_CHANGE": {"schedule_interval": "30 3 * * *"},  # 3:30 AM
    "SEGMENT_DRIFT": {"schedule_interval": "0 4 * * *"},  # 4:00 AM
    "INFLUENCE_DRIFT": {"schedule_interval": "30 4 * * *"},  # 4:30 AM
    "REQUIRED_PERFORMANCE": {"schedule_interval": "0 5 * * *"},  # 5:00 AM
    "SIGNIFICANT_SEGMENTS": {"schedule_interval": "30 5 * * *"},  # 5:30 AM
    "LIKELY_STATUS": {"schedule_interval": "0 6 * * *"},  # 6:00 AM
}


def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    """
    Creates a DAG for a given story group.

    Args:
    - group (str): The name of the story group.
    - meta (dict[str, Any]): The metadata for the story group.
    """
    dag_id = f"GROUP_{group}_STORY_DAG"

    @dag(dag_id=dag_id, start_date=datetime(2024, 9, 26), schedule_interval=meta["schedule_interval"], catchup=False)
    def story_group_dag():
        @task(task_id="get_auth_header")
        def get_auth_header():
            """
            Fetches the authentication header.

            Returns:
            - dict[str, str]: The authentication header.
            """
            return {"Authorization": f"Bearer {fetch_auth_token()}"}

        @task(task_id="fetch_tenants")
        def fetch_tenants(auth_header) -> list[int]:
            """
            Fetches all tenant IDs from the Insights Backend.

            Args:
            - auth_header (dict[str, str]): The authentication header for the request.

            Returns:
            - list[int]: A list of tenant IDs.
            """
            logger.info("Fetching tenant IDs from Insights Backend")
            url = f"{INSIGHTS_BACKEND_SERVER_HOST.strip('/')}/tenants/all?limit=100"

            response = requests.get(url, headers=auth_header, timeout=30)
            response.raise_for_status()
            tenants_data = response.json()
            tenant_ids = [tenant["id"] for tenant in tenants_data.get("results", []) if tenant["id"] != 2]
            logger.info("Successfully fetched %s tenant IDs", len(tenant_ids))
            return tenant_ids

        @task(task_id="fetch_metric_ids", multiple_outputs=True)
        def fetch_metric_ids(auth_header, tenants: list[int]) -> dict[str, list[str]]:
            """
            Fetches metric IDs for each tenant.

            Args:
            - auth_header (dict[str, str]): The authentication header for the request.
            - tenants (list[int]): A list of tenant IDs.

            Returns:
            - dict[str, list[str]]: A dictionary mapping tenant IDs to lists of metric IDs.
            """
            results = defaultdict(list)
            for tenant_id in tenants:
                # Add tenant to auth header
                tenant_auth_header = auth_header.copy()
                tenant_auth_header["X-Tenant-Id"] = str(tenant_id)
                logger.info("Fetching metric IDs for tenant %s", tenant_id)
                url = f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics?limit=1000"
                response = requests.get(url, headers=tenant_auth_header, timeout=30)
                response.raise_for_status()
                metrics_data = response.json()
                results[str(tenant_id)] = [metric["metric_id"] for metric in metrics_data.get("results", [])]
                logger.info("Successfully fetched %s metric IDs for tenant %s", len(results[str(tenant_id)]), tenant_id)
            logger.info("Successfully fetched metric IDs for all tenants")
            return results

        @task(task_id="fetch_group_meta_task", multiple_outputs=True)
        def fetch_group_meta_task(auth_header, tenants: list[int]) -> dict[str, list[str]]:
            """
            Fetches group metadata for each tenant.

            Args:
            - auth_header (dict[str, str]): The authentication header for the request.
            - tenants (list[int]): A list of tenant IDs.

            Returns:
            - dict[str, list[str]]: A dictionary mapping tenant IDs to lists of group metadata.
            """
            results = defaultdict(list)
            for tenant_id in tenants:
                # Add tenant to auth header
                tenant_auth_header = auth_header.copy()
                tenant_auth_header["X-Tenant-Id"] = str(tenant_id)
                url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
                logger.info("Fetching group meta for tenant %s", tenant_id)
                response = requests.get(url, headers=tenant_auth_header, timeout=20)
                response.raise_for_status()
                _meta = response.json()
                results[str(tenant_id)] = _meta.get("grains", [])
                logger.info("Successfully fetched group meta for tenant %s", tenant_id)
            logger.info("Successfully fetched group meta for all tenants")
            return results

        @task(task_id="prepare_story_builder_commands")
        def prepare_story_builder_commands(
            _tenants: list[int], _metric_ids_map: dict[str, list[str]], _grains_map: dict[str, list[str]]
        ) -> list[str]:
            """
            Prepares story builder commands for all tenants.

            Args:
            - _tenants (list[int]): A list of tenant IDs.
            - _metric_ids_map (dict[str, list[str]]): A dictionary mapping tenant IDs to lists of metric IDs.
            - _grains_map (dict[str, list[str]]): A dictionary mapping tenant IDs to lists of group metadata.

            Returns:
            - list[str]: A list of story builder commands.
            """
            logger.info("Preparing story builder commands for all tenants")
            today = datetime.utcnow()

            commands = []
            for tenant_id in _tenants:
                logger.info("Preparing story builder commands for tenant %s", tenant_id)
                tenant_id_str = str(tenant_id)
                # Skip tenant if it doesn't have any metrics
                if tenant_id_str not in _metric_ids_map:
                    continue
                # Skip tenant if it doesn't have any grains
                if tenant_id_str not in _grains_map:
                    continue
                # Prepare story builder commands
                metrics = _metric_ids_map[tenant_id_str]
                grains = _grains_map[tenant_id_str]

                # Filter grains based on the current date
                filtered_grains = filter_grains(grains, today)
                commands.extend(
                    [
                        f"story generate {group} {metric_id} {tenant_id} {grain}"
                        for metric_id in metrics
                        for grain in filtered_grains
                    ]
                )
                logger.info("Prepared %s story builder commands for tenant %s", len(commands), tenant_id)
            logger.info("Successfully prepared story builder commands for all tenants")
            return commands

        auth_header = get_auth_header()
        tenants = fetch_tenants(auth_header)
        metric_ids_map = fetch_metric_ids(auth_header, tenants)
        grains_map = fetch_group_meta_task(auth_header, tenants)
        commands = prepare_story_builder_commands(tenants, metric_ids_map, grains_map)

        if ENV == "local":
            # For local development, we use DockerOperator to run tasks in Docker containers
            DockerOperator.partial(
                task_id="generate_stories",
                image=STORY_BUILDER_IMAGE,
                docker_url=DOCKER_HOST,
                network_mode="bridge",
                auto_remove="success",
                environment={
                    "SERVER_HOST": SERVER_HOST,
                    "SECRET_KEY": SECRET_KEY,
                    "ANALYSIS_MANAGER_SERVER_HOST": ANALYSIS_MANAGER_SERVER_HOST,
                    "QUERY_MANAGER_SERVER_HOST": QUERY_MANAGER_SERVER_HOST,
                    "DATABASE_URL": DATABASE_URL,
                    "AUTH0_API_AUDIENCE": AUTH0_API_AUDIENCE,
                    "AUTH0_ISSUER": AUTH0_ISSUER,
                    "AUTH0_CLIENT_ID": AUTH0_CLIENT_ID,
                    "AUTH0_CLIENT_SECRET": AUTH0_CLIENT_SECRET,
                },
            ).expand(command=commands)

        else:
            # For production, we use ECS to run tasks in a managed container environment
            @task
            def prepare_ecs_overrides(commands: list[str]) -> list[dict]:
                """
                Prepares the ECS overrides based on the commands.

                Args:
                - commands (list[str]): A list of story builder commands.

                Returns:
                - list[dict]: A list of ECS overrides.
                """
                # Prepare the ECS overrides based on the commands
                return [
                    {"containerOverrides": [{"name": "story-builder-manager", "command": command.split(" ")}]}
                    for command in commands
                ]

            ecs_overrides = prepare_ecs_overrides(commands)

            # Use EcsRunTaskOperator to run tasks on AWS ECS
            EcsRunTaskOperator.partial(
                task_id="generate_stories",
                cluster=ECS_CLUSTER_NAME,
                task_definition=ECS_TASK_DEFINITION_NAME,
                launch_type="FARGATE",
                network_configuration={"awsvpcConfiguration": {"subnets": ECS_SUBNETS, "assignPublicIp": "ENABLED"}},
                region_name=ECS_REGION,
                retries=3,
                retry_delay=timedelta(minutes=2),
            ).expand(overrides=ecs_overrides)

    story_group_dag()


# Iterate through each story group and its metadata to create a DAG for each
for story_group, group_meta in STORY_GROUP_META.items():
    create_story_group_dag(story_group, group_meta)

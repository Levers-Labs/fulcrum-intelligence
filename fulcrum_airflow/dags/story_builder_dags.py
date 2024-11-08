import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.docker.operators.docker import DockerOperator

logger = logging.getLogger(__name__)


def get_config(key: str, default: str | None = None) -> str:
    # Check if running locally or in production
    if os.getenv("AIRFLOW_ENV") == "local":
        return os.getenv(key, default)  # type: ignore
    return Variable.get(key, default)


# Fetch environment-specific settings
# Common configurations
ENV = get_config("AIRFLOW_ENV")
DEFAULT_SCHEDULE = get_config("DEFAULT_SCHEDULE", "0 0 * * *")
SECRET_KEY = get_config("SECRET_KEY")
STORY_MANAGER_SERVER_HOST = get_config("STORY_MANAGER_SERVER_HOST")
ANALYSIS_MANAGER_SERVER_HOST = get_config("ANALYSIS_MANAGER_SERVER_HOST")
QUERY_MANAGER_SERVER_HOST = get_config("QUERY_MANAGER_SERVER_HOST")
DSENSEI_BASE_URL = get_config("DSENSEI_BASE_URL")
DATABASE_URL = get_config("DATABASE_URL")
AUTH0_API_AUDIENCE = get_config("AUTH0_API_AUDIENCE")
AUTH0_ISSUER = get_config("AUTH0_ISSUER")
AUTH0_CLIENT_SECRET = get_config("AUTH0_CLIENT_SECRET")
AUTH0_CLIENT_ID = get_config("AUTH0_CLIENT_ID")
INSIGHTS_BACKEND_SERVER_HOST = get_config("INSIGHTS_BACKEND_SERVER_HOST")

# Local (Docker run) configurations
STORY_BUILDER_IMAGE = get_config("STORY_BUILDER_IMAGE", "story-builder-manager:latest")
DOCKER_HOST = get_config("DOCKER_HOST", "")
SERVER_HOST = get_config("SERVER_HOST", "http://localhost:8003")

# ECS configurations
ECS_SUBNETS = get_config("ECS_SUBNETS").split(",") if get_config("ECS_SUBNETS") else None
ECS_REGION = get_config("ECS_REGION")
ECS_TASK_DEFINITION_NAME = get_config("ECS_TASK_DEFINITION_NAME")
ECS_CLUSTER_NAME = get_config("ECS_CLUSTER_NAME")

# Default schedule for midnight
DEFAULT_SCHEDULE = get_config("DEFAULT_SCHEDULE", "0 0 * * *")  # Every day at 12:00 AM

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


def fetch_auth_token():
    url = f"{AUTH0_ISSUER.rstrip('/')}/oauth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "client_id": AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    response = requests.post(url, headers=headers, json=data, timeout=30)
    response_data = response.json()
    return response_data["access_token"]


def fetch_all_metrics(auth_header: dict[str, str]) -> list[str]:

    response = requests.get(f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics", headers=auth_header, timeout=30)
    response.raise_for_status()
    metrics_data = response.json().get("results", [])

    return [metric["metric_id"] for metric in metrics_data]


def fetch_group_meta(group: str, auth_header: dict[str, str]) -> dict[str, Any]:
    url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
    response = requests.get(url, headers=auth_header, timeout=20)
    response.raise_for_status()
    return response.json()


def filter_grains(grains: list[str], today: datetime) -> list[str]:
    """
    Filter grains based on the current date.
    
    Args:
        grains (list[str]): List of grains to filter.
        today (datetime): Current date.

    Returns:
        list[str]: Filtered list of grains.
    """
    # Filter grains based on the current date
    return [
        grain
        for grain in grains
        # Include 'week' grain if today is Monday (weekday() == 0)
        if (grain == "week" and today.weekday() == 0)
        # Include 'month' grain if today is the first day of the month
        or (grain == "month" and today.day == 1)
        # Always include 'day' grain
        or (grain == "day")
    ]


def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    dag_id = f"GROUP_{group}_STORY_DAG"

    @dag(dag_id=dag_id, start_date=datetime(2024, 9, 26), schedule_interval=meta["schedule_interval"], catchup=False)
    def story_group_dag():
        @task(task_id="get_auth_header")
        def get_auth_header():
            return {"Authorization": f"Bearer {fetch_auth_token()}"}

        @task(task_id="fetch_tenants")
        def fetch_tenants(auth_header) -> list[int]:
            """
            Fetch all tenant IDs from the Insights Backend.
            Returns:
                list[int]: A list of tenant IDs.
            """

            logger.info("Fetching tenant IDs from Insights Backend")
            url = f"{INSIGHTS_BACKEND_SERVER_HOST.strip('/')}/tenants/all?limit=100"

            response = requests.get(url, headers=auth_header, timeout=30)
            response.raise_for_status()
            tenants_data = response.json()
            tenant_ids = [tenant["id"] for tenant in tenants_data.get("results", [])]
            logger.info("Successfully fetched %s tenant IDs", len(tenant_ids))
            return tenant_ids

        @task(task_id="fetch_metric_ids", multiple_outputs=True)
        def fetch_metric_ids(auth_header, tenants: list[int]) -> dict[str, list[str]]:
            results = defaultdict(list)
            for tenant_id in tenants:
                # Add tenant to auth header
                tenant_auth_header = auth_header.copy()
                tenant_auth_header["X-Tenant-Id"] = str(tenant_id)
                logger.info("Fetching metric IDs for tenant %s", tenant_id)
                response = requests.get(
                    f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics?limit=1000", headers=tenant_auth_header, timeout=30
                )
                response.raise_for_status()
                metrics_data = response.json()
                results[str(tenant_id)] = [metric["metric_id"] for metric in metrics_data.get("results", [])]
                logger.info("Successfully fetched %s metric IDs for tenant %s", len(results[str(tenant_id)]), tenant_id)
            logger.info("Successfully fetched metric IDs for all tenants")
            return results

        @task(task_id="fetch_group_meta_task", multiple_outputs=True)
        def fetch_group_meta_task(auth_header, tenants: list[int]) -> dict[str, list[str]]:
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
            ).expand(overrides=ecs_overrides)

    story_group_dag()


for story_group, group_meta in STORY_GROUP_META.items():
    create_story_group_dag(story_group, group_meta)

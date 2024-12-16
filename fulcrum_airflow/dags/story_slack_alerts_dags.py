import logging
from collections import defaultdict
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.config import (  # ECS_SLACK_ALERTS_TASK_DEFINITION_NAME,
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
    ENV,
    INSIGHTS_BACKEND_SERVER_HOST,
    QUERY_MANAGER_SERVER_HOST,
    SECRET_KEY,
    SERVER_HOST,
    STORY_BUILDER_IMAGE,
)
from utils.story_utils import Granularity, fetch_auth_token, filter_grains

logger = logging.getLogger(__name__)

# Define the schedule for the send-alerts task, running at 9 AM every day
ALERT_DAG_SCHEDULE = "0 9 * * *"  # 9:00 AM every day


def create_slack_alert_dag() -> None:
    dag_id = "SEND_SLACK_ALERTS_DAG"

    @dag(dag_id=dag_id, start_date=datetime(2024, 9, 26), schedule_interval=ALERT_DAG_SCHEDULE, catchup=False)
    def slack_alert_dag():
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
            """
            Fetches metric IDs for all tenants that have Slack notifications enabled.
            This is used to prepare alert commands for sending Slack notifications.
            """
            tenant_metrics_map = defaultdict(list)
            for tenant_id in tenants:
                # Creates a tenant-specific auth header by copying the original auth header and adding the tenant ID.
                tenant_auth_header = auth_header.copy()
                tenant_auth_header["X-Tenant-Id"] = str(tenant_id)

                logger.info("Fetching metric IDs for tenant %s", tenant_id)

                # Makes a GET request to the Query Manager Server to fetch all metrics for the tenant that have Slack
                # notifications enabled.
                metrics_response = requests.get(
                    f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics?slack_enabled=true&limit=1000",
                    headers=tenant_auth_header,
                    timeout=30,
                )
                metrics_response.raise_for_status()
                metrics_data = metrics_response.json()
                # Extracts metric IDs from the response and stores them in the tenant_metrics_map.
                tenant_metrics_map[str(tenant_id)] = [metric["metric_id"] for metric in metrics_data.get("results", [])]
                logger.info(
                    "Fetched metrics with Slack notifications enabled for tenant %s",
                    tenant_id,
                )

            logger.info("Successfully fetched Slack-enabled metrics for all tenants")
            return tenant_metrics_map

        @task(task_id="fetch_grains", multiple_outputs=True)
        def fetch_grains(tenants: list[int]) -> dict[str, list[str]]:
            results = defaultdict(list)
            for tenant_id in tenants:
                results[str(tenant_id)] = [grain.value for grain in Granularity]
                logger.info("Successfully fetched grains for tenant %s", tenant_id)
            logger.info("Successfully fetched grains for all tenants")
            return results

        @task(task_id="prepare_alert_commands")
        def prepare_alert_commands(
            _tenants: list[int], _metric_ids_map: dict[str, list[str]], _grains_map: dict[str, list[str]]
        ) -> list[str]:
            """
            Prepare the commands for sending Slack alerts for each tenant and metric.
            """
            logger.info("Preparing Slack alert commands for all tenants")
            today = datetime.utcnow()

            commands = []
            for tenant_id in _tenants:
                logger.info("Preparing alert commands for tenant %s", tenant_id)
                tenant_id_str = str(tenant_id)

                # Skip tenant if it doesn't have any metrics or grains
                if tenant_id_str not in _metric_ids_map or tenant_id_str not in _grains_map:
                    continue

                metrics = _metric_ids_map[tenant_id_str]
                grains = _grains_map[tenant_id_str]

                # Filter grains based on the current date
                filtered_grains = filter_grains(grains, today)
                commands.extend(
                    [
                        f"story send-slack-alert {metric_id} {tenant_id} {grain} {today.date()}"
                        for metric_id in metrics
                        for grain in filtered_grains
                    ]
                )

                logger.info("Prepared %s alert commands for tenant %s", len(commands), tenant_id)
            logger.info("Successfully prepared Slack alert commands for all tenants")
            return commands

        # Fetch the necessary data
        auth_header = get_auth_header()
        tenants = fetch_tenants(auth_header)
        metric_ids_map = fetch_metric_ids(auth_header, tenants)
        grains_map = fetch_grains(tenants)
        commands = prepare_alert_commands(tenants, metric_ids_map, grains_map)

        if ENV == "local":
            # For local development, we use DockerOperator to run tasks in Docker containers
            DockerOperator.partial(
                task_id="send_slack_alerts",
                image=STORY_BUILDER_IMAGE,
                docker_url=DOCKER_HOST,
                network_mode="bridge",
                auto_remove="success",
                environment={
                    "SERVER_HOST": SERVER_HOST,
                    "SECRET_KEY": SECRET_KEY,
                    "QUERY_MANAGER_SERVER_HOST": QUERY_MANAGER_SERVER_HOST,
                    "ANALYSIS_MANAGER_SERVER_HOST": ANALYSIS_MANAGER_SERVER_HOST,
                    "INSIGHTS_BACKEND_SERVER_HOST": INSIGHTS_BACKEND_SERVER_HOST,
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
                    {"containerOverrides": [{"name": "alert-sender", "command": command.split(" ")}]}
                    for command in commands
                ]

            ecs_overrides = prepare_ecs_overrides(commands)

            # Use EcsRunTaskOperator to run tasks on AWS ECS
            EcsRunTaskOperator.partial(
                task_id="send_slack_alerts",
                cluster=ECS_CLUSTER_NAME,
                task_definition="airflow-story-slack-alerts",
                launch_type="FARGATE",
                network_configuration={"awsvpcConfiguration": {"subnets": ECS_SUBNETS, "assignPublicIp": "ENABLED"}},
                region_name=ECS_REGION,
            ).expand(overrides=ecs_overrides)

    slack_alert_dag()


# Create the Slack Alert DAG that runs at 9 AM daily
create_slack_alert_dag()

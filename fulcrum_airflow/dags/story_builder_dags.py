import os
from datetime import datetime
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.docker.operators.docker import DockerOperator


def get_config(key: str, default: str = "") -> str:
    # Check if running locally or in production
    if os.getenv("AIRFLOW_ENV") == "local":
        return os.getenv(key, default)
    return Variable.get(key, default)


# Fetch environment-specific settings
ENV = get_config("AIRFLOW_ENV")
DEFAULT_SCHEDULE = get_config("DEFAULT_SCHEDULE", "0 0 * * *")
STORY_BUILDER_IMAGE = get_config("STORY_BUILDER_IMAGE", "story-builder-manager:latest")
DOCKER_HOST = get_config("DOCKER_HOST", "")
SERVER_HOST = get_config("SERVER_HOST", "http://localhost:8002")
SECRET_KEY = get_config("SECRET_KEY")
STORY_MANAGER_SERVER_HOST = get_config("STORY_MANAGER_SERVER_HOST")
ANALYSIS_MANAGER_SERVER_HOST = get_config("ANALYSIS_MANAGER_SERVER_HOST")
QUERY_MANAGER_SERVER_HOST = get_config("QUERY_MANAGER_SERVER_HOST")
DSENSEI_BASE_URL = get_config("DSENSEI_BASE_URL")
DATABASE_URL = get_config("DATABASE_URL")
START_DATE = get_config("STORY_GENERATION_START_DATE")
AUTH0_API_AUDIENCE = get_config("STORY_GENERATION_START_DATE")
AUTH0_ISSUER = get_config("AUTH0_ISSUER")
AUTH0_CLIENT_SECRET = get_config("AUTH0_CLIENT_SECRET")
AUTH0_CLIENT_ID = get_config("AUTH0_CLIENT_ID")
ECS_SUBNETS = get_config("ECS_SUBNETS").split(" ")
ECS_REGION = get_config("ECS_REGION")
ECS_TASK_DEFINITION_NAME = get_config("ECS_TASK_DEFINITION_NAME")
ECS_CLUSTER_NAME = get_config("ECS_CLUSTER_NAME")

STORY_GROUP_META = {
    "GROWTH_RATES": {"schedule_interval": DEFAULT_SCHEDULE},
    "TREND_CHANGES": {"schedule_interval": DEFAULT_SCHEDULE},
    "TREND_EXCEPTIONS": {"schedule_interval": DEFAULT_SCHEDULE},
    "LONG_RANGE": {"schedule_interval": DEFAULT_SCHEDULE},
    "GOAL_VS_ACTUAL": {"schedule_interval": DEFAULT_SCHEDULE},
    "LIKELY_STATUS": {"schedule_interval": DEFAULT_SCHEDULE},
    "RECORD_VALUES": {"schedule_interval": DEFAULT_SCHEDULE},
    "STATUS_CHANGE": {"schedule_interval": DEFAULT_SCHEDULE},
    "SEGMENT_DRIFT": {"schedule_interval": DEFAULT_SCHEDULE},
    "REQUIRED_PERFORMANCE": {"schedule_interval": DEFAULT_SCHEDULE},
    "SIGNIFICANT_SEGMENTS": {"schedule_interval": DEFAULT_SCHEDULE},
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
    metrics_data = response.json()
    for metric in metrics_data.get("results"):
        metric["id"] = metric["metric_id"]

    return [metric["id"] for metric in metrics_data.get("results", [])]


def fetch_group_meta(group: str, auth_header: dict[str, str]) -> dict[str, Any]:
    url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
    response = requests.get(url, headers=auth_header, timeout=20)
    response.raise_for_status()
    return response.json()


def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    date_format = "%Y-%m-%d"
    date_object = datetime.strptime(START_DATE, date_format)

    @dag(dag_id="story_group_dag", start_date=date_object, schedule_interval=meta["schedule_interval"], catchup=False)
    def story_group_dag():
        @task(task_id="get_auth_header")
        def get_auth_header():
            return {"Authorization": f"Bearer {fetch_auth_token()}"}

        @task(task_id="fetch_metric_ids")
        def fetch_metric_ids(auth_header) -> list[str]:
            return fetch_all_metrics(auth_header)

        @task(task_id="fetch_group_meta")
        def fetch_group_meta_task(auth_header) -> list[str]:
            _meta = fetch_group_meta(group, auth_header)  # Use a specific group or refactor as needed
            return _meta.get("grains", [])

        @task(task_id="prepare_story_builder_commands")
        def prepare_story_builder_commands(_metrics: list[str], _grains: list[str]) -> list[str]:
            return [f"story generate {group} {metric_id} {grain}" for metric_id in _metrics for grain in _grains]

        auth_header = get_auth_header()
        metrics = fetch_metric_ids(auth_header)
        grains = fetch_group_meta_task(auth_header)
        commands = prepare_story_builder_commands(metrics, grains)

        if ENV == "local":
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

            @task
            def prepare_ecs_overrides(commands: list[str]) -> list[dict]:
                # Prepare the ECS overrides based on the commands
                return [
                    {"containerOverrides": [{"name": "story-builder-manager", "command": command.split(" ")}]}
                    for command in commands
                ]

            ecs_overrides = prepare_ecs_overrides(commands)

            EcsRunTaskOperator.partial(
                task_id="run_ecs_task",
                cluster=ECS_CLUSTER_NAME,
                task_definition=ECS_TASK_DEFINITION_NAME,
                launch_type="FARGATE",
                network_configuration={
                    "awsvpcConfiguration": {
                        "subnets": ECS_SUBNETS,
                        "assignPublicIp": "ENABLED",
                    }
                },
                region_name=ECS_REGION,
            ).expand(overrides=ecs_overrides)

    story_group_dag()


for story_group, group_meta in STORY_GROUP_META.items():
    create_story_group_dag(story_group, group_meta)

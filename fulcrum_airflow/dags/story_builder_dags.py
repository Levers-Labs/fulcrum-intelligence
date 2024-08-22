from datetime import datetime
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

# Environment variables
DEFAULT_SCHEDULE = Variable.get("DEFAULT_SCHEDULE", "0 0 * * *")
STORY_BUILDER_IMAGE = Variable.get("STORY_BUILDER_IMAGE", "story-builder-manager:latest")
DOCKER_HOST = Variable.get("DOCKER_HOST")
SERVER_HOST = Variable.get("SERVER_HOST", "http://localhost:8002")
SECRET_KEY = Variable.get("SECRET_KEY")
STORY_MANAGER_SERVER_HOST = Variable.get("STORY_MANAGER_SERVER_HOST")
ANALYSIS_MANAGER_SERVER_HOST = Variable.get("ANALYSIS_MANAGER_SERVER_HOST")
QUERY_MANAGER_SERVER_HOST = Variable.get("QUERY_MANAGER_SERVER_HOST")
DSENSEI_BASE_URL = Variable.get("DSENSEI_BASE_URL")
DATABASE_URL = Variable.get("DATABASE_URL")
START_DATE = Variable.get("STORY_GENERATION_START_DATE")

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


def m2m_auth():
    url = "https://insights-web-app.us.auth0.com/oauth/token"
    headers = {"Content-Type": "application/json"}

    data = {
        "client_id": "Ihu4GZuicrFp3mlnTmZJJMBKxVQIYQop",
        "client_secret": "0i_-sIBz21ctAeWwwHs1wrCl-CpijNuJ5lUB3h55ocB5r4N7BjXtM1jkTNfduFkN",
        "grant_type": "client_credentials",
    }
    response = requests.post(url, headers=headers, json=data, timeout=30)
    response_data = response.json()
    return response_data["access_token"]


headers = {"Authorization": f"Bearer {m2m_auth()}"}


def fetch_all_metrics() -> list[str]:
    response = requests.get(f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics", headers=headers, timeout=30)
    response.raise_for_status()
    metrics_data = response.json()
    for metric in metrics_data.get("results"):
        metric["id"] = metric["metric_id"]

    return [metric["id"] for metric in metrics_data.get("results", [])]


def fetch_group_meta(group: str) -> dict[str, Any]:
    url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return response.json()


def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    date_format = "%Y-%m-%d"
    date_object = datetime.strptime(START_DATE, date_format)

    @dag(dag_id="story_group_dag", start_date=date_object, schedule_interval=meta["schedule_interval"], catchup=False)
    def story_group_dag():
        @task(task_id="fetch_metric_ids")
        def fetch_metric_ids() -> list[str]:
            return fetch_all_metrics()

        @task(task_id="fetch_group_meta")
        def fetch_group_meta_task() -> list[str]:
            _meta = fetch_group_meta(group)  # Use a specific group or refactor as needed
            return _meta.get("grains", [])

        @task(task_id="prepare_story_builder_commands")
        def prepare_story_builder_commands(_metrics: list[str], _grains: list[str]) -> list[str]:
            return [f"story generate {group} {metric_id} {grain}" for metric_id in _metrics for grain in _grains]

        @task(task_id="run_ecs_tasks")
        def run_ecs_tasks(commands: list[str]):
            for idx, command in enumerate(commands):
                EcsRunTaskOperator(
                    task_id=f"run_ecs_task_{idx}",
                    cluster="airflow-ecs-operator",
                    task_definition="airflow-story-builder-task",
                    launch_type="FARGATE",
                    overrides={
                        "containerOverrides": [
                            {
                                "name": "story-builder-manager",
                                "command": command.split(" "),
                            }
                        ]
                    },
                    network_configuration={
                        "awsvpcConfiguration": {
                            "subnets": ["subnet-034a4d3dd0cccd058", "subnet-096d8d27095fad03c"],
                            "assignPublicIp": "ENABLED",
                        }
                    },
                    region_name="us-west-1",
                ).execute(context=get_current_context())

        metrics = fetch_metric_ids()
        grains = fetch_group_meta_task()
        commands = prepare_story_builder_commands(metrics, grains)
        run_ecs_tasks(commands)

    story_group_dag()


for story_group, group_meta in STORY_GROUP_META.items():
    create_story_group_dag(story_group, group_meta)

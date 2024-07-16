import os
from datetime import datetime
from typing import Any

import requests
from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

# Environment variables
DEFAULT_SCHEDULE = os.getenv("DEFAULT_SCHEDULE", "0 0 * * *")  # daily 12:00 AM
STORY_BUILDER_IMAGE = os.getenv("STORY_BUILDER_IMAGE", "story-builder-manager:latest")
DOCKER_HOST = os.getenv("DOCKER_HOST")
SERVER_HOST = os.getenv("SERVER_HOST", "http://localhost:8002")
SECRET_KEY = os.environ["SECRET_KEY"]
STORY_MANAGER_SERVER_HOST = os.environ["STORY_MANAGER_SERVER_HOST"]
ANALYSIS_MANAGER_SERVER_HOST = os.environ["ANALYSIS_MANAGER_SERVER_HOST"]
QUERY_MANAGER_SERVER_HOST = os.environ["QUERY_MANAGER_SERVER_HOST"]
DSENSEI_BASE_URL = os.environ["DSENSEI_BASE_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]


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


# Fetch a list of all metrics from the query manager
def fetch_all_metrics() -> list[str]:
    """
    Fetches a list of all metrics from the query manager.
    """
    # todo: add M2M auth
    response = requests.get(f"{QUERY_MANAGER_SERVER_HOST.strip('/')}/metrics", timeout=30)
    response.raise_for_status()
    metrics_data = response.json()
    return [metric["id"] for metric in metrics_data.get("results", [])]


# Fetch group details (grains) for a given story group
def fetch_group_meta(group: str) -> dict[str, Any]:
    """
    Fetches group details (grains) for a given story group using the get_story_group_meta endpoint.

    Args:
        group (str): The story group for which to fetch details.

    Returns:
        dict[str, Any]: A dictionary containing the group details, including supported grains.
    """
    # todo: add M2M auth
    url = f"{STORY_MANAGER_SERVER_HOST.strip('/')}/stories/groups/{group}"
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json()


# Function to create a DAG for a given story group
def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    """
    creates an Airflow DAG for a given story group.

    Args:
        group (str): The story group for which to create the DAG.
        meta (dict[str, Any]): A dictionary containing the group metadata.
    """
    dag_id = f"GROUP_{group}_STORY_DAG"

    @dag(dag_id=dag_id, start_date=datetime(2024, 7, 1), schedule_interval=meta["schedule_interval"], catchup=False)
    def story_group_dag():
        @task(task_id="fetch_metric_ids")
        def fetch_metric_ids() -> list[str]:
            """
            Airflow task to fetch metrics.
            """
            return fetch_all_metrics()

        @task(task_id="fetch_group_meta")
        def fetch_group_meta_task() -> list[str]:
            """
            Airflow task to fetch group details (grains) for the story group.
            """
            _meta = fetch_group_meta(group)
            return _meta.get("grains", [])

        @task(task_id="prepare_story_builder_commands")
        def prepare_story_builder_commands(_metrics: list[str], _grains: list[str]):
            """
            Airflow task to generate story tasks for each combination of metric and grain.

            Args:
                _metrics (list[str]): The list of metrics.
                _grains (list[str]): The list of grains.
            """
            return [f"story generate {group} {metric_id} {grain}" for metric_id in _metrics for grain in _grains]

        # Define the task dependencies
        metrics = fetch_metric_ids()
        grains = fetch_group_meta_task()
        commands = prepare_story_builder_commands(metrics, grains)
        # Run the story builder manager container with the commands
        # All the commands are run in parallel
        # The story builder manager container is run for each command
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
            },
        ).expand(command=commands)

    story_group_dag()


# Create DAGs for all story groups
for story_group, group_meta in STORY_GROUP_META.items():
    create_story_group_dag(story_group, group_meta)

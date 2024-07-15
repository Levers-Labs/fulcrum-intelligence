import logging
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_SCHEDULE = "*/30 * * * *"

STORY_GROUP_META = {"GROWTH_RATES": {"schedule_interval": DEFAULT_SCHEDULE}}


# Fetch list of all metrics from the query manager
def fetch_all_metrics() -> list[str]:
    """
    Fetches a list of all metrics from the query manager.
    """
    # response = requests.get("http://host.docker.internal:8002/v1/metrics")
    # response.raise_for_status()
    return ["NewProsps"]


# Fetch group details (grains) for a given story group
def fetch_group_meta(group: str) -> dict[str, Any]:
    """
    Fetches group details (grains) for a given story group.
    """
    # Example grains for the story group
    return {"grains": ["day", "week", "month"]}


# Function to create a DAG for a given story group
def create_story_group_dag(group: str, meta: dict[str, Any]) -> None:
    """
    Creates an Airflow DAG for a given story group.

    Args:
        story_group (StoryGroup): The story group for which to create the DAG.
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
            group_meta = fetch_group_meta(group)
            return group_meta.get("grains", [])

        @task(task_id="prepare_story_builder_commands")
        def prepare_story_builder_commands(metrics: list[str], grains: list[str]):
            """
            Airflow task to generate story tasks for each combination of metric and grain.

            Args:
                metric_id (str): The metric id.
                grain (str): The grain.
            """
            return [f"story generate {group} {metric_id} {grain}" for metric_id in metrics for grain in grains]

        # Define the task dependencies
        metrics = fetch_metric_ids()
        grains = fetch_group_meta_task()
        commands = prepare_story_builder_commands(metrics, grains)
        # Run the story builder manager container with the commands
        # All the commands are run in parallel
        # The story builder manager container is run for each command
        DockerOperator.partial(
            task_id="generate_stories",
            image="story-builder-manager:latest",
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            auto_remove="success",
            environment={
                "SERVER_HOST": "http://localhost:8002",
                "DEBUG": "true",
                "ENV": "dev",
                "BACKEND_CORS_ORIGINS": '["*"]',
                "SECRET_KEY": "some-secret-key",
                "ANALYSIS_MANAGER_SERVER_HOST": "http://host.docker.internal:8002/v1/",
                "QUERY_MANAGER_SERVER_HOST": "http://host.docker.internal:8002/v1/",
                "DATABASE_URL": "postgresql+asyncpg://postgres:passwordpw@host.docker.internal:5432/fulcrum_db",
            },
        ).expand(command=commands)

    story_group_dag()


# Create DAGs for all story groups
for group, meta in STORY_GROUP_META.items():
    create_story_group_dag(group, meta)

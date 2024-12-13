import os

from airflow.models import Variable


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
ECS_SLACK_ALERTS_TASK_DEFINITION_NAME = get_config("ECS_SLACK_ALERTS_TASK_DEFINITION_NAME")

# Default schedule for midnight
DEFAULT_SCHEDULE = get_config("DEFAULT_SCHEDULE", "0 0 * * *")  # Every day at 12:00 AM

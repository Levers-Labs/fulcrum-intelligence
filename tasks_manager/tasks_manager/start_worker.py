#!/usr/bin/env python3
"""
Prefect Worker Runner for ECS Deployment

This script runs a self-hosted Prefect worker that connects to Prefect Cloud
but executes flows on your own compute (ECS containers) instead of
Prefect-managed infrastructure. It is used to run the Prefect worker in the ECS container.
"""

import asyncio
import logging
import os
import sys
from typing import Any

from prefect import get_client
from prefect.workers.process import ProcessWorker

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def load_app_config() -> dict[str, Any]:
    """Load application configuration from Prefect blocks or environment variables."""
    config: dict[str, Any] = {}

    # Try to load from Prefect blocks first, fallback to environment variables
    try:
        from tasks_manager.config import AppConfig

        app_config = await AppConfig.load("app-config")
        logger.info("✓ Loaded configuration from Prefect blocks")
        return app_config.dict()
    except Exception as e:
        logger.warning(f"Could not load from Prefect blocks: {e}")
        logger.info("Loading configuration from environment variables")

        # Required environment variables
        required_vars = [
            "PREFECT_API_KEY",
            "PREFECT_API_URL",
            "DATABASE_URL",
            "SECRET_KEY",
            "AUTH0_API_AUDIENCE",
            "AUTH0_ISSUER",
        ]

        for var in required_vars:
            if not os.getenv(var):
                logger.error(f"Required environment variable {var} not set")
                sys.exit(1)

        logger.info("✓ All required environment variables are set")
        return config


async def start_prefect_worker(work_pool_name: str = "tasks-manager-ecs-pool") -> None:
    """Start the Prefect worker for the specified work pool."""

    logger.info(f"Starting Prefect worker for work pool: {work_pool_name}")

    # Load configuration
    await load_app_config()

    # Verify connection to Prefect Cloud
    async with get_client() as client:
        try:
            # Test connection
            await client.api_healthcheck()
            logger.info("✓ Connected to Prefect API successfully")

            # Check if work pool exists
            try:
                work_pool = await client.read_work_pool(work_pool_name)
                logger.info(f"✓ Found work pool: {work_pool.name} (Type: {work_pool.type})")
            except Exception as e:
                logger.error(f"Work pool '{work_pool_name}' not found: {e}")
                logger.error("Please create the work pool in Prefect Cloud first")
                sys.exit(1)

        except Exception as e:
            logger.error(f"Failed to connect to Prefect API: {e}")
            logger.error("Check PREFECT_API_KEY and PREFECT_API_URL")
            sys.exit(1)

    # Start the worker
    logger.info("Starting Prefect worker...")

    worker = ProcessWorker(
        work_pool_name=work_pool_name,
        work_queues=["default"],  # Default queue
    )

    try:
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for the worker runner."""

    logger.info("🚀 Starting Fulcrum Prefect Worker")
    logger.info("This worker will execute flows on your own compute infrastructure")

    # Get work pool name from environment or use default
    work_pool_name = os.getenv("PREFECT_WORK_POOL_NAME", "tasks-manager-ecs-pool")

    try:
        asyncio.run(start_prefect_worker(work_pool_name))
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

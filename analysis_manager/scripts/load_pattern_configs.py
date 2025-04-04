#!/usr/bin/env python
"""
Script to load default pattern configurations from JSON for a specific tenant.

Usage:
    python load_pattern_configs.py --tenant-id 1
"""

import argparse
import asyncio
import json
import logging
import sys

from analysis_manager.config import get_settings
from analysis_manager.db.config import get_async_session
from analysis_manager.patterns.manager import PatternManager
from commons.utilities.context import set_tenant_id
from levers.models import PatternConfig

logger = logging.getLogger(__name__)


async def load_pattern_configs_for_tenant(tenant_id: int) -> None:
    """
    Load default pattern configurations for a specific tenant.

    Args:
        tenant_id: The tenant ID to load configurations for
    """
    logger.info(f"Loading pattern configurations for tenant {tenant_id}")

    settings = get_settings()
    # Set tenant context
    set_tenant_id(tenant_id)

    # Path to the default configurations file
    config_file = settings.PATHS.ROOT_DIR / "data" / "pattern_configs.json"

    if not config_file.exists():
        logger.error("Configuration file not found: %s", config_file)
        return

    try:
        # Load configurations from file
        with open(config_file) as f:
            configs = json.load(f)

        logger.info("Loaded %d pattern configurations from %s", len(configs), config_file)

        # Create session
        async with get_async_session() as session:
            pattern_manager = PatternManager(session)

            # Check existing patterns
            try:
                existing_patterns = await pattern_manager.list_pattern_configs()
                existing_pattern_names = (
                    {config.pattern_name for config in existing_patterns} if existing_patterns else set()
                )
                logger.info(
                    "Found %d existing pattern configurations for tenant %d", len(existing_pattern_names), tenant_id
                )
            except Exception as e:
                logger.error("Error listing existing pattern configurations: %s", e)
                existing_pattern_names = set()

            # Load each configuration that doesn't already exist
            loaded_count = 0
            for config_data in configs:
                pattern_name = config_data["pattern_name"]

                if pattern_name in existing_pattern_names:
                    logger.info(
                        "Configuration for pattern '%s' already exists for tenant %d, skipping", pattern_name, tenant_id
                    )
                    continue

                # Create PatternConfig from data
                try:
                    config = PatternConfig.model_validate(config_data)

                    # Store configuration for the tenant
                    await pattern_manager.store_pattern_config(config)
                    loaded_count += 1
                    logger.info("Stored configuration for pattern '%s' for tenant %d", pattern_name, tenant_id)
                except Exception as e:
                    logger.error("Failed to load configuration for pattern '%s': %s", pattern_name, e)

            logger.info("Loaded %d new pattern configurations for tenant %d", loaded_count, tenant_id)

            # Commit session to ensure changes are persisted
            await session.commit()

    except Exception as e:
        logger.error("Error loading pattern configurations: %s", e)


async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Load pattern configurations for a tenant.")
    parser.add_argument("--tenant-id", type=int, required=True, help="Tenant ID to load configurations for")

    args = parser.parse_args()
    tenant_id = args.tenant_id

    logger.info("Starting pattern configuration loader for tenant %d", tenant_id)

    try:
        await load_pattern_configs_for_tenant(tenant_id)
        logger.info("Pattern configuration loading completed for tenant %d", tenant_id)
    except Exception as e:
        logger.error("Error during pattern configuration loading: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

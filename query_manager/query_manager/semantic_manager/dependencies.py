"""
Dependencies for semantic manager module.

This module provides FastAPI dependency functions for the semantic manager module.
"""

import logging
from typing import Annotated

from fastapi import Depends

from commons.clients.snowflake import SnowflakeClient, SnowflakeConfigModel
from query_manager.core.dependencies import InsightBackendClientDep
from query_manager.db.config import AsyncSessionDep
from query_manager.semantic_manager.cache_manager import SnowflakeSemanticCacheManager
from query_manager.semantic_manager.crud import SemanticManager

logger = logging.getLogger(__name__)


async def get_semantic_manager(session: AsyncSessionDep) -> SemanticManager:
    return SemanticManager(session)


async def get_snowflake_client(insight_client: InsightBackendClientDep) -> SnowflakeClient | None:
    """Get Snowflake client from insights backend tenant configuration."""
    # Get snowflake configuration from tenant config
    snowflake_config = await insight_client.get_snowflake_config()

    # Create SnowflakeConfigModel from the API response
    client_config = SnowflakeConfigModel(
        account_identifier=snowflake_config["account_identifier"],
        username=snowflake_config["username"],
        password=snowflake_config.get("password"),
        private_key=snowflake_config.get("private_key"),
        private_key_passphrase=snowflake_config.get("private_key_passphrase"),
        database=snowflake_config["database"],
        db_schema=snowflake_config["db_schema"],
        warehouse=snowflake_config.get("warehouse"),
        role=snowflake_config.get("role"),
        auth_method=snowflake_config["auth_method"],
    )

    # Create and return Snowflake client
    return SnowflakeClient(config=client_config)


async def get_cache_manager(
    session: AsyncSessionDep,
    snowflake_client: Annotated[SnowflakeClient | None, Depends(get_snowflake_client)],
    insight_client: InsightBackendClientDep,
) -> SnowflakeSemanticCacheManager:
    # Get tenant name from insights backend
    tenant_details = await insight_client.get_tenant_details()
    tenant_identifier = tenant_details.get("identifier")

    return SnowflakeSemanticCacheManager(session, snowflake_client, tenant_identifier)


SemanticManagerDep = Annotated[SemanticManager, Depends(get_semantic_manager)]
CacheManagerDep = Annotated[SnowflakeSemanticCacheManager, Depends(get_cache_manager)]

"""Snowflake resource built from Insights Backend configuration.

This resource resolves tenant-aware Snowflake configuration using Insights Backend
and constructs a commons SnowflakeClient for use by assets.
"""

import logging

from dagster import ConfigurableResource, ResourceDependency

from asset_manager.resources import AppConfigResource
from asset_manager.services.auth import get_client_auth
from commons.clients.insight_backend import InsightBackendClient
from commons.clients.snowflake import SnowflakeClient, SnowflakeConfigModel

logger = logging.getLogger(__name__)


class SnowflakeResource(ConfigurableResource):
    app_config: ResourceDependency[AppConfigResource]

    async def get_client(self) -> SnowflakeClient:
        auth = get_client_auth(self.app_config)
        s = self.app_config.settings
        insights = InsightBackendClient(base_url=s.insights_backend_server_host, auth=auth)
        cfg = await insights.get_snowflake_config()
        model = SnowflakeConfigModel(
            account_identifier=cfg["account_identifier"],
            username=cfg["username"],
            password=cfg.get("password"),
            private_key=cfg.get("private_key"),
            private_key_passphrase=cfg.get("private_key_passphrase"),
            database=cfg["database"],
            db_schema=cfg["db_schema"],
            warehouse=cfg.get("warehouse"),
            role=cfg.get("role"),
            auth_method=cfg["auth_method"],
        )
        logger.info("Initialized Snowflake client for %s.%s", model.database, model.db_schema)
        return SnowflakeClient(config=model)

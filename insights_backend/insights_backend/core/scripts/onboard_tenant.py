import json
import logging
import os

from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from insights_backend.core.models import Tenant, TenantConfig

# Set up logging
logger = logging.getLogger(__name__)


async def onboard_tenant(session: AsyncSession, file_path: str) -> None:
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist")
        raise FileNotFoundError(f"File {file_path} does not exist")

    # Load tenant data from JSON file
    logger.info(f"Loading tenant data from {file_path}")
    tenant_data = json.load(open(file_path))

    # Extract config data from tenant_data
    config = tenant_data.pop("config", None)

    # Validate tenant data
    logger.debug("Validating tenant data")
    try:
        Tenant.model_validate(tenant_data)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise

    # Upsert Tenant object
    logger.debug("Upserting Tenant object")
    stmt = insert(Tenant).values(**tenant_data).on_conflict_do_update(index_elements=["identifier"], set_=tenant_data)
    result = await session.execute(stmt)
    tenant_id = result.inserted_primary_key[0]

    # flush the session
    await session.flush()

    # Fetch the tenant to ensure we have the latest data
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        logger.error(f"Failed to fetch tenant with ID {tenant_id}")
        raise ValueError(f"Failed to fetch tenant with ID {tenant_id}")

    # Validate config data
    config["tenant_id"] = tenant_id
    logger.debug("Validating config data")
    try:
        TenantConfig.model_validate(config)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise

    # Upsert TenantConfig object
    logger.debug("Upserting TenantConfig object")
    config_stmt = insert(TenantConfig).values(**config).on_conflict_do_update(index_elements=["tenant_id"], set_=config)
    await session.execute(config_stmt)

    # Commit changes to database
    logger.info("Committing changes to database")
    await session.commit()

    logger.info(f"Tenant {tenant.identifier} successfully onboarded")

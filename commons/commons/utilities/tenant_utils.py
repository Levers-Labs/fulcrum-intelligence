import logging

from commons.exceptions import InvalidTenantError
from commons.utilities.context import get_tenant_id
from commons.utilities.dependencies import get_insights_backend_client

# Set up logging
logger = logging.getLogger(__name__)


async def validate_tenant(settings, tenant_id: int | None = None):
    """
    Validates the existence of a tenant by attempting to retrieve its configuration.
    This function asynchronously retrieves the Insights Backend Client instance using the provided settings.
    It then attempts to fetch the tenant configuration using the client. If the attempt fails, it logs a warning
    indicating that the tenant ID does not exist and raises an InvalidTenantError.
    :param settings: The settings object containing the necessary configuration.
    :param tenant_id: The ID of the tenant to validate.
    :raises InvalidTenantError: If the tenant ID does not exist.
    """
    # Retrieve the Insights Backend Client instance asynchronously
    insights_client = await get_insights_backend_client(settings)

    # Attempt to fetch the tenant configuration
    try:
        _ = await insights_client.get_tenant_config()
    except Exception as ex:
        if not tenant_id:
            tenant_id = get_tenant_id()
        # Log a warning if the tenant ID does not exist
        logger.warning("Tenant ID: %s does not exist. Aborting upsert.", tenant_id)
        # Raise an InvalidTenantError
        raise InvalidTenantError(tenant_id) from ex

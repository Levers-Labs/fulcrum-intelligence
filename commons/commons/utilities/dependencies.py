from typing import Annotated

from fastapi import Depends

from commons.clients.auth import ClientCredsAuth
from commons.clients.insight_backend import InsightBackendClient


async def get_insights_backend_client(settings) -> InsightBackendClient:
    """
    This function initializes the InsightBackendClient with the server host and authentication details.
    :param settings: The settings object containing the necessary configuration.
    :return: An instance of InsightBackendClient.
    """
    return InsightBackendClient(
        settings.INSIGHTS_BACKEND_SERVER_HOST,
        auth=ClientCredsAuth(
            auth0_issuer=settings.AUTH0_ISSUER,  # Auth0 issuer URL
            client_id=settings.AUTH0_CLIENT_ID,  # Client ID for authentication
            client_secret=settings.AUTH0_CLIENT_SECRET,  # Client secret for authentication
            api_audience=settings.AUTH0_API_AUDIENCE,  # API audience for authentication
        ),
    )


InsightBackendClientDep = Annotated[InsightBackendClient, Depends(get_insights_backend_client)]

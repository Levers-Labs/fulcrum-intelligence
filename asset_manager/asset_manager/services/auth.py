"""Auth utilities for constructing client credential auth from app config."""

from asset_manager.resources.config import AppConfigResource
from commons.clients.auth import ClientCredsAuth


def get_client_auth(config: AppConfigResource) -> ClientCredsAuth:
    s = config.settings
    return ClientCredsAuth(
        client_id=s.auth0_client_id,
        client_secret=s.auth0_client_secret.get_secret_value(),
        api_audience=s.auth0_api_audience,
        auth0_issuer=s.auth0_issuer,
    )

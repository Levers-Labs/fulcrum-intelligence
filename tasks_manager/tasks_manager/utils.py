from commons.clients.auth import ClientCredsAuth
from tasks_manager.config import AppConfig


def get_client_auth_from_config(config: AppConfig) -> ClientCredsAuth:
    return ClientCredsAuth(
        auth0_issuer=config.auth0_issuer,
        client_id=config.auth0_client_id,
        client_secret=config.auth0_client_secret.get_secret_value(),
        api_audience=config.auth0_api_audience,
    )

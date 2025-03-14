from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request

from commons.auth.auth import Oauth2Auth
from commons.clients.slack import SlackClient
from commons.utilities.context import get_tenant_id
from commons.utilities.request_utils import get_referer
from insights_backend.config import get_settings
from insights_backend.core.crud import CRUDUser, TenantCRUD
from insights_backend.core.models import Tenant, User
from insights_backend.db.config import AsyncSessionDep
from insights_backend.services.slack_oauth import SlackOAuthService


async def get_users_crud(session: AsyncSessionDep) -> CRUDUser:
    return CRUDUser(model=User, session=session)


async def get_tenants_crud(session: AsyncSessionDep) -> TenantCRUD:
    return TenantCRUD(model=Tenant, session=session)


UsersCRUDDep = Annotated[CRUDUser, Depends(get_users_crud)]
TenantsCRUDDep = Annotated[TenantCRUD, Depends(get_tenants_crud)]


def oauth2_auth() -> Oauth2Auth:
    settings = get_settings()
    return Oauth2Auth(issuer=settings.AUTH0_ISSUER, api_audience=settings.AUTH0_API_AUDIENCE)


def get_slack_oauth_service(request: Request) -> SlackOAuthService:
    settings = get_settings()
    # Get the referrer URL from the request headers to ensure proper redirect
    referrer = get_referer(request) or str(request.base_url).rstrip("/")
    # Construct the full redirect URI
    redirect_uri = f"{referrer}/{settings.SLACK_OAUTH_REDIRECT_PATH.lstrip('/')}"
    return SlackOAuthService(
        client_id=settings.SLACK_CLIENT_ID,
        client_secret=settings.SLACK_CLIENT_SECRET,
        redirect_uri=redirect_uri,
        scopes=settings.SLACK_OAUTH_SCOPES,
    )


async def get_slack_client(
    tenant_crud: TenantsCRUDDep,
) -> SlackClient:
    tenant_id = get_tenant_id()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found in request context")
    tenant = await tenant_crud.get_tenant_config(tenant_id)
    if not tenant or tenant.slack_connection is None:
        raise HTTPException(status_code=422, detail="Slack bot token not configured for tenant")
    return SlackClient(token=tenant.slack_connection["bot_token"])  # type:ignore


SlackClientDep = Annotated[SlackClient, Depends(get_slack_client)]


SlackOAuthServiceDep = Annotated[SlackOAuthService, Depends(get_slack_oauth_service)]
